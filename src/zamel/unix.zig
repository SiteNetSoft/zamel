const std = @import("std");
const posix = std.posix;
const sync = @import("sync.zig");
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

// -------- path parsing --------

pub fn parseUnixPath(rest: []const u8) ![]const u8 {
    var clean = rest;
    // Strip leading "//" if present
    if (std.mem.startsWith(u8, clean, "//")) clean = clean[2..];

    // Strip query params
    const qmark = std.mem.indexOfScalar(u8, clean, '?');
    const path = if (qmark) |q| clean[0..q] else clean;

    if (path.len == 0) return error.InvalidUnixPath;

    return path;
}

fn parseQueryParam(rest: []const u8, key: []const u8) ?[]const u8 {
    const qmark = std.mem.indexOfScalar(u8, rest, '?') orelse return null;
    var query = rest[qmark + 1 ..];

    while (query.len > 0) {
        const amp = std.mem.indexOfScalar(u8, query, '&') orelse query.len;
        const param = query[0..amp];

        if (std.mem.startsWith(u8, param, key) and param.len > key.len and param[key.len] == '=') {
            return param[key.len + 1 ..];
        }

        if (amp < query.len) {
            query = query[amp + 1 ..];
        } else break;
    }
    return null;
}

// -------- helpers --------

fn sendAll(fd: posix.fd_t, data: []const u8) !void {
    var sent: usize = 0;
    while (sent < data.len) {
        const rc = posix.system.sendto(fd, data.ptr + sent, data.len - sent, 0, null, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => sent += rc,
            .INTR => continue,
            else => return error.SendFailed,
        }
    }
}

// -------- Unix Socket Producer (client) --------

const UnixCtx = struct {
    path: []const u8,
};

pub fn makeUnixEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    const path = try parseUnixPath(rest);
    const ctx = try allocator.create(UnixCtx);
    ctx.* = .{ .path = path };
    return .{
        .ctx = ctx,
        .createProducerFn = unixCreateProducer,
        .deinitFn = unixDeinit,
    };
}

fn unixDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *UnixCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn unixCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = unixSend };
}

fn unixSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const UnixCtx = @ptrCast(@alignCast(ctx.?));

    // Create Unix domain socket
    const sock_rc = posix.system.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const stream: posix.fd_t = @intCast(sock_rc);
    defer posix.close(stream);

    // Build sockaddr_un
    var addr: posix.sockaddr.un = .{ .path = undefined };
    @memset(&addr.path, 0);
    if (c.path.len >= addr.path.len) return error.PathTooLong;
    @memcpy(addr.path[0..c.path.len], c.path);

    const connect_rc = posix.system.connect(stream, @ptrCast(&addr), @sizeOf(posix.sockaddr.un));
    if (posix.errno(connect_rc) != .SUCCESS) return error.ConnectFailed;

    // Send exchange body
    if (ex.body.len > 0) try sendAll(stream, ex.body);

    // Shutdown write side to signal end of data (SHUT_WR = 1)
    _ = std.os.linux.shutdown(stream, 1);

    // Read response
    var response_buf: std.ArrayList(u8) = .empty;
    defer response_buf.deinit(ex.allocator);

    var recv_buf: [4096]u8 = undefined;
    while (true) {
        const n_rc = posix.system.recvfrom(stream, &recv_buf, recv_buf.len, 0, null, null);
        if (posix.errno(n_rc) != .SUCCESS) break;
        if (n_rc == 0) break;
        const n: usize = @intCast(n_rc);
        try response_buf.appendSlice(ex.allocator, recv_buf[0..n]);
    }

    if (response_buf.items.len > 0) {
        try ex.setBody(response_buf.items);
    }

    // Set socket path header
    try ex.putHeader("CamelUnixSocketPath", c.path);
}

// -------- Unix Socket Consumer (server) --------

const UnixConsumerCtx = struct {
    path: []const u8,
    max_connections: u32,
};

pub fn makeUnixConsumer(allocator: std.mem.Allocator, rest: []const u8) !Consumer {
    const path = try parseUnixPath(rest);

    var max_connections: u32 = 1;
    if (parseQueryParam(rest, "connections")) |val| {
        max_connections = std.fmt.parseUnsigned(u32, val, 10) catch 1;
    }

    const ctx = try allocator.create(UnixConsumerCtx);
    ctx.* = .{ .path = path, .max_connections = max_connections };

    return .{
        .ctx = ctx,
        .startFn = unixConsumerStart,
        .deinitFn = unixConsumerDeinit,
    };
}

fn unixConsumerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *UnixConsumerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn unixConsumerStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan_ptr: *const RoutePlan, route_id: u32) !void {
    const c: *const UnixConsumerCtx = @ptrCast(@alignCast(ctx.?));

    // Unlink stale socket before bind
    var unlink_path: [108]u8 = undefined;
    @memset(&unlink_path, 0);
    @memcpy(unlink_path[0..c.path.len], c.path);
    _ = posix.system.unlinkat(posix.AT.FDCWD, @ptrCast(&unlink_path), 0);

    // Create server socket
    const sock_rc = posix.system.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const server_fd: posix.fd_t = @intCast(sock_rc);
    defer posix.close(server_fd);

    // Build sockaddr_un
    var bind_addr: posix.sockaddr.un = .{ .path = undefined };
    @memset(&bind_addr.path, 0);
    if (c.path.len >= bind_addr.path.len) return error.PathTooLong;
    @memcpy(bind_addr.path[0..c.path.len], c.path);

    const bind_rc = posix.system.bind(server_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.un));
    if (posix.errno(bind_rc) != .SUCCESS) return error.BindFailed;

    // Listen
    const listen_rc = posix.system.listen(server_fd, 8);
    if (posix.errno(listen_rc) != .SUCCESS) return error.ListenFailed;

    // Accept loop
    var connections_served: u32 = 0;
    while (connections_served < c.max_connections) {
        const accept_rc = posix.system.accept(server_fd, null, null);
        if (posix.errno(accept_rc) != .SUCCESS) break;
        const client_fd: posix.fd_t = @intCast(accept_rc);
        defer posix.close(client_fd);

        // Read data from client
        var data_buf: std.ArrayList(u8) = .empty;
        defer data_buf.deinit(allocator);

        var recv_buf: [4096]u8 = undefined;
        while (true) {
            const n_rc = posix.system.recvfrom(client_fd, &recv_buf, recv_buf.len, 0, null, null);
            if (posix.errno(n_rc) != .SUCCESS) break;
            if (n_rc == 0) break;
            const n: usize = @intCast(n_rc);
            try data_buf.appendSlice(allocator, recv_buf[0..n]);
            if (n < recv_buf.len) break;
        }

        // Create exchange
        var ex = Exchange.init(allocator);
        defer ex.deinit();
        if (data_buf.items.len > 0) try ex.setBody(data_buf.items);

        // Set headers
        try ex.putHeader("CamelUnixSocketPath", c.path);

        // Run route
        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        exec.run(plan_ptr, route_id, &ex) catch {};

        // Send response back
        if (ex.body.len > 0) sendAll(client_fd, ex.body) catch {};

        connections_served += 1;
    }

    // Clean up socket file
    _ = posix.system.unlinkat(posix.AT.FDCWD, @ptrCast(&unlink_path), 0);
}

// -------- tests --------

fn testClock(_: ?*anyopaque) u64 {
    return 0;
}

test "parseUnixPath basic" {
    const path = try parseUnixPath("/tmp/test.sock");
    try std.testing.expectEqualStrings("/tmp/test.sock", path);
}

test "parseUnixPath strips leading //" {
    const path = try parseUnixPath("///tmp/test.sock");
    try std.testing.expectEqualStrings("/tmp/test.sock", path);
}

test "parseUnixPath with query params" {
    const path = try parseUnixPath("/tmp/test.sock?connections=5");
    try std.testing.expectEqualStrings("/tmp/test.sock", path);
}

test "parseUnixPath rejects empty" {
    try std.testing.expectError(error.InvalidUnixPath, parseUnixPath(""));
    try std.testing.expectError(error.InvalidUnixPath, parseUnixPath("?connections=5"));
}

test "parseQueryParam extracts value" {
    try std.testing.expectEqualStrings("5", parseQueryParam("/tmp/test.sock?connections=5", "connections").?);
    try std.testing.expect(parseQueryParam("/tmp/test.sock?connections=5", "missing") == null);
    try std.testing.expect(parseQueryParam("/tmp/test.sock", "connections") == null);
}

test "unix producer-consumer roundtrip" {
    const alloc = std.testing.allocator;
    const Step = @import("step.zig").Step;
    const Processor = @import("processor.zig").Processor;

    // Use a unique temp path
    const sock_path = "/tmp/zamel_test_unix.sock";

    // Ensure no stale socket
    var unlink_buf: [108]u8 = undefined;
    @memset(&unlink_buf, 0);
    @memcpy(unlink_buf[0..sock_path.len], sock_path);
    _ = posix.system.unlinkat(posix.AT.FDCWD, @ptrCast(&unlink_buf), 0);

    // Uppercase processor for the server route
    const uppercaseProcessor = struct {
        fn process(_: ?*anyopaque, ex: *Exchange) !void {
            const body = try ex.allocator.dupe(u8, ex.body);
            defer ex.allocator.free(body);
            for (body) |*ch| {
                if (ch.* >= 'a' and ch.* <= 'z') ch.* -= 32;
            }
            try ex.setBody(body);
        }
    }.process;

    // Build route plan: uppercase processor
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const rid = try plan.addRoute(alloc, &steps);

    const services = Services{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
    };

    // Start server in background thread
    const ServerCtx = struct {
        allocator: std.mem.Allocator,
        services: Services,
        plan: *const RoutePlan,
        route_id: u32,
        done: sync.Event = .{},
    };
    var server_ctx = ServerCtx{
        .allocator = alloc,
        .services = services,
        .plan = &plan,
        .route_id = rid,
    };

    // Create server socket manually for the test
    const sock_rc = posix.system.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const server_fd: posix.fd_t = @intCast(sock_rc);

    var bind_addr: posix.sockaddr.un = .{ .path = undefined };
    @memset(&bind_addr.path, 0);
    @memcpy(bind_addr.path[0..sock_path.len], sock_path);

    const bind_rc = posix.system.bind(server_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.un));
    if (posix.errno(bind_rc) != .SUCCESS) {
        posix.close(server_fd);
        return error.BindFailed;
    }

    const listen_rc = posix.system.listen(server_fd, 1);
    if (posix.errno(listen_rc) != .SUCCESS) {
        posix.close(server_fd);
        return error.ListenFailed;
    }

    // Server thread: accept one connection, run route, send response
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(sctx: *ServerCtx, sfd: posix.fd_t) void {
            defer posix.close(sfd);
            defer sctx.done.set();

            const accept_rc = posix.system.accept(sfd, null, null);
            if (posix.errno(accept_rc) != .SUCCESS) return;
            const cfd: posix.fd_t = @intCast(accept_rc);
            defer posix.close(cfd);

            // Read data
            var buf: [4096]u8 = undefined;
            var total: usize = 0;
            while (true) {
                const n_rc = posix.system.recvfrom(cfd, buf[total..].ptr, buf.len - total, 0, null, null);
                if (posix.errno(n_rc) != .SUCCESS or n_rc == 0) break;
                total += @intCast(n_rc);
            }

            // Create exchange and run route
            var ex = Exchange.init(sctx.allocator);
            defer ex.deinit();
            ex.setBody(buf[0..total]) catch return;

            var exec = SyncExecutor.init(sctx.services);
            defer exec.deinit();
            exec.run(sctx.plan, sctx.route_id, &ex) catch {};

            // Send response
            if (ex.body.len > 0) sendAll(cfd, ex.body) catch {};
        }
    }.run, .{ &server_ctx, server_fd });

    // Client: connect and send data
    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello unix");

    const ep = try makeUnixEndpoint(alloc, sock_path);
    defer ep.deinit(alloc);
    const prod = try ep.createProducer(alloc);
    try prod.send(&ex);

    // Wait for server
    server_thread.join();

    // Cleanup socket file
    _ = posix.system.unlinkat(posix.AT.FDCWD, @ptrCast(&unlink_buf), 0);

    try std.testing.expectEqualStrings("HELLO UNIX", ex.body);
    try std.testing.expectEqualStrings(sock_path, ex.getHeader("CamelUnixSocketPath").?);
}
