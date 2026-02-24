const std = @import("std");
const posix = std.posix;
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

// -------- address parsing --------

pub const TcpAddress = struct {
    host: []const u8,
    port: u16,
};

pub fn parseTcpAddress(rest: []const u8) !TcpAddress {
    var clean = rest;
    // Strip leading "//" if present
    if (std.mem.startsWith(u8, clean, "//")) clean = clean[2..];

    // Strip query params for parsing
    const qmark = std.mem.indexOfScalar(u8, clean, '?');
    const addr_part = if (qmark) |q| clean[0..q] else clean;

    // Find last ':' for port separator
    const colon = std.mem.lastIndexOfScalar(u8, addr_part, ':') orelse return error.InvalidTcpAddress;
    if (colon == 0 or colon + 1 >= addr_part.len) return error.InvalidTcpAddress;

    const host = addr_part[0..colon];
    const port = std.fmt.parseUnsigned(u16, addr_part[colon + 1 ..], 10) catch return error.InvalidTcpAddress;

    return .{ .host = host, .port = port };
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

// -------- IP parsing (duplicated from http.zig for independence) --------

fn parseIp4(host: []const u8) ?[4]u8 {
    var result: [4]u8 = undefined;
    var idx: usize = 0;
    var it = std.mem.splitScalar(u8, host, '.');
    while (it.next()) |part| {
        if (idx >= 4) return null;
        result[idx] = std.fmt.parseUnsigned(u8, part, 10) catch return null;
        idx += 1;
    }
    if (idx != 4) return null;
    return result;
}

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

// -------- TCP Producer (client) --------

const TcpCtx = struct {
    host: []const u8,
    port: u16,
};

pub fn makeTcpEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    const addr = try parseTcpAddress(rest);
    const ctx = try allocator.create(TcpCtx);
    ctx.* = .{ .host = addr.host, .port = addr.port };
    return .{
        .ctx = ctx,
        .createProducerFn = tcpCreateProducer,
        .deinitFn = tcpDeinit,
    };
}

fn tcpDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *TcpCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn tcpCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = tcpSend };
}

fn tcpSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const TcpCtx = @ptrCast(@alignCast(ctx.?));

    const ip4_bytes = parseIp4(c.host) orelse return error.TcpRequiresNumericIp;
    const ip4_addr: u32 = @bitCast(ip4_bytes);
    const port_be = std.mem.nativeToBig(u16, c.port);

    var addr: posix.sockaddr.in = .{
        .port = port_be,
        .addr = ip4_addr,
    };

    const sock_rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const stream: posix.fd_t = @intCast(sock_rc);
    defer posix.close(stream);

    try posix.connect(stream, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));

    // Send exchange body
    if (ex.body.len > 0) try sendAll(stream, ex.body);

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

    // Set remote address header
    var addr_buf: [64]u8 = undefined;
    const addr_str = std.fmt.bufPrint(&addr_buf, "{d}.{d}.{d}.{d}:{d}", .{
        ip4_bytes[0], ip4_bytes[1], ip4_bytes[2], ip4_bytes[3], c.port,
    }) catch "unknown";
    try ex.putHeader("CamelTcpRemoteAddr", addr_str);
}

// -------- TCP Consumer (server) --------

const TcpConsumerCtx = struct {
    port: u16,
    max_connections: u32,
};

pub fn makeTcpConsumer(allocator: std.mem.Allocator, rest: []const u8) !Consumer {
    const addr = try parseTcpAddress(rest);

    var max_connections: u32 = 1;
    if (parseQueryParam(rest, "connections")) |val| {
        max_connections = std.fmt.parseUnsigned(u32, val, 10) catch 1;
    }

    const ctx = try allocator.create(TcpConsumerCtx);
    ctx.* = .{ .port = addr.port, .max_connections = max_connections };

    return .{
        .ctx = ctx,
        .startFn = tcpConsumerStart,
        .deinitFn = tcpConsumerDeinit,
    };
}

fn tcpConsumerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *TcpConsumerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn tcpConsumerStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan_ptr: *const RoutePlan, route_id: u32) !void {
    const c: *const TcpConsumerCtx = @ptrCast(@alignCast(ctx.?));

    const port_be = std.mem.nativeToBig(u16, c.port);

    // Create server socket
    const sock_rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const server_fd: posix.fd_t = @intCast(sock_rc);
    defer posix.close(server_fd);

    // Set SO_REUSEADDR
    const one: c_int = 1;
    _ = posix.system.setsockopt(server_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one), @sizeOf(c_int));

    // Bind
    var bind_addr: posix.sockaddr.in = .{
        .port = port_be,
        .addr = 0, // INADDR_ANY
    };
    const bind_rc = posix.system.bind(server_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in));
    if (posix.errno(bind_rc) != .SUCCESS) return error.BindFailed;

    // Listen
    const listen_rc = posix.system.listen(server_fd, 8);
    if (posix.errno(listen_rc) != .SUCCESS) return error.ListenFailed;

    // Accept loop
    var connections_served: u32 = 0;
    while (connections_served < c.max_connections) {
        var client_addr: posix.sockaddr.in = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        const accept_rc = posix.system.accept(server_fd, @ptrCast(&client_addr), &addr_len);
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
            // For TCP, if we got less than buffer, assume message complete
            if (n < recv_buf.len) break;
        }

        // Create exchange
        var ex = Exchange.init(allocator);
        defer ex.deinit();
        if (data_buf.items.len > 0) try ex.setBody(data_buf.items);

        // Set headers
        var port_buf: [8]u8 = undefined;
        const port_str = std.fmt.bufPrint(&port_buf, "{d}", .{c.port}) catch "0";
        try ex.putHeader("CamelTcpLocalPort", port_str);

        // Extract client address
        const client_ip_raw: [4]u8 = @bitCast(client_addr.addr);
        var client_addr_buf: [64]u8 = undefined;
        const client_addr_str = std.fmt.bufPrint(&client_addr_buf, "{d}.{d}.{d}.{d}:{d}", .{
            client_ip_raw[0], client_ip_raw[1], client_ip_raw[2], client_ip_raw[3],
            std.mem.bigToNative(u16, client_addr.port),
        }) catch "unknown";
        try ex.putHeader("CamelTcpRemoteAddr", client_addr_str);

        // Run route
        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        exec.run(plan_ptr, route_id, &ex) catch {};

        // Send response back
        if (ex.body.len > 0) sendAll(client_fd, ex.body) catch {};

        connections_served += 1;
    }
}

// -------- tests --------

test "parseTcpAddress parses host:port" {
    const addr = try parseTcpAddress("127.0.0.1:9000");
    try std.testing.expectEqualStrings("127.0.0.1", addr.host);
    try std.testing.expectEqual(@as(u16, 9000), addr.port);
}

test "parseTcpAddress strips leading //" {
    const addr = try parseTcpAddress("//127.0.0.1:8080");
    try std.testing.expectEqualStrings("127.0.0.1", addr.host);
    try std.testing.expectEqual(@as(u16, 8080), addr.port);
}

test "parseTcpAddress with query params" {
    const addr = try parseTcpAddress("127.0.0.1:9000?connections=5");
    try std.testing.expectEqualStrings("127.0.0.1", addr.host);
    try std.testing.expectEqual(@as(u16, 9000), addr.port);
}

test "parseTcpAddress rejects invalid input" {
    try std.testing.expectError(error.InvalidTcpAddress, parseTcpAddress("noport"));
    try std.testing.expectError(error.InvalidTcpAddress, parseTcpAddress(":9000"));
    try std.testing.expectError(error.InvalidTcpAddress, parseTcpAddress("host:"));
}

test "parseQueryParam extracts value" {
    try std.testing.expectEqualStrings("5", parseQueryParam("127.0.0.1:9000?connections=5", "connections").?);
    try std.testing.expect(parseQueryParam("127.0.0.1:9000?connections=5", "missing") == null);
    try std.testing.expect(parseQueryParam("127.0.0.1:9000", "connections") == null);
}

test "tcp producer-consumer roundtrip" {
    const alloc = std.testing.allocator;

    // Start server in background thread
    const ServerCtx = struct {
        port: u16,
        allocator: std.mem.Allocator,
        done: std.Thread.ResetEvent = .unset,
    };
    var server_ctx = ServerCtx{ .port = 0, .allocator = alloc };

    // Create server socket first to get ephemeral port
    const sock_rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const server_fd: posix.fd_t = @intCast(sock_rc);

    const one: c_int = 1;
    _ = posix.system.setsockopt(server_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one), @sizeOf(c_int));

    var bind_addr: posix.sockaddr.in = .{
        .port = 0, // Ephemeral
        .addr = std.mem.nativeToBig(u32, 0x7f000001), // 127.0.0.1
    };
    const bind_rc = posix.system.bind(server_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in));
    if (posix.errno(bind_rc) != .SUCCESS) {
        posix.close(server_fd);
        return error.BindFailed;
    }

    // Get assigned port
    var actual_addr: posix.sockaddr.in = undefined;
    var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    _ = posix.system.getsockname(server_fd, @ptrCast(&actual_addr), &addr_len);
    server_ctx.port = std.mem.bigToNative(u16, actual_addr.port);

    const listen_rc = posix.system.listen(server_fd, 1);
    if (posix.errno(listen_rc) != .SUCCESS) {
        posix.close(server_fd);
        return error.ListenFailed;
    }

    // Server thread: accept one connection, echo back uppercased
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(sctx: *ServerCtx, sfd: posix.fd_t) void {
            defer posix.close(sfd);
            defer sctx.done.set();

            var client_addr: posix.sockaddr.in = undefined;
            var al: posix.socklen_t = @sizeOf(posix.sockaddr.in);
            const accept_rc = posix.system.accept(sfd, @ptrCast(&client_addr), &al);
            if (posix.errno(accept_rc) != .SUCCESS) return;
            const cfd: posix.fd_t = @intCast(accept_rc);
            defer posix.close(cfd);

            var buf: [4096]u8 = undefined;
            const n_rc = posix.system.recvfrom(cfd, &buf, buf.len, 0, null, null);
            if (posix.errno(n_rc) != .SUCCESS or n_rc == 0) return;
            const n: usize = @intCast(n_rc);

            // Uppercase and send back
            for (buf[0..n]) |*ch| {
                if (ch.* >= 'a' and ch.* <= 'z') ch.* -= 32;
            }
            sendAll(cfd, buf[0..n]) catch {};
        }
    }.run, .{ &server_ctx, server_fd });

    // Client: connect and send data
    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello tcp");

    // Build address string
    var addr_str_buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&addr_str_buf, "127.0.0.1:{d}", .{server_ctx.port});

    const ep = try makeTcpEndpoint(alloc, addr_str);
    defer ep.deinit(alloc);
    const prod = try ep.createProducer(alloc);
    try prod.send(&ex);

    // Wait for server
    server_thread.join();

    try std.testing.expectEqualStrings("HELLO TCP", ex.body);
    try std.testing.expect(ex.getHeader("CamelTcpRemoteAddr") != null);
}
