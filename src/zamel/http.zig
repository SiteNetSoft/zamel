const std = @import("std");
const posix = std.posix;
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;

const HttpCtx = struct {
    url: []const u8,
};

pub fn makeHttpEndpoint(allocator: std.mem.Allocator, url: []const u8) !Endpoint {
    const ctx = try allocator.create(HttpCtx);
    ctx.* = .{ .url = url };
    return .{
        .ctx = ctx,
        .createProducerFn = httpCreateProducer,
        .deinitFn = httpDeinit,
    };
}

fn httpDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *HttpCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn httpCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = httpSend };
}

fn httpSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const HttpCtx = @ptrCast(@alignCast(ctx.?));

    const parsed = parseHttpUrl(c.url) orelse return error.InvalidHttpUrl;

    // Determine method from header or body presence
    const method = if (ex.getHeader("CamelHttpMethod")) |m| m else if (ex.body.len > 0) "POST" else "GET";

    // Build request
    const req_line = try std.fmt.allocPrint(ex.allocator, "{s} {s} HTTP/1.1\r\nHost: {s}\r\nConnection: close\r\nContent-Length: {d}\r\n\r\n", .{
        method,
        parsed.path,
        parsed.host,
        ex.body.len,
    });
    defer ex.allocator.free(req_line);

    // Parse IPv4 address from host (MVP: numeric IPs only, no DNS)
    const ip4_bytes = parseIp4(parsed.host) orelse return error.HttpRequiresNumericIp;
    const ip4_addr: u32 = @bitCast(ip4_bytes);
    const port_be = std.mem.nativeToBig(u16, parsed.port);

    var addr: posix.sockaddr.in = .{
        .port = port_be,
        .addr = ip4_addr,
    };

    // Create socket via syscall
    const sock_rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    if (posix.errno(sock_rc) != .SUCCESS) return error.SocketFailed;
    const stream: posix.fd_t = @intCast(sock_rc);
    defer posix.close(stream);

    const connect_rc = posix.system.connect(stream, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    if (posix.errno(connect_rc) != .SUCCESS) return error.ConnectFailed;

    // Send request line + headers
    try sendAll(stream, req_line);
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

    // Parse response status and body
    const resp = response_buf.items;
    if (std.mem.indexOf(u8, resp, "\r\n")) |status_end| {
        const status_line = resp[0..status_end];
        if (std.mem.indexOf(u8, status_line, " ")) |space1| {
            const after_space = status_line[space1 + 1 ..];
            if (std.mem.indexOf(u8, after_space, " ")) |space2| {
                try ex.putHeader("CamelHttpResponseCode", after_space[0..space2]);
            }
        }
    }

    // Extract body (after \r\n\r\n)
    if (std.mem.indexOf(u8, resp, "\r\n\r\n")) |body_start| {
        try ex.setBody(resp[body_start + 4 ..]);
    }
}

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

pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    path: []const u8,
};

pub fn parseHttpUrl(url: []const u8) ?ParsedUrl {
    var rest = url;
    var default_port: u16 = 80;
    if (std.mem.startsWith(u8, rest, "https://")) {
        rest = rest["https://".len..];
        default_port = 443;
    } else if (std.mem.startsWith(u8, rest, "http://")) {
        rest = rest["http://".len..];
    } else return null;

    const path_start = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
    const host_port = rest[0..path_start];
    const path = if (path_start < rest.len) rest[path_start..] else "/";

    if (std.mem.indexOfScalar(u8, host_port, ':')) |colon| {
        const port = std.fmt.parseUnsigned(u16, host_port[colon + 1 ..], 10) catch return null;
        return .{ .host = host_port[0..colon], .port = port, .path = path };
    }
    return .{ .host = host_port, .port = default_port, .path = path };
}

// -------- tests --------

test "parseHttpUrl parses basic URL" {
    const p = parseHttpUrl("http://example.com/path").?;
    try std.testing.expectEqualStrings("example.com", p.host);
    try std.testing.expectEqual(@as(u16, 80), p.port);
    try std.testing.expectEqualStrings("/path", p.path);
}

test "parseHttpUrl parses URL with port" {
    const p = parseHttpUrl("http://localhost:8080/api").?;
    try std.testing.expectEqualStrings("localhost", p.host);
    try std.testing.expectEqual(@as(u16, 8080), p.port);
    try std.testing.expectEqualStrings("/api", p.path);
}

test "parseHttpUrl parses https URL" {
    const p = parseHttpUrl("https://secure.example.com/").?;
    try std.testing.expectEqualStrings("secure.example.com", p.host);
    try std.testing.expectEqual(@as(u16, 443), p.port);
    try std.testing.expectEqualStrings("/", p.path);
}

test "parseHttpUrl returns null for invalid URL" {
    try std.testing.expect(parseHttpUrl("ftp://bad.com") == null);
}

test "parseHttpUrl handles URL without path" {
    const p = parseHttpUrl("http://example.com").?;
    try std.testing.expectEqualStrings("example.com", p.host);
    try std.testing.expectEqualStrings("/", p.path);
}

test "parseIp4 parses valid IPv4" {
    const ip = parseIp4("127.0.0.1").?;
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, ip);
}

test "parseIp4 returns null for hostname" {
    try std.testing.expect(parseIp4("example.com") == null);
}
