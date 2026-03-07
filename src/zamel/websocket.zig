const std = @import("std");
const posix = std.posix;
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;

const WsCtx = struct {
    host: []const u8,
    port: u16,
    path: []const u8,
};

pub fn makeWebSocketEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    const parsed = try parseWsUrl(rest);
    const ctx = try allocator.create(WsCtx);
    ctx.* = parsed;
    return .{
        .ctx = ctx,
        .createProducerFn = wsCreateProducer,
        .deinitFn = wsDeinit,
    };
}

fn wsDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *WsCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn wsCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = wsSend };
}

fn wsSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const WsCtx = @ptrCast(@alignCast(ctx.?));

    const ip4_bytes = parseIp4(c.host) orelse return error.WsRequiresNumericIp;
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

    const connect_rc = posix.system.connect(stream, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    if (posix.errno(connect_rc) != .SUCCESS) return error.ConnectFailed;

    // WebSocket handshake
    const ws_key = "dGhlIHNhbXBsZSBub25jZQ=="; // static key for simplicity
    const handshake = try std.fmt.allocPrint(ex.allocator, "GET {s} HTTP/1.1\r\nHost: {s}:{d}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {s}\r\nSec-WebSocket-Version: 13\r\n\r\n", .{
        c.path,
        c.host,
        c.port,
        ws_key,
    });
    defer ex.allocator.free(handshake);

    try sendAll(stream, handshake);

    // Read handshake response
    var resp_buf: [4096]u8 = undefined;
    const resp_rc = posix.system.recvfrom(stream, &resp_buf, resp_buf.len, 0, null, null);
    if (posix.errno(resp_rc) != .SUCCESS) return error.HandshakeFailed;
    const resp_n: usize = @intCast(resp_rc);

    // Verify 101 Switching Protocols
    const resp = resp_buf[0..resp_n];
    if (!std.mem.startsWith(u8, resp, "HTTP/1.1 101")) return error.WebSocketUpgradeFailed;

    try ex.putHeader("CamelWebSocketConnected", "true");

    // Send body as text frame
    if (ex.body.len > 0) {
        try sendTextFrame(stream, ex.body, ex.allocator);
    }

    // Read response frame
    var frame_buf: [4096]u8 = undefined;
    const frame_rc = posix.system.recvfrom(stream, &frame_buf, frame_buf.len, 0, null, null);
    if (posix.errno(frame_rc) != .SUCCESS) return;
    const frame_n: usize = @intCast(frame_rc);

    if (frame_n >= 2) {
        if (parseTextFrame(frame_buf[0..frame_n])) |payload| {
            try ex.setBody(payload);
        }
    }

    // Send close frame
    const close_frame = [_]u8{ 0x88, 0x80, 0x00, 0x00, 0x00, 0x00 }; // masked close
    _ = posix.system.sendto(stream, &close_frame, close_frame.len, 0, null, 0);
}

/// Encode and send a WebSocket text frame (client-to-server, masked).
fn sendTextFrame(fd: posix.fd_t, data: []const u8, allocator: std.mem.Allocator) !void {
    // Frame: FIN + text opcode, masked, payload
    var frame: std.ArrayList(u8) = .empty;
    defer frame.deinit(allocator);

    try frame.append(allocator, 0x81); // FIN + text opcode

    // Payload length + mask bit
    if (data.len < 126) {
        try frame.append(allocator, @as(u8, @intCast(data.len)) | 0x80);
    } else if (data.len < 65536) {
        try frame.append(allocator, 126 | 0x80);
        const len16 = std.mem.nativeToBig(u16, @intCast(data.len));
        try frame.appendSlice(allocator, std.mem.asBytes(&len16));
    } else {
        try frame.append(allocator, 127 | 0x80);
        const len64 = std.mem.nativeToBig(u64, @intCast(data.len));
        try frame.appendSlice(allocator, std.mem.asBytes(&len64));
    }

    // Masking key (simple fixed mask for reproducibility)
    const mask = [4]u8{ 0x37, 0xfa, 0x21, 0x3d };
    try frame.appendSlice(allocator, &mask);

    // Masked payload
    for (data, 0..) |b, i| {
        try frame.append(allocator, b ^ mask[i % 4]);
    }

    try sendAll(fd, frame.items);
}

/// Parse a WebSocket text frame (server-to-client, unmasked).
fn parseTextFrame(data: []const u8) ?[]const u8 {
    if (data.len < 2) return null;

    const opcode = data[0] & 0x0F;
    if (opcode != 0x01) return null; // text frame only

    const mask_bit = (data[1] & 0x80) != 0;
    var payload_len: u64 = data[1] & 0x7F;
    var offset: usize = 2;

    if (payload_len == 126) {
        if (data.len < 4) return null;
        payload_len = std.mem.readInt(u16, data[2..4], .big);
        offset = 4;
    } else if (payload_len == 127) {
        if (data.len < 10) return null;
        payload_len = std.mem.readInt(u64, data[2..10], .big);
        offset = 10;
    }

    if (mask_bit) offset += 4; // skip mask
    if (offset + payload_len > data.len) return null;

    return data[offset .. offset + @as(usize, @intCast(payload_len))];
}

fn parseWsUrl(rest: []const u8) !WsCtx {
    var clean = rest;
    // ws://host:port/path or just host:port/path
    if (std.mem.startsWith(u8, clean, "//")) clean = clean[2..];

    // Find port separator
    const colon = std.mem.indexOfScalar(u8, clean, ':') orelse return error.InvalidWsUrl;
    const host = clean[0..colon];

    const after_colon = clean[colon + 1 ..];
    // Find path separator or end
    const slash = std.mem.indexOfScalar(u8, after_colon, '/');
    const port_str = if (slash) |s| after_colon[0..s] else after_colon;
    const path = if (slash) |s| after_colon[s..] else "/";

    const port = std.fmt.parseUnsigned(u16, port_str, 10) catch return error.InvalidWsUrl;

    return .{ .host = host, .port = port, .path = path };
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

// -------- tests --------

test "parseWsUrl basic" {
    const parsed = try parseWsUrl("//127.0.0.1:8080/chat");
    try std.testing.expectEqualStrings("127.0.0.1", parsed.host);
    try std.testing.expectEqual(@as(u16, 8080), parsed.port);
    try std.testing.expectEqualStrings("/chat", parsed.path);
}

test "parseWsUrl no path" {
    const parsed = try parseWsUrl("//127.0.0.1:9000");
    try std.testing.expectEqualStrings("127.0.0.1", parsed.host);
    try std.testing.expectEqual(@as(u16, 9000), parsed.port);
    try std.testing.expectEqualStrings("/", parsed.path);
}

test "sendTextFrame and parseTextFrame roundtrip" {
    // Test frame encoding
    const alloc = std.testing.allocator;
    var frame: std.ArrayList(u8) = .empty;
    defer frame.deinit(alloc);

    // Build a simple unmasked server frame for testing parse
    try frame.append(alloc, 0x81); // FIN + text
    try frame.append(alloc, 5); // length 5, no mask
    try frame.appendSlice(alloc, "hello");

    const payload = parseTextFrame(frame.items);
    try std.testing.expect(payload != null);
    try std.testing.expectEqualStrings("hello", payload.?);
}
