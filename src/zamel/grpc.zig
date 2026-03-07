const std = @import("std");
const posix = std.posix;
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;

/// gRPC endpoint for unary RPC calls over HTTP/2-like framing.
/// Uses a simplified wire format: length-prefixed messages over TCP.
///
/// URI: grpc:host:port/service/method
///
/// Wire format (gRPC over HTTP/2 DATA frame, simplified):
///   [1 byte: compressed flag (0)] [4 bytes: message length, big-endian] [N bytes: message]
///
/// The exchange body is sent as the request message.
/// The response message is set back as the exchange body.
/// Headers set:
///   CamelGrpcService: service name
///   CamelGrpcMethod: method name
///   CamelGrpcStatus: "0" on success

const GrpcCtx = struct {
    host: []const u8,
    port: u16,
    service: []const u8,
    method: []const u8,
};

pub fn makeGrpcEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    const parsed = try parseGrpcUrl(rest);
    const ctx = try allocator.create(GrpcCtx);
    ctx.* = parsed;
    return .{
        .ctx = ctx,
        .createProducerFn = grpcCreateProducer,
        .deinitFn = grpcDeinit,
    };
}

fn grpcDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *GrpcCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn grpcCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = grpcSend };
}

fn grpcSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const GrpcCtx = @ptrCast(@alignCast(ctx.?));

    try ex.putHeader("CamelGrpcService", c.service);
    try ex.putHeader("CamelGrpcMethod", c.method);

    const ip4_bytes = parseIp4(c.host) orelse return error.GrpcRequiresNumericIp;
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

    // Send HTTP/2 connection preface
    try sendAll(stream, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

    // Send HEADERS frame (simplified - just the path)
    const path = try std.fmt.allocPrint(ex.allocator, "/{s}/{s}", .{ c.service, c.method });
    defer ex.allocator.free(path);
    try sendHeadersFrame(stream, path, ex.allocator);

    // Send gRPC message as DATA frame
    try sendGrpcMessage(stream, ex.body, ex.allocator);

    // Read response
    var resp_buf: [8192]u8 = undefined;
    const resp_rc = posix.system.recvfrom(stream, &resp_buf, resp_buf.len, 0, null, null);
    if (posix.errno(resp_rc) != .SUCCESS) {
        try ex.putHeader("CamelGrpcStatus", "14"); // UNAVAILABLE
        return;
    }
    const resp_n: usize = @intCast(resp_rc);

    // Try to extract gRPC message from response
    if (resp_n > 0) {
        if (extractGrpcMessage(resp_buf[0..resp_n])) |msg| {
            try ex.setBody(msg);
            try ex.putHeader("CamelGrpcStatus", "0"); // OK
        } else {
            // Raw response
            try ex.setBody(resp_buf[0..resp_n]);
            try ex.putHeader("CamelGrpcStatus", "0");
        }
    }
}

/// Encode a gRPC length-prefixed message.
/// Format: [0x00] [4-byte big-endian length] [message bytes]
pub fn encodeGrpcMessage(data: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const frame = try allocator.alloc(u8, 5 + data.len);
    frame[0] = 0; // not compressed
    const len_be = std.mem.nativeToBig(u32, @intCast(data.len));
    @memcpy(frame[1..5], std.mem.asBytes(&len_be));
    @memcpy(frame[5..], data);
    return frame;
}

/// Decode a gRPC length-prefixed message.
/// Returns the message payload, or null if the frame is malformed.
pub fn decodeGrpcMessage(data: []const u8) ?[]const u8 {
    if (data.len < 5) return null;
    // data[0] = compressed flag
    const len_bytes = data[1..5];
    const msg_len = std.mem.readInt(u32, len_bytes, .big);
    if (5 + msg_len > data.len) return null;
    return data[5 .. 5 + msg_len];
}

fn sendGrpcMessage(fd: posix.fd_t, data: []const u8, allocator: std.mem.Allocator) !void {
    const frame = try encodeGrpcMessage(data, allocator);
    defer allocator.free(frame);

    // Wrap in HTTP/2 DATA frame (type=0x00)
    var h2_frame: [9]u8 = undefined;
    const payload_len: u24 = @intCast(frame.len);
    // 3 bytes length
    h2_frame[0] = @intCast((payload_len >> 16) & 0xFF);
    h2_frame[1] = @intCast((payload_len >> 8) & 0xFF);
    h2_frame[2] = @intCast(payload_len & 0xFF);
    h2_frame[3] = 0x00; // DATA type
    h2_frame[4] = 0x01; // END_STREAM flag
    // Stream ID = 1
    h2_frame[5] = 0;
    h2_frame[6] = 0;
    h2_frame[7] = 0;
    h2_frame[8] = 1;

    try sendAll(fd, &h2_frame);
    try sendAll(fd, frame);
}

fn sendHeadersFrame(fd: posix.fd_t, path: []const u8, allocator: std.mem.Allocator) !void {
    // Simplified HEADERS frame with HPACK-lite encoding
    // Just encode :method POST, :path, content-type application/grpc
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    // :method POST (indexed header 3)
    try payload.append(allocator, 0x83);
    // :path (literal, name index 4)
    try payload.append(allocator, 0x04);
    try payload.append(allocator, @intCast(path.len));
    try payload.appendSlice(allocator, path);
    // content-type: application/grpc (literal)
    try payload.append(allocator, 0x00);
    const ct_name = "content-type";
    try payload.append(allocator, @intCast(ct_name.len));
    try payload.appendSlice(allocator, ct_name);
    const ct_val = "application/grpc";
    try payload.append(allocator, @intCast(ct_val.len));
    try payload.appendSlice(allocator, ct_val);
    // te: trailers
    try payload.append(allocator, 0x00);
    const te_name = "te";
    try payload.append(allocator, @intCast(te_name.len));
    try payload.appendSlice(allocator, te_name);
    const te_val = "trailers";
    try payload.append(allocator, @intCast(te_val.len));
    try payload.appendSlice(allocator, te_val);

    // HTTP/2 HEADERS frame
    var h2_frame: [9]u8 = undefined;
    const plen: u24 = @intCast(payload.items.len);
    h2_frame[0] = @intCast((plen >> 16) & 0xFF);
    h2_frame[1] = @intCast((plen >> 8) & 0xFF);
    h2_frame[2] = @intCast(plen & 0xFF);
    h2_frame[3] = 0x01; // HEADERS type
    h2_frame[4] = 0x04; // END_HEADERS flag
    h2_frame[5] = 0;
    h2_frame[6] = 0;
    h2_frame[7] = 0;
    h2_frame[8] = 1; // Stream ID = 1

    try sendAll(fd, &h2_frame);
    try sendAll(fd, payload.items);
}

fn extractGrpcMessage(data: []const u8) ?[]const u8 {
    // Scan for gRPC message within HTTP/2 frames
    var i: usize = 0;
    while (i + 9 <= data.len) {
        const frame_len = (@as(u24, data[i]) << 16) | (@as(u24, data[i + 1]) << 8) | @as(u24, data[i + 2]);
        const frame_type = data[i + 3];
        if (frame_type == 0x00) { // DATA frame
            const payload_start = i + 9;
            const payload_end = payload_start + frame_len;
            if (payload_end <= data.len) {
                return decodeGrpcMessage(data[payload_start..payload_end]);
            }
        }
        i += 9 + frame_len;
    }
    // Fallback: try decoding from beginning (raw gRPC message without HTTP/2 framing)
    return decodeGrpcMessage(data);
}

fn parseGrpcUrl(rest: []const u8) !GrpcCtx {
    var clean = rest;
    if (std.mem.startsWith(u8, clean, "//")) clean = clean[2..];

    // host:port/service/method
    const colon = std.mem.indexOfScalar(u8, clean, ':') orelse return error.InvalidGrpcUrl;
    const host = clean[0..colon];

    const after_colon = clean[colon + 1 ..];
    const first_slash = std.mem.indexOfScalar(u8, after_colon, '/') orelse return error.InvalidGrpcUrl;
    const port_str = after_colon[0..first_slash];
    const port = std.fmt.parseUnsigned(u16, port_str, 10) catch return error.InvalidGrpcUrl;

    const path = after_colon[first_slash + 1 ..];
    const second_slash = std.mem.indexOfScalar(u8, path, '/') orelse return error.InvalidGrpcUrl;
    const service = path[0..second_slash];
    const method = path[second_slash + 1 ..];
    if (method.len == 0) return error.InvalidGrpcUrl;

    return .{ .host = host, .port = port, .service = service, .method = method };
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

test "parseGrpcUrl" {
    const parsed = try parseGrpcUrl("//127.0.0.1:50051/mypackage.MyService/MyMethod");
    try std.testing.expectEqualStrings("127.0.0.1", parsed.host);
    try std.testing.expectEqual(@as(u16, 50051), parsed.port);
    try std.testing.expectEqualStrings("mypackage.MyService", parsed.service);
    try std.testing.expectEqualStrings("MyMethod", parsed.method);
}

test "encodeGrpcMessage and decodeGrpcMessage roundtrip" {
    const alloc = std.testing.allocator;
    const msg = "hello gRPC";

    const encoded = try encodeGrpcMessage(msg, alloc);
    defer alloc.free(encoded);

    try std.testing.expectEqual(@as(usize, 5 + msg.len), encoded.len);
    try std.testing.expectEqual(@as(u8, 0), encoded[0]); // not compressed

    const decoded = decodeGrpcMessage(encoded);
    try std.testing.expect(decoded != null);
    try std.testing.expectEqualStrings(msg, decoded.?);
}

test "encodeGrpcMessage empty" {
    const alloc = std.testing.allocator;
    const encoded = try encodeGrpcMessage("", alloc);
    defer alloc.free(encoded);

    try std.testing.expectEqual(@as(usize, 5), encoded.len);
    const decoded = decodeGrpcMessage(encoded);
    try std.testing.expect(decoded != null);
    try std.testing.expectEqual(@as(usize, 0), decoded.?.len);
}

test "decodeGrpcMessage too short" {
    const data = [_]u8{ 0, 0, 0 };
    try std.testing.expect(decodeGrpcMessage(&data) == null);
}
