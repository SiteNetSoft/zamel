const std = @import("std");
const Processor = @import("processor.zig").Processor;
const Exchange = @import("exchange.zig").Exchange;

/// Comptime processor: replace exchange body with a static value. No allocator needed.
pub fn setBody(comptime value: []const u8) Processor {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!void {
            try ex.setBody(value);
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

/// Comptime processor: set a header to a static value. No allocator needed.
pub fn setHeader(comptime key: []const u8, comptime value: []const u8) Processor {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!void {
            try ex.putHeader(key, value);
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

/// Comptime processor: remove a header by key. No allocator needed.
pub fn removeHeader(comptime key: []const u8) Processor {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!void {
            if (ex.headers.get(key)) |old| ex.allocator.free(old);
            _ = ex.headers.remove(key);
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

// -------- tests --------

test "ct setBody replaces exchange body" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("original");

    const p = comptime setBody("replaced");
    try p.call(&ex);
    try std.testing.expectEqualStrings("replaced", ex.body);
}

test "ct setHeader sets a header value" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const p = comptime setHeader("Content-Type", "text/plain");
    try p.call(&ex);
    try std.testing.expectEqualStrings("text/plain", ex.getHeader("Content-Type").?);
}

test "ct removeHeader removes an existing header" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("temp", "value");

    const p = comptime removeHeader("temp");
    try p.call(&ex);
    try std.testing.expect(ex.getHeader("temp") == null);
}

test "ct removeHeader is no-op for missing header" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const p = comptime removeHeader("nonexistent");
    try p.call(&ex);
    try std.testing.expect(ex.getHeader("nonexistent") == null);
}
