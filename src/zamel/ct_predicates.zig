const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Exchange = @import("exchange.zig").Exchange;

/// Comptime predicate: header(key) == value. No allocator needed.
pub fn headerEq(comptime key: []const u8, comptime value: []const u8) Predicate {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!bool {
            const got = ex.getHeader(key) orelse return false;
            return std.mem.eql(u8, got, value);
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

/// Comptime predicate: body contains needle substring. No allocator needed.
pub fn bodyContains(comptime needle: []const u8) Predicate {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!bool {
            return std.mem.indexOf(u8, ex.body, needle) != null;
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

/// Comptime predicate: header exists (any value). No allocator needed.
pub fn headerExists(comptime key: []const u8) Predicate {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!bool {
            return ex.getHeader(key) != null;
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

/// Comptime predicate: body is empty. No allocator needed.
pub fn bodyEmpty() Predicate {
    const S = struct {
        fn call(_: ?*anyopaque, ex: *Exchange) anyerror!bool {
            return ex.body.len == 0;
        }
    };
    return .{ .ctx = null, .callFn = S.call };
}

// -------- tests --------

test "ct headerEq matches when header present and equal" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "A");

    const p = comptime headerEq("type", "A");
    try std.testing.expect(try p.call(&ex));
}

test "ct headerEq returns false on mismatch" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "B");

    const p = comptime headerEq("type", "A");
    try std.testing.expect(!(try p.call(&ex)));
}

test "ct headerEq returns false when header missing" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const p = comptime headerEq("type", "A");
    try std.testing.expect(!(try p.call(&ex)));
}

test "ct bodyContains matches when substring present" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello world");

    const p = comptime bodyContains("world");
    try std.testing.expect(try p.call(&ex));
}

test "ct bodyContains returns false when substring absent" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    const p = comptime bodyContains("world");
    try std.testing.expect(!(try p.call(&ex)));
}

test "ct headerExists returns true when present" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("X-Token", "abc");

    const p = comptime headerExists("X-Token");
    try std.testing.expect(try p.call(&ex));
}

test "ct headerExists returns false when absent" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const p = comptime headerExists("Missing");
    try std.testing.expect(!(try p.call(&ex)));
}

test "ct bodyEmpty returns true for empty body" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const p = comptime bodyEmpty();
    try std.testing.expect(try p.call(&ex));
}

test "ct bodyEmpty returns false for non-empty body" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("data");

    const p = comptime bodyEmpty();
    try std.testing.expect(!(try p.call(&ex)));
}
