const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Exchange = @import("exchange.zig").Exchange;

/// Predicate: header(key) == value
pub fn headerEq(allocator: std.mem.Allocator, key: []const u8, value: []const u8) !Predicate {
    const Ctx = struct { key: []const u8, value: []const u8 };

    const p = try allocator.create(Ctx);
    p.* = .{ .key = key, .value = value };

    return Predicate.fromFn(headerEqCall, p);
}

fn headerEqCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { key: []const u8, value: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));

    const got = ex.getHeader(c.key) orelse return false;
    return std.mem.eql(u8, got, c.value);
}

/// Predicate: body contains substring needle
pub fn bodyContains(allocator: std.mem.Allocator, needle: []const u8) !Predicate {
    const Ctx = struct { needle: []const u8 };

    const p = try allocator.create(Ctx);
    p.* = .{ .needle = needle };

    return Predicate.fromFn(bodyContainsCall, p);
}

fn bodyContainsCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { needle: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));

    return std.mem.indexOf(u8, ex.body, c.needle) != null;
}

// -------- tests --------

test "headerEq matches when header present and equal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "A");

    const pred = try headerEq(alloc, "type", "A");
    try std.testing.expect(try pred.call(&ex));
}

test "headerEq returns false on mismatch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "B");

    const pred = try headerEq(alloc, "type", "A");
    try std.testing.expect(!(try pred.call(&ex)));
}

test "headerEq returns false when header missing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const pred = try headerEq(alloc, "type", "A");
    try std.testing.expect(!(try pred.call(&ex)));
}

test "bodyContains matches when substring present" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello world");

    const pred = try bodyContains(alloc, "world");
    try std.testing.expect(try pred.call(&ex));
}

test "bodyContains returns false when substring absent" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    const pred = try bodyContains(alloc, "world");
    try std.testing.expect(!(try pred.call(&ex)));
}
