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

// -------- combinators --------

/// Predicate: NOT inner
pub fn predNot(allocator: std.mem.Allocator, inner: Predicate) !Predicate {
    const Ctx = struct { inner: Predicate };
    const p = try allocator.create(Ctx);
    p.* = .{ .inner = inner };
    return Predicate.fromFn(notCall, p);
}

fn notCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { inner: Predicate };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    return !(try c.inner.call(ex));
}

/// Predicate: a AND b (short-circuit)
pub fn predAnd(allocator: std.mem.Allocator, a: Predicate, b: Predicate) !Predicate {
    const Ctx = struct { a: Predicate, b: Predicate };
    const p = try allocator.create(Ctx);
    p.* = .{ .a = a, .b = b };
    return Predicate.fromFn(andCall, p);
}

fn andCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { a: Predicate, b: Predicate };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    if (!(try c.a.call(ex))) return false;
    return try c.b.call(ex);
}

/// Predicate: a OR b (short-circuit)
pub fn predOr(allocator: std.mem.Allocator, a: Predicate, b: Predicate) !Predicate {
    const Ctx = struct { a: Predicate, b: Predicate };
    const p = try allocator.create(Ctx);
    p.* = .{ .a = a, .b = b };
    return Predicate.fromFn(orCall, p);
}

fn orCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { a: Predicate, b: Predicate };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    if (try c.a.call(ex)) return true;
    return try c.b.call(ex);
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

test "predNot inverts predicate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "A");

    const inner = try headerEq(alloc, "type", "A");
    const pred = try predNot(alloc, inner);
    try std.testing.expect(!(try pred.call(&ex)));
}

test "predAnd short-circuits on false" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "A");
    try ex.setBody("hello world");

    const a = try headerEq(alloc, "type", "A");
    const b = try bodyContains(alloc, "world");
    const both = try predAnd(alloc, a, b);
    try std.testing.expect(try both.call(&ex));

    // Change header to make 'a' false
    try ex.putHeader("type", "B");
    try std.testing.expect(!(try both.call(&ex)));
}

test "predOr short-circuits on true" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "X");
    try ex.setBody("no match");

    const a = try headerEq(alloc, "type", "A");
    const b = try headerEq(alloc, "type", "X");
    const either = try predOr(alloc, a, b);
    try std.testing.expect(try either.call(&ex));

    // Neither matches
    try ex.putHeader("type", "Z");
    try std.testing.expect(!(try either.call(&ex)));
}
