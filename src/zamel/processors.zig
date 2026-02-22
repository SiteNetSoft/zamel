const std = @import("std");
const Processor = @import("processor.zig").Processor;
const Exchange = @import("exchange.zig").Exchange;

/// Processor: replace exchange body with a static value
pub fn setBody(allocator: std.mem.Allocator, value: []const u8) !Processor {
    const Ctx = struct { value: []const u8 };
    const p = try allocator.create(Ctx);
    p.* = .{ .value = value };
    return Processor.fromFn(setBodyCall, p);
}

fn setBodyCall(ctx: ?*anyopaque, ex: *Exchange) !void {
    const Ctx = struct { value: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    try ex.setBody(c.value);
}

/// Processor: set a header to a static value
pub fn setHeader(allocator: std.mem.Allocator, key: []const u8, value: []const u8) !Processor {
    const Ctx = struct { key: []const u8, value: []const u8 };
    const p = try allocator.create(Ctx);
    p.* = .{ .key = key, .value = value };
    return Processor.fromFn(setHeaderCall, p);
}

fn setHeaderCall(ctx: ?*anyopaque, ex: *Exchange) !void {
    const Ctx = struct { key: []const u8, value: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    try ex.putHeader(c.key, c.value);
}

/// Processor: remove a header by key
pub fn removeHeader(allocator: std.mem.Allocator, key: []const u8) !Processor {
    const Ctx = struct { key: []const u8 };
    const p = try allocator.create(Ctx);
    p.* = .{ .key = key };
    return Processor.fromFn(removeHeaderCall, p);
}

fn removeHeaderCall(ctx: ?*anyopaque, ex: *Exchange) !void {
    const Ctx = struct { key: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    if (ex.headers.get(c.key)) |old| ex.allocator.free(old);
    _ = ex.headers.remove(c.key);
}

/// Processor: transform body using a user-supplied function
pub fn transformBody(
    allocator: std.mem.Allocator,
    transform_fn: *const fn (body: []const u8, alloc: std.mem.Allocator) anyerror![]u8,
) !Processor {
    const Ctx = struct {
        transform_fn: *const fn (body: []const u8, alloc: std.mem.Allocator) anyerror![]u8,
    };
    const p = try allocator.create(Ctx);
    p.* = .{ .transform_fn = transform_fn };
    return Processor.fromFn(transformBodyCall, p);
}

fn transformBodyCall(ctx: ?*anyopaque, ex: *Exchange) !void {
    const Ctx = struct {
        transform_fn: *const fn (body: []const u8, alloc: std.mem.Allocator) anyerror![]u8,
    };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
    const new_body = try c.transform_fn(ex.body, ex.allocator);
    if (ex.body.len != 0) ex.allocator.free(ex.body);
    ex.body = new_body;
}

// -------- tests --------

test "setBody replaces exchange body" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("original");

    const proc = try setBody(arena.allocator(), "replaced");
    try proc.call(&ex);
    try std.testing.expectEqualStrings("replaced", ex.body);
}

test "setHeader sets a header value" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const proc = try setHeader(arena.allocator(), "Content-Type", "text/plain");
    try proc.call(&ex);
    try std.testing.expectEqualStrings("text/plain", ex.getHeader("Content-Type").?);
}

test "removeHeader removes an existing header" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("temp", "value");

    const proc = try removeHeader(arena.allocator(), "temp");
    try proc.call(&ex);
    try std.testing.expect(ex.getHeader("temp") == null);
}

test "removeHeader is no-op for missing header" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    const proc = try removeHeader(arena.allocator(), "nonexistent");
    try proc.call(&ex);
    try std.testing.expect(ex.getHeader("nonexistent") == null);
}

test "transformBody applies user function" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    const proc = try transformBody(arena.allocator(), struct {
        fn transform(body: []const u8, alloc: std.mem.Allocator) ![]u8 {
            const result = try alloc.alloc(u8, body.len);
            for (body, 0..) |ch, i| {
                result[i] = if (ch >= 'a' and ch <= 'z') ch - 32 else ch;
            }
            return result;
        }
    }.transform);
    try proc.call(&ex);
    try std.testing.expectEqualStrings("HELLO", ex.body);
}
