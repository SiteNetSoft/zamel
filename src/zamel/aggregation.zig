const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;

/// Custom aggregation strategy following the fn_ptr + anyopaque pattern.
/// Called iteratively (fold) to combine exchange bodies during aggregation.
pub const AggregationStrategy = struct {
    aggregateFn: *const fn (ctx: ?*anyopaque, old_body: []const u8, new_body: []const u8, allocator: std.mem.Allocator) anyerror![]u8,
    ctx: ?*anyopaque = null,

    pub fn call(self: AggregationStrategy, old_body: []const u8, new_body: []const u8, allocator: std.mem.Allocator) ![]u8 {
        return self.aggregateFn(self.ctx, old_body, new_body, allocator);
    }

    pub fn fromFn(
        f: *const fn (ctx: ?*anyopaque, old_body: []const u8, new_body: []const u8, allocator: std.mem.Allocator) anyerror![]u8,
        ctx: ?*anyopaque,
    ) AggregationStrategy {
        return .{ .aggregateFn = f, .ctx = ctx };
    }
};

// -------- built-in strategies --------

/// Joins bodies with a separator string (fold: old + sep + new).
pub fn joinStrategy(allocator: std.mem.Allocator, separator: []const u8) !AggregationStrategy {
    const Ctx = struct { sep: []const u8 };
    const p = try allocator.create(Ctx);
    p.* = .{ .sep = separator };
    return .{
        .aggregateFn = struct {
            fn aggregate(ctx: ?*anyopaque, old_body: []const u8, new_body: []const u8, alloc: std.mem.Allocator) ![]u8 {
                const c: *const Ctx = @ptrCast(@alignCast(ctx.?));
                if (old_body.len == 0) return try alloc.dupe(u8, new_body);
                const total = old_body.len + c.sep.len + new_body.len;
                const result = try alloc.alloc(u8, total);
                @memcpy(result[0..old_body.len], old_body);
                @memcpy(result[old_body.len .. old_body.len + c.sep.len], c.sep);
                @memcpy(result[old_body.len + c.sep.len ..], new_body);
                return result;
            }
        }.aggregate,
        .ctx = p,
    };
}

/// Appends new body directly to old body (no separator).
pub fn appendStrategy() AggregationStrategy {
    return .{
        .aggregateFn = struct {
            fn aggregate(_: ?*anyopaque, old_body: []const u8, new_body: []const u8, alloc: std.mem.Allocator) ![]u8 {
                if (old_body.len == 0) return try alloc.dupe(u8, new_body);
                const total = old_body.len + new_body.len;
                const result = try alloc.alloc(u8, total);
                @memcpy(result[0..old_body.len], old_body);
                @memcpy(result[old_body.len..], new_body);
                return result;
            }
        }.aggregate,
        .ctx = null,
    };
}

/// Always takes the last (newest) body, discarding the old.
pub fn lastStrategy() AggregationStrategy {
    return .{
        .aggregateFn = struct {
            fn aggregate(_: ?*anyopaque, _: []const u8, new_body: []const u8, alloc: std.mem.Allocator) ![]u8 {
                return try alloc.dupe(u8, new_body);
            }
        }.aggregate,
        .ctx = null,
    };
}

// -------- tests --------

test "joinStrategy joins with separator" {
    const alloc = std.testing.allocator;
    const strat = try joinStrategy(alloc, ",");
    defer alloc.destroy(@as(*const struct { sep: []const u8 }, @ptrCast(@alignCast(strat.ctx.?))));

    // First call with empty old
    const r1 = try strat.call("", "aaa", alloc);
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("aaa", r1);

    // Second call
    const r2 = try strat.call("aaa", "bbb", alloc);
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("aaa,bbb", r2);

    // Third call
    const r3 = try strat.call("aaa,bbb", "ccc", alloc);
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("aaa,bbb,ccc", r3);
}

test "appendStrategy concatenates without separator" {
    const alloc = std.testing.allocator;
    const strat = appendStrategy();

    const r1 = try strat.call("", "hello", alloc);
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("hello", r1);

    const r2 = try strat.call("hello", " world", alloc);
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("hello world", r2);
}

test "lastStrategy keeps only the newest body" {
    const alloc = std.testing.allocator;
    const strat = lastStrategy();

    const r1 = try strat.call("old", "new", alloc);
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("new", r1);

    const r2 = try strat.call("anything", "final", alloc);
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("final", r2);
}

test "fromFn creates strategy from function pointer" {
    const alloc = std.testing.allocator;
    const strat = AggregationStrategy.fromFn(
        struct {
            fn agg(_: ?*anyopaque, old: []const u8, new: []const u8, a: std.mem.Allocator) ![]u8 {
                _ = old;
                // Custom: prefix with ">"
                const result = try a.alloc(u8, new.len + 1);
                result[0] = '>';
                @memcpy(result[1..], new);
                return result;
            }
        }.agg,
        null,
    );

    const r = try strat.call("ignored", "data", alloc);
    defer alloc.free(r);
    try std.testing.expectEqualStrings(">data", r);
}
