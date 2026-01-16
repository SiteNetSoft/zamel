const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Exchange = @import("exchange.zig").Exchange;

/// Predicate: header(key) == value
pub fn headerEq(key: []const u8, value: []const u8) Predicate {
    const Ctx = struct { key: []const u8, value: []const u8 };

    const ctx_ptr = blk: {
        const p = std.heap.page_allocator.create(Ctx) catch unreachable;
        p.* = .{ .key = key, .value = value };
        break :blk p;
    };

    return Predicate.fromFn(headerEqCall, ctx_ptr);
}

fn headerEqCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { key: []const u8, value: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));

    const got = ex.getHeader(c.key) orelse return false;
    return std.mem.eql(u8, got, c.value);
}

/// Predicate: body contains substring needle
pub fn bodyContains(needle: []const u8) Predicate {
    const Ctx = struct { needle: []const u8 };

    const ctx_ptr = blk: {
        const p = std.heap.page_allocator.create(Ctx) catch unreachable;
        p.* = .{ .needle = needle };
        break :blk p;
    };

    return Predicate.fromFn(bodyContainsCall, ctx_ptr);
}

fn bodyContainsCall(ctx: ?*anyopaque, ex: *Exchange) !bool {
    const Ctx = struct { needle: []const u8 };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));

    return std.mem.indexOf(u8, ex.body, c.needle) != null;
}
