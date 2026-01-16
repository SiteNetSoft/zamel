const Exchange = @import("exchange.zig").Exchange;

pub const Predicate = struct {
    ctx: ?*anyopaque = null,
    callFn: *const fn (ctx: ?*anyopaque, ex: *Exchange) anyerror!bool,

    pub fn call(self: Predicate, ex: *Exchange) !bool {
        return try self.callFn(self.ctx, ex);
    }

    pub fn fromFn(
        f: *const fn (?*anyopaque, *Exchange) anyerror!bool,
        ctx: ?*anyopaque,
    ) Predicate {
        return .{ .ctx = ctx, .callFn = f };
    }
};
