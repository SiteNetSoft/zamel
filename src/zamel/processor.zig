const Exchange = @import("exchange.zig").Exchange;

pub const Processor = struct {
    ctx: ?*anyopaque = null,
    callFn: *const fn (ctx: ?*anyopaque, ex: *Exchange) anyerror!void,

    pub fn call(self: Processor, ex: *Exchange) !void {
        try self.callFn(self.ctx, ex);
    }

    pub fn fromFn(f: *const fn (?*anyopaque, *Exchange) anyerror!void, ctx: ?*anyopaque) Processor {
        return .{ .ctx = ctx, .callFn = f };
    }
};
