const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;

pub const BeanFn = *const fn (ex: *Exchange) anyerror!void;

const BeanCtx = struct {
    func: BeanFn,
};

pub fn makeBeanEndpoint(allocator: std.mem.Allocator, func: BeanFn) !Endpoint {
    const ctx = try allocator.create(BeanCtx);
    ctx.* = .{ .func = func };
    return .{
        .ctx = ctx,
        .createProducerFn = beanCreateProducer,
        .deinitFn = beanDeinit,
    };
}

fn beanDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *BeanCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn beanCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = beanSend };
}

fn beanSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const BeanCtx = @ptrCast(@alignCast(ctx.?));
    try c.func(ex);
}

// -------- tests --------

test "bean endpoint calls function" {
    const alloc = std.testing.allocator;

    const ep = try makeBeanEndpoint(alloc, struct {
        fn call(ex: *Exchange) !void {
            try ex.setBody("bean-result");
        }
    }.call);
    defer ep.deinit(alloc);

    const prod = try ep.createProducer(alloc);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("input");

    try prod.send(&ex);
    try std.testing.expectEqualStrings("bean-result", ex.body);
}
