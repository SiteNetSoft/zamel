const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;
const Registry = @import("registry.zig").Registry;

const DirectCtx = struct {
    name: []const u8,
    registry: *Registry,
};

pub fn makeDirectEndpoint(allocator: std.mem.Allocator, name: []const u8, registry: *Registry) !Endpoint {
    const ctx = try allocator.create(DirectCtx);
    ctx.* = .{ .name = name, .registry = registry };
    return .{
        .ctx = ctx,
        .createProducerFn = directCreateProducer,
        .deinitFn = directDeinit,
    };
}

fn directDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *DirectCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn directCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = directSend };
}

fn directSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const DirectCtx = @ptrCast(@alignCast(ctx.?));
    // Check route lifecycle state
    const state = c.registry.getRouteState(c.name);
    switch (state) {
        .stopped => return error.RouteStopped,
        .suspended => return error.RouteSuspended,
        .started => {},
    }
    const named = c.registry.lookupRoute(c.name) orelse return error.DirectRouteNotFound;
    var exec = SyncExecutor.init(c.registry.services);
    defer exec.deinit();
    try exec.run(named.plan, named.route_id, ex);
}

const DirectConsumerCtx = struct {
    name: []const u8,
    registry: *Registry,
};

pub fn makeDirectConsumer(allocator: std.mem.Allocator, name: []const u8, registry: *Registry) !Consumer {
    const ctx = try allocator.create(DirectConsumerCtx);
    ctx.* = .{ .name = name, .registry = registry };
    return .{
        .ctx = ctx,
        .startFn = directConsumerStart,
        .deinitFn = directConsumerDeinit,
    };
}

fn directConsumerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *DirectConsumerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn directConsumerStart(ctx: ?*anyopaque, _: std.mem.Allocator, _: Services, plan: *const RoutePlan, route_id: u32) !void {
    const c: *const DirectConsumerCtx = @ptrCast(@alignCast(ctx.?));
    // Register route under name — passive, doesn't poll
    try c.registry.registerRoute(c.name, plan, route_id);
}
