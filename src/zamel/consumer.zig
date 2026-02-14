const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

pub const Consumer = struct {
    ctx: ?*anyopaque = null,
    startFn: *const fn (ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) anyerror!void,
    deinitFn: ?*const fn (ctx: ?*anyopaque, allocator: std.mem.Allocator) void = null,

    pub fn start(self: Consumer, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
        try self.startFn(self.ctx, allocator, services, plan, route_id);
    }

    pub fn deinit(self: Consumer, allocator: std.mem.Allocator) void {
        if (self.deinitFn) |f| f(self.ctx, allocator);
    }
};
