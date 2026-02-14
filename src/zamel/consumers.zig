const std = @import("std");
const posix = std.posix;
const Consumer = @import("consumer.zig").Consumer;
const Exchange = @import("exchange.zig").Exchange;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

pub const TimerCtx = struct {
    interval_ms: u64,
    repeat: u64,
};

pub fn timer(allocator: std.mem.Allocator, interval_ms: u64, repeat: u64) Consumer {
    const ctx_ptr = blk: {
        const p = allocator.create(TimerCtx) catch unreachable;
        p.* = .{ .interval_ms = interval_ms, .repeat = repeat };
        break :blk p;
    };

    return .{
        .ctx = ctx_ptr,
        .startFn = timerStart,
        .deinitFn = timerDeinit,
    };
}

fn timerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const p: *TimerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
}

fn timerStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
    const cfg: *const TimerCtx = @ptrCast(@alignCast(ctx.?));

    var exec = SyncExecutor.init(services);

    var i: u64 = 0;
    while (cfg.repeat == 0 or i < cfg.repeat) : (i += 1) {
        var ex = Exchange.init(allocator);
        defer ex.deinit();

        var tick_buf: [32]u8 = undefined;
        const tick_str = try std.fmt.bufPrint(&tick_buf, "{d}", .{i});
        try ex.putHeader("tick", tick_str);

        var body_buf: [64]u8 = undefined;
        const body_str = try std.fmt.bufPrint(&body_buf, "tick {d}", .{i});
        try ex.setBody(body_str);

        if (i % 2 == 0)
            try ex.putHeader("type", "A")
        else
            try ex.putHeader("type", "B");

        try exec.run(plan, @intCast(route_id), &ex);

        sleepMs(cfg.interval_ms);
    }
}

fn sleepMs(ms: u64) void {
    const ns = ms * std.time.ns_per_ms;
    var req = posix.timespec{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
    while (true) {
        switch (posix.errno(posix.system.nanosleep(&req, &req))) {
            .SUCCESS => return,
            .INTR => continue,
            else => unreachable,
        }
    }
}
