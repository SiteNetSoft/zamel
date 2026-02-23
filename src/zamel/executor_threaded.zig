const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const RoutePlan = @import("plan.zig").RoutePlan;
const Step = @import("step.zig").Step;
const RouteId = @import("step.zig").RouteId;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;

const ThreadCtx = struct {
    plan: *const RoutePlan,
    route_id: RouteId,
    services: Services,
    child: *Exchange,
    err: ?anyerror = null,
};

fn threadEntry(ctx: *ThreadCtx) void {
    var sync = SyncExecutor.init(ctx.services);
    defer sync.deinit();
    sync.run(ctx.plan, ctx.route_id, ctx.child) catch |err| {
        ctx.err = err;
    };
}

pub const ThreadedExecutor = struct {
    services: Services,

    pub fn init(services: Services) ThreadedExecutor {
        return .{ .services = services };
    }

    pub fn run(self: *ThreadedExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !void {
        const route = plan.route(route_id);
        for (route.steps) |step| {
            switch (step) {
                .Multicast => |m| {
                    if (m.parallel) {
                        try self.runParallelMulticast(plan, m.routes, ex);
                    } else {
                        var sync = SyncExecutor.init(self.services);
                        defer sync.deinit();
                        for (m.routes) |rid| try sync.run(plan, rid, ex);
                    }
                },
                else => {
                    // Delegate full route to SyncExecutor
                    var sync = SyncExecutor.init(self.services);
                    defer sync.deinit();
                    try sync.run(plan, route_id, ex);
                    return;
                },
            }
        }
    }

    fn runParallelMulticast(self: *ThreadedExecutor, plan: *const RoutePlan, routes: []const RouteId, ex: *Exchange) !void {
        const alloc = self.services.allocator;

        // Create per-thread contexts with cloned exchanges
        var contexts = try alloc.alloc(ThreadCtx, routes.len);
        defer alloc.free(contexts);

        var children = try alloc.alloc(Exchange, routes.len);
        defer alloc.free(children);

        var initialized: usize = 0;
        errdefer {
            for (0..initialized) |i| children[i].deinit();
        }

        for (routes, 0..) |rid, i| {
            children[i] = Exchange.init(alloc);
            try children[i].setBody(ex.body);
            var hit = ex.headers.iterator();
            while (hit.next()) |e| try children[i].putHeader(e.key_ptr.*, e.value_ptr.*);
            initialized += 1;

            contexts[i] = .{
                .plan = plan,
                .route_id = rid,
                .services = self.services,
                .child = &children[i],
            };
        }

        // Spawn threads
        var threads = try alloc.alloc(std.Thread, routes.len);
        defer alloc.free(threads);

        var spawned: usize = 0;
        for (contexts, 0..) |*ctx, i| {
            threads[i] = try std.Thread.spawn(.{}, threadEntry, .{ctx});
            spawned += 1;
        }

        // Join all threads
        for (0..spawned) |i| threads[i].join();

        // Check for errors, clean up
        var first_err: ?anyerror = null;
        for (0..initialized) |i| {
            if (first_err == null and contexts[i].err != null) first_err = contexts[i].err;
            children[i].deinit();
        }

        if (first_err) |err| return err;
    }
};

// -------- tests --------

const Processor = @import("processor.zig").Processor;

fn uppercaseProcessor(_: ?*anyopaque, ex: *Exchange) !void {
    const old = ex.body;
    const new = try ex.allocator.alloc(u8, old.len);
    for (old, 0..) |ch, i| {
        new[i] = if (ch >= 'a' and ch <= 'z') ch - 32 else ch;
    }
    if (old.len != 0) ex.allocator.free(old);
    ex.body = new;
}

fn testClock(_: ?*anyopaque) u64 {
    return 0;
}

fn testServices() Services {
    return .{
        .allocator = std.testing.allocator,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
    };
}

test "ThreadedExecutor sequential multicast" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Multicast = .{
        .routes = &[_]RouteId{sub_rid},
        .parallel = false,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = ThreadedExecutor.init(testServices());
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}
