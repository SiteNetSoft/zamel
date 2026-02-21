const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const RoutePlan = @import("plan.zig").RoutePlan;
const Step = @import("step.zig").Step;
const PolicyKind = @import("step.zig").PolicyKind;
const RouteId = @import("step.zig").RouteId;

const CircuitState = struct {
    failures: u32 = 0,
    open_until_ms: u64 = 0,
};

pub const SyncExecutor = struct {
    services: Services,
    circuit_states: std.AutoHashMap(RouteId, CircuitState),

    pub fn init(services: Services) SyncExecutor {
        return .{
            .services = services,
            .circuit_states = std.AutoHashMap(RouteId, CircuitState).init(services.allocator),
        };
    }

    pub fn deinit(self: *SyncExecutor) void {
        self.circuit_states.deinit();
    }

    pub fn run(self: *SyncExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !void {
        const route = plan.route(route_id);
        for (route.steps) |step| {
            switch (step) {
                .Process => |p| try p.call(ex),
                .Filter => |pred| {
                    if (!(try pred.call(ex))) return; // stop route on filter fail (Camel-style filter)
                },
                .To => |eref| try self.sendTo(eref, ex),

                .Choice => |c| {
                    var matched = false;
                    for (c.branches) |b| {
                        if (try b.when.call(ex)) {
                            matched = true;
                            try self.run(plan, b.route, ex);
                            break;
                        }
                    }
                    if (!matched) if (c.otherwise) |oid| try self.run(plan, oid, ex);
                },

                .Multicast => |m| {
                    // Sync MVP: sequential execution
                    for (m.routes) |rid| try self.run(plan, rid, ex);
                },

                .Split => |s| {
                    // Split body, run sub-route for each part
                    var parts: std.ArrayList([]const u8) = .empty;
                    defer parts.deinit(ex.allocator);

                    switch (s.kind) {
                        .scalar => |d| {
                            var it = std.mem.splitScalar(u8, ex.body, d);
                            while (it.next()) |part| try parts.append(ex.allocator, part);
                        },
                        .sequence => |sep| {
                            var it = std.mem.splitSequence(u8, ex.body, sep);
                            while (it.next()) |part| try parts.append(ex.allocator, part);
                        },
                        .custom => |splitter| {
                            try splitter.call(ex.body, &parts, ex.allocator);
                        },
                    }

                    var idx: u32 = 0;
                    for (parts.items) |part| {
                        var child = Exchange.init(ex.allocator);
                        defer child.deinit();
                        try child.setBody(part);

                        // Copy headers from parent
                        var hit = ex.headers.iterator();
                        while (hit.next()) |e| try child.putHeader(e.key_ptr.*, e.value_ptr.*);

                        // Set split metadata headers
                        var idx_buf: [16]u8 = undefined;
                        const idx_str = try std.fmt.bufPrint(&idx_buf, "{d}", .{idx});
                        try child.putHeader("CamelSplitIndex", idx_str);

                        try self.run(plan, s.route, &child);
                        idx += 1;
                    }
                },

                .Aggregate => |a| {
                    // Split body by separator, run sub-route on each part,
                    // then join the resulting bodies back with separator
                    var parts: std.ArrayList([]u8) = .empty;
                    defer {
                        for (parts.items) |p| ex.allocator.free(p);
                        parts.deinit(ex.allocator);
                    }

                    var it = std.mem.splitSequence(u8, ex.body, a.separator);
                    while (it.next()) |part| {
                        var child = Exchange.init(ex.allocator);
                        defer child.deinit();
                        try child.setBody(part);

                        var hit = ex.headers.iterator();
                        while (hit.next()) |e| try child.putHeader(e.key_ptr.*, e.value_ptr.*);

                        try self.run(plan, a.route, &child);

                        // Collect the result
                        try parts.append(ex.allocator, try ex.allocator.dupe(u8, child.body));
                    }

                    // Join results back into parent body
                    const joined = try std.mem.join(ex.allocator, a.separator, parts.items);
                    if (ex.body.len != 0) ex.allocator.free(ex.body);
                    ex.body = joined;
                },

                .Policy => |pol| {
                    switch (pol.kind) {
                        .Retry => |retry| {
                            var last_err: ?anyerror = null;
                            for (0..retry.max) |_| {
                                if (self.run(plan, pol.route, ex)) {
                                    last_err = null;
                                    break;
                                } else |err| {
                                    last_err = err;
                                    if (retry.backoff_ms > 0) {
                                        @import("time_util.zig").sleepMs(@as(u64, retry.backoff_ms));
                                    }
                                }
                            }
                            if (last_err) |err| return err;
                        },
                        .Timeout => |t| {
                            const start_ms = self.services.clock.nowMillis();
                            try self.run(plan, pol.route, ex);
                            const elapsed = self.services.clock.nowMillis() - start_ms;
                            if (elapsed > @as(u64, t.ms)) return error.TimeoutExceeded;
                        },

                        .DeadLetter => |dl| {
                            self.run(plan, pol.route, ex) catch {
                                // Route failed — send to dead letter endpoint
                                try self.sendTo(dl.endpoint, ex);
                            };
                        },

                        .CircuitBreaker => |cb| {
                            const state = try self.circuit_states.getOrPut(pol.route);
                            if (!state.found_existing) state.value_ptr.* = .{};

                            const now = self.services.clock.nowMillis();

                            // If circuit is open, check if reset period elapsed
                            if (state.value_ptr.failures >= cb.failure_threshold) {
                                if (now < state.value_ptr.open_until_ms) {
                                    return error.CircuitOpen;
                                }
                                // Half-open: reset and try
                                state.value_ptr.failures = 0;
                            }

                            if (self.run(plan, pol.route, ex)) {
                                state.value_ptr.failures = 0;
                            } else |err| {
                                state.value_ptr.failures += 1;
                                if (state.value_ptr.failures >= cb.failure_threshold) {
                                    state.value_ptr.open_until_ms = now + @as(u64, cb.reset_ms);
                                }
                                return err;
                            }
                        },
                    }
                },
            }
        }
    }

    fn sendTo(self: *SyncExecutor, eref: anytype, ex: *Exchange) !void {
        _ = self;
        switch (eref) {
            .endpoint => |ep| {
                const prod = try ep.createProducer(ex.allocator);
                try prod.send(ex);
            },
        }
    }
};

// -------- tests --------

const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const ChoiceBranch = @import("step.zig").ChoiceBranch;
const Clock = @import("services.zig").Clock;

fn testClock(_: ?*anyopaque) u64 {
    return 0;
}

fn testServices() Services {
    return .{
        .allocator = std.testing.allocator,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
    };
}

fn appendProcessor(ctx: ?*anyopaque, ex: *Exchange) !void {
    _ = ctx;
    const old = ex.body;
    const new = try ex.allocator.alloc(u8, old.len + 1);
    @memcpy(new[0..old.len], old);
    new[old.len] = 'X';
    if (old.len != 0) ex.allocator.free(old);
    ex.body = new;
}

test "Process step executes processor" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hi");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("hiX", ex.body);
}

fn alwaysFalse(_: ?*anyopaque, _: *Exchange) !bool {
    return false;
}

fn alwaysTrue(_: ?*anyopaque, _: *Exchange) !bool {
    return true;
}

test "Filter stops route when predicate returns false" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{
        .{ .Filter = Predicate.fromFn(alwaysFalse, null) },
        .{ .Process = Processor.fromFn(appendProcessor, null) },
    };
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hi");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    // Processor should NOT have run
    try std.testing.expectEqualStrings("hi", ex.body);
}

test "Choice branches correctly" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Sub-route for the "true" branch: appends X
    var true_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const true_rid = try plan.addRoute(alloc, &true_steps);

    // Main route with a choice
    var branches = [_]ChoiceBranch{
        .{ .when = Predicate.fromFn(alwaysTrue, null), .route = true_rid },
    };
    var main_steps = [_]Step{.{ .Choice = .{ .branches = &branches, .otherwise = null } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hi");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqualStrings("hiX", ex.body);
}

var fail_count: u32 = 0;

fn failingProcessor(_: ?*anyopaque, _: *Exchange) !void {
    if (fail_count > 0) {
        fail_count -= 1;
        return error.TransientFailure;
    }
}

test "Retry succeeds after transient failures" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Sub-route that fails twice then succeeds
    fail_count = 2;
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(failingProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Retry = .{ .max = 5, .backoff_ms = 0 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Should have succeeded (fail_count should be 0)
    try std.testing.expectEqual(@as(u32, 0), fail_count);
}

test "Retry returns error when all attempts fail" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    fail_count = 100; // Will never reach 0 in 3 attempts
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(failingProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Retry = .{ .max = 3, .backoff_ms = 0 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    const result = exec.run(&plan, main_rid, &ex);
    try std.testing.expectError(error.TransientFailure, result);
}

test "Split processes each part independently" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Sub-route: appends X to each split part
    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    // Main route: split by newline
    var main_steps = [_]Step{.{ .Split = .{ .kind = .{ .scalar = '\n' }, .route = sub_rid } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa\nbbb\nccc");
    try ex.putHeader("key", "val");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Split doesn't modify parent body (it runs sub-routes on copies)
    try std.testing.expectEqualStrings("aaa\nbbb\nccc", ex.body);
}

test "Aggregate transforms and rejoins body" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Sub-route: appends X to each part
    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    // Main route: aggregate by newline
    var main_steps = [_]Step{.{ .Aggregate = .{ .separator = "\n", .route = sub_rid } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa\nbbb\nccc");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Each part should have X appended, then rejoined
    try std.testing.expectEqualStrings("aaaX\nbbbX\ncccX", ex.body);
}
