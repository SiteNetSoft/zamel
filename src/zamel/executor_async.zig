const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const RoutePlan = @import("plan.zig").RoutePlan;
const RouteId = @import("step.zig").RouteId;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;

const WorkItem = struct {
    plan: *const RoutePlan,
    route_id: RouteId,
    exchange: *Exchange,
    result: anyerror!void = {},
    done: std.Thread.ResetEvent = .unset,
};

pub const AsyncHandle = struct {
    item: *WorkItem,

    /// Block until the job completes, then propagate any error.
    pub fn wait(self: AsyncHandle) !void {
        self.item.done.wait();
        try self.item.result;
    }
};

pub const AsyncExecutor = struct {
    services: Services,
    pool_size: usize,
    workers: []std.Thread,
    allocator: std.mem.Allocator,
    started: bool,

    // Job queue protected by mutex
    queue: std.ArrayList(*WorkItem),
    queue_mutex: std.Thread.Mutex,
    queue_cond: std.Thread.Condition,
    shutdown: bool,

    pub fn init(services: Services, pool_size: usize) !AsyncExecutor {
        const alloc = services.allocator;
        return .{
            .services = services,
            .pool_size = pool_size,
            .workers = try alloc.alloc(std.Thread, pool_size),
            .allocator = alloc,
            .started = false,
            .queue = .empty,
            .queue_mutex = .{},
            .queue_cond = .{},
            .shutdown = false,
        };
    }

    /// Spawn worker threads. Must be called after the struct is at its final address.
    pub fn start(self: *AsyncExecutor) !void {
        if (self.started) return;
        for (self.workers) |*w| {
            w.* = try std.Thread.spawn(.{}, workerLoop, .{self});
        }
        self.started = true;
    }

    pub fn deinit(self: *AsyncExecutor) void {
        if (!self.started) {
            self.allocator.free(self.workers);
            self.queue.deinit(self.allocator);
            return;
        }

        // Signal shutdown
        self.queue_mutex.lock();
        self.shutdown = true;
        self.queue_cond.broadcast();
        self.queue_mutex.unlock();

        // Join all workers
        for (self.workers) |w| w.join();
        self.allocator.free(self.workers);

        // Clean up remaining queue items
        self.queue.deinit(self.allocator);
    }

    /// Submit a job for async execution. Returns a handle to wait on.
    /// Lazily starts workers on first submit.
    pub fn submit(self: *AsyncExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !AsyncHandle {
        if (!self.started) try self.start();

        const item = try self.allocator.create(WorkItem);
        item.* = .{
            .plan = plan,
            .route_id = route_id,
            .exchange = ex,
        };

        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        try self.queue.append(self.allocator, item);
        self.queue_cond.signal();

        return .{ .item = item };
    }

    /// Synchronous wrapper: submit and wait.
    pub fn run(self: *AsyncExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !void {
        const handle = try self.submit(plan, route_id, ex);
        try handle.wait();
        self.allocator.destroy(handle.item);
    }

    fn workerLoop(self: *AsyncExecutor) void {
        while (true) {
            self.queue_mutex.lock();

            // Wait for work or shutdown
            while (self.queue.items.len == 0 and !self.shutdown) {
                self.queue_cond.wait(&self.queue_mutex);
            }

            if (self.shutdown and self.queue.items.len == 0) {
                self.queue_mutex.unlock();
                return;
            }

            // Pop a work item
            const item = self.queue.orderedRemove(0);
            self.queue_mutex.unlock();

            // Execute with a fresh SyncExecutor
            var exec = SyncExecutor.init(self.services);
            defer exec.deinit();

            item.result = exec.run(item.plan, item.route_id, item.exchange);
            item.done.set();
        }
    }
};

// -------- tests --------

const Processor = @import("processor.zig").Processor;
const Step = @import("step.zig").Step;

fn testClock(_: ?*anyopaque) u64 {
    return 0;
}

fn testServices() Services {
    return .{
        .allocator = std.testing.allocator,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
    };
}

fn appendProcessor(_: ?*anyopaque, ex: *Exchange) !void {
    const old = ex.body;
    const new = try ex.allocator.alloc(u8, old.len + 1);
    @memcpy(new[0..old.len], old);
    new[old.len] = 'X';
    if (old.len != 0) ex.allocator.free(old);
    ex.body = new;
}

test "async executor runs route" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const rid = try plan.addRoute(alloc, &steps);

    var async_exec = try AsyncExecutor.init(testServices(), 2);
    defer async_exec.deinit();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hi");

    try async_exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("hiX", ex.body);
}

test "async executor parallel multicast" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Two routes that each append a char
    var steps1 = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const rid1 = try plan.addRoute(alloc, &steps1);

    var steps2 = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const rid2 = try plan.addRoute(alloc, &steps2);

    var async_exec = try AsyncExecutor.init(testServices(), 2);
    defer async_exec.deinit();

    // Submit two jobs
    var ex1 = Exchange.init(alloc);
    defer ex1.deinit();
    try ex1.setBody("a");

    var ex2 = Exchange.init(alloc);
    defer ex2.deinit();
    try ex2.setBody("b");

    const h1 = try async_exec.submit(&plan, rid1, &ex1);
    const h2 = try async_exec.submit(&plan, rid2, &ex2);

    try h1.wait();
    try h2.wait();

    // Clean up handles
    alloc.destroy(h1.item);
    alloc.destroy(h2.item);

    try std.testing.expectEqualStrings("aX", ex1.body);
    try std.testing.expectEqualStrings("bX", ex2.body);
}
