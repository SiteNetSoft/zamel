const std = @import("std");
const sync = @import("sync.zig");
const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const RoutePlan = @import("plan.zig").RoutePlan;
const Step = @import("step.zig").Step;
const PolicyKind = @import("step.zig").PolicyKind;
const RouteId = @import("step.zig").RouteId;
const EndpointRef = @import("endpoint.zig").EndpointRef;
const marshal = @import("marshal.zig");
const Metrics = @import("metrics.zig").Metrics;
const Registry = @import("registry.zig").Registry;

const CircuitState = struct {
    failures: u32 = 0,
    open_until_ms: u64 = 0,
    half_open: bool = false,
    half_open_successes: u32 = 0,
};

const BatchBuffer = struct {
    bodies: std.ArrayList([]u8),

    fn init() BatchBuffer {
        return .{ .bodies = .empty };
    }

    fn deinit(self: *BatchBuffer, allocator: std.mem.Allocator) void {
        for (self.bodies.items) |b| allocator.free(b);
        self.bodies.deinit(allocator);
    }
};

const ReseqEntry = struct {
    seq: u64,
    body: []u8,
    headers: std.StringHashMap([]u8),
};

const ReseqBuffer = struct {
    entries: std.ArrayList(ReseqEntry),

    fn init() ReseqBuffer {
        return .{ .entries = .empty };
    }

    fn deinit(self: *ReseqBuffer, allocator: std.mem.Allocator) void {
        for (self.entries.items) |*e| {
            allocator.free(e.body);
            var it = e.headers.iterator();
            while (it.next()) |entry| allocator.free(entry.value_ptr.*);
            e.headers.deinit();
        }
        self.entries.deinit(allocator);
    }
};

pub const SyncExecutor = struct {
    services: Services,
    circuit_states: std.AutoHashMap(RouteId, CircuitState),
    lb_counters: std.AutoHashMap(usize, usize),
    batch_buffers: std.AutoHashMap(RouteId, BatchBuffer),
    reseq_buffers: std.AutoHashMap(RouteId, ReseqBuffer),
    correlation_counter: u64 = 0,
    registry: ?*Registry = null,

    pub fn init(services: Services) SyncExecutor {
        return .{
            .services = services,
            .circuit_states = std.AutoHashMap(RouteId, CircuitState).init(services.allocator),
            .lb_counters = std.AutoHashMap(usize, usize).init(services.allocator),
            .batch_buffers = std.AutoHashMap(RouteId, BatchBuffer).init(services.allocator),
            .reseq_buffers = std.AutoHashMap(RouteId, ReseqBuffer).init(services.allocator),
        };
    }

    pub fn deinit(self: *SyncExecutor) void {
        self.circuit_states.deinit();
        self.lb_counters.deinit();
        var bit = self.batch_buffers.iterator();
        while (bit.next()) |e| e.value_ptr.deinit(self.services.allocator);
        self.batch_buffers.deinit();
        var rit = self.reseq_buffers.iterator();
        while (rit.next()) |e| e.value_ptr.deinit(self.services.allocator);
        self.reseq_buffers.deinit();
    }

    /// Run a route with global error handler support.
    /// Use this as the top-level entry point; sub-routes use `run` directly.
    pub fn runRoute(self: *SyncExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !void {
        if (self.run(plan, route_id, ex)) {} else |err| {
            if (self.services.error_handler) |eh| {
                // Retry if configured
                var last_err: anyerror = err;
                for (0..eh.max_retries) |_| {
                    if (eh.retry_delay_ms > 0)
                        @import("time_util.zig").sleepMs(@as(u64, eh.retry_delay_ms));
                    if (self.run(plan, route_id, ex)) {
                        return;
                    } else |retry_err| {
                        last_err = retry_err;
                    }
                }
                // Send to error endpoint
                try ex.putHeader("CamelExceptionMessage", @errorName(last_err));
                var ts_buf: [32]u8 = undefined;
                const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{self.services.clock.nowMillis()}) catch "0";
                try ex.putHeader("CamelExceptionTimestamp", ts_str);
                var rid_buf: [16]u8 = undefined;
                const rid_str = std.fmt.bufPrint(&rid_buf, "{d}", .{route_id}) catch "?";
                try ex.putHeader("CamelFailedRouteId", rid_str);
                try self.sendTo(eh.endpoint, ex);
            } else {
                return err;
            }
        }
    }

    pub fn run(self: *SyncExecutor, plan: *const RoutePlan, route_id: RouteId, ex: *Exchange) !void {
        errdefer {
            if (self.services.metrics) |m| {
                var buf: [64]u8 = undefined;
                const name = std.fmt.bufPrint(&buf, "route.{d}.errors", .{route_id}) catch "route.?.errors";
                m.increment(name);
            }
        }
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
                    if (m.parallel and m.routes.len > 1) {
                        try self.runParallelMulticast(plan, m.routes, ex);
                    } else {
                        for (m.routes) |rid| try self.run(plan, rid, ex);
                    }
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
                    if (a.strategy) |strat| {
                        // Custom strategy: fold iteratively
                        var accumulated: []u8 = &[_]u8{};
                        defer if (accumulated.len != 0) ex.allocator.free(accumulated);

                        var it = std.mem.splitSequence(u8, ex.body, a.separator);
                        while (it.next()) |part| {
                            var child = Exchange.init(ex.allocator);
                            defer child.deinit();
                            try child.setBody(part);

                            var hit = ex.headers.iterator();
                            while (hit.next()) |e| try child.putHeader(e.key_ptr.*, e.value_ptr.*);

                            try self.run(plan, a.route, &child);

                            const new_acc = try strat.call(accumulated, child.body, ex.allocator);
                            if (accumulated.len != 0) ex.allocator.free(accumulated);
                            accumulated = new_acc;
                        }

                        if (ex.body.len != 0) ex.allocator.free(ex.body);
                        ex.body = accumulated;
                        accumulated = &[_]u8{}; // prevent defer from freeing
                    } else {
                        // Default: split, run sub-route, join back with separator
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

                            try parts.append(ex.allocator, try ex.allocator.dupe(u8, child.body));
                        }

                        const joined = try std.mem.join(ex.allocator, a.separator, parts.items);
                        if (ex.body.len != 0) ex.allocator.free(ex.body);
                        ex.body = joined;
                    }
                },

                .WireTap => |eref| {
                    // Fire-and-forget: copy exchange, send to tap, ignore errors
                    var copy = Exchange.init(ex.allocator);
                    defer copy.deinit();
                    try copy.setBody(ex.body);
                    var hit = ex.headers.iterator();
                    while (hit.next()) |e| try copy.putHeader(e.key_ptr.*, e.value_ptr.*);
                    self.sendTo(eref, &copy) catch {};
                },

                .RecipientList => |rl| {
                    var recipients: std.ArrayList(EndpointRef) = .empty;
                    defer recipients.deinit(ex.allocator);
                    try rl.resolver.call(ex, &recipients, ex.allocator);
                    for (recipients.items) |eref| try self.sendTo(eref, ex);
                },

                .Throttle => |t| {
                    @import("time_util.zig").sleepMs(@as(u64, t.interval_ms));
                },

                .IdempotentConsumer => |ic| {
                    const store = self.services.store orelse return error.StateStoreRequired;
                    const key_val = ex.getHeader(ic.key_header) orelse return error.IdempotentKeyMissing;
                    // Check if already seen
                    if (try store.get(ex.allocator, key_val)) |existing| {
                        ex.allocator.free(existing);
                        // Duplicate detected
                        try ex.putHeader("CamelDuplicateMessage", "true");
                        if (!ic.skip_duplicate) {
                            // Non-skip mode: run route anyway with duplicate header set
                            try self.run(plan, ic.route, ex);
                        }
                    } else {
                        // First time
                        if (ic.eager) {
                            // Eager: store key before execution
                            try store.put(key_val, "1");
                            try self.run(plan, ic.route, ex);
                        } else {
                            // Non-eager: store key after successful execution
                            try self.run(plan, ic.route, ex);
                            try store.put(key_val, "1");
                        }
                    }
                },

                .Enrich => |e| {
                    // Save original body
                    const original = try ex.allocator.dupe(u8, ex.body);

                    // Send to enrichment endpoint (modifies exchange body)
                    try self.sendTo(e.endpoint, ex);

                    if (e.merge) |merge_proc| {
                        // Set header with original body for merge processor
                        try ex.putHeader("CamelEnrichOriginalBody", original);
                        ex.allocator.free(original);
                        try merge_proc.call(ex);
                        // Clean up the temporary header
                        if (ex.headers.get("CamelEnrichOriginalBody")) |hval| ex.allocator.free(hval);
                        _ = ex.headers.remove("CamelEnrichOriginalBody");
                    } else {
                        ex.allocator.free(original);
                        // No merge — enriched body remains as-is
                    }
                },

                .DoTry => |dt| {
                    if (self.run(plan, dt.try_route, ex)) {} else |err| {
                        // Set exception header
                        try ex.putHeader("CamelExceptionMessage", @errorName(err));
                        // Run catch route if present
                        if (dt.catch_route) |cid| {
                            try self.run(plan, cid, ex);
                        }
                    }
                    // Always run finally route if present
                    if (dt.finally_route) |fid| {
                        try self.run(plan, fid, ex);
                    }
                },

                .Marshal => |m| {
                    switch (m.format) {
                        .json => try marshal.marshalJson(ex),
                    }
                },

                .Unmarshal => |u| {
                    switch (u.format) {
                        .json => try marshal.unmarshalJson(ex),
                    }
                },

                .ClaimCheck => |cc| {
                    const store = self.services.store orelse return error.StateStoreRequired;
                    switch (cc.action) {
                        .store => {
                            try store.put(cc.key, ex.body);
                            try ex.setBody(cc.key);
                        },
                        .retrieve => {
                            if (try store.get(ex.allocator, cc.key)) |val| {
                                if (ex.body.len != 0) ex.allocator.free(ex.body);
                                ex.body = val;
                            }
                        },
                    }
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
                            // Thread-based timeout: run route in a thread, wait with timeout
                            const start_ms = self.services.clock.nowMillis();

                            const Ctx = struct {
                                executor: *SyncExecutor,
                                plan_ptr: *const RoutePlan,
                                route_id: RouteId,
                                ex_ptr: *Exchange,
                                result: anyerror!void = {},
                                done: sync.Event = .{},
                            };
                            var tctx = Ctx{
                                .executor = self,
                                .plan_ptr = plan,
                                .route_id = pol.route,
                                .ex_ptr = ex,
                            };

                            const thread = std.Thread.spawn(.{}, struct {
                                fn run(c: *Ctx) void {
                                    c.result = c.executor.run(c.plan_ptr, c.route_id, c.ex_ptr);
                                    c.done.set();
                                }
                            }.run, .{&tctx}) catch {
                                // Fallback to synchronous check if thread spawn fails
                                try self.run(plan, pol.route, ex);
                                const elapsed = self.services.clock.nowMillis() - start_ms;
                                if (elapsed > @as(u64, t.ms)) return error.TimeoutExceeded;
                                return;
                            };

                            tctx.done.timedWait(@as(u64, t.ms) * std.time.ns_per_ms) catch {
                                // Timeout expired — still join thread for cleanup
                                thread.join();
                                return error.TimeoutExceeded;
                            };

                            thread.join();
                            try tctx.result;

                            // Also check logical clock for backward compatibility
                            const elapsed = self.services.clock.nowMillis() - start_ms;
                            if (elapsed > @as(u64, t.ms)) return error.TimeoutExceeded;
                        },

                        .DeadLetter => |dl| {
                            var last_err: ?anyerror = null;
                            const attempts: u32 = dl.retries + 1;
                            for (0..attempts) |attempt| {
                                if (self.run(plan, pol.route, ex)) {
                                    last_err = null;
                                    break;
                                } else |err| {
                                    last_err = err;
                                    if (attempt + 1 < attempts and dl.retry_backoff_ms > 0) {
                                        @import("time_util.zig").sleepMs(@as(u64, dl.retry_backoff_ms));
                                    }
                                }
                            }
                            if (last_err) |err| {
                                // Enrich exchange with error metadata
                                try ex.putHeader("CamelExceptionMessage", @errorName(err));
                                var ts_buf: [32]u8 = undefined;
                                const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{self.services.clock.nowMillis()}) catch "0";
                                try ex.putHeader("CamelExceptionTimestamp", ts_str);
                                var rid_buf: [16]u8 = undefined;
                                const rid_str = std.fmt.bufPrint(&rid_buf, "{d}", .{pol.route}) catch "?";
                                try ex.putHeader("CamelFailedRouteId", rid_str);
                                // Send to dead letter endpoint
                                try self.sendTo(dl.endpoint, ex);
                            }
                        },

                        .CircuitBreaker => |cb| {
                            const state = try self.circuit_states.getOrPut(pol.route);
                            if (!state.found_existing) state.value_ptr.* = .{};

                            const now = self.services.clock.nowMillis();

                            // If circuit is open, check if reset period elapsed
                            if (state.value_ptr.failures >= cb.failure_threshold and !state.value_ptr.half_open) {
                                if (now < state.value_ptr.open_until_ms) {
                                    return error.CircuitOpen;
                                }
                                // Enter half-open: allow a probe
                                state.value_ptr.half_open = true;
                                state.value_ptr.half_open_successes = 0;
                            }

                            if (self.run(plan, pol.route, ex)) {
                                if (state.value_ptr.half_open) {
                                    state.value_ptr.half_open_successes += 1;
                                    if (state.value_ptr.half_open_successes >= cb.success_threshold) {
                                        // Close circuit
                                        state.value_ptr.failures = 0;
                                        state.value_ptr.half_open = false;
                                        state.value_ptr.half_open_successes = 0;
                                    }
                                } else {
                                    state.value_ptr.failures = 0;
                                }
                            } else |err| {
                                if (state.value_ptr.half_open) {
                                    // Failed in half-open — reopen immediately
                                    state.value_ptr.half_open = false;
                                    state.value_ptr.open_until_ms = now + @as(u64, cb.reset_ms);
                                } else {
                                    state.value_ptr.failures += 1;
                                    if (state.value_ptr.failures >= cb.failure_threshold) {
                                        state.value_ptr.open_until_ms = now + @as(u64, cb.reset_ms);
                                    }
                                }
                                return err;
                            }
                        },

                        .Redelivery => |rd| {
                            const total_attempts: u32 = rd.max_redeliveries + 1;
                            var last_err: ?anyerror = null;
                            var current_delay: u32 = rd.initial_delay_ms;

                            for (0..total_attempts) |attempt| {
                                if (attempt > 0) {
                                    // Set redelivery headers
                                    var cnt_buf: [16]u8 = undefined;
                                    const cnt_str = std.fmt.bufPrint(&cnt_buf, "{d}", .{attempt}) catch "?";
                                    try ex.putHeader("CamelRedeliveryCounter", cnt_str);
                                    try ex.putHeader("CamelRedelivered", "true");
                                    var max_buf: [16]u8 = undefined;
                                    const max_str = std.fmt.bufPrint(&max_buf, "{d}", .{rd.max_redeliveries}) catch "?";
                                    try ex.putHeader("CamelRedeliveryMaxCounter", max_str);

                                    if (current_delay > 0) {
                                        @import("time_util.zig").sleepMs(@as(u64, current_delay));
                                    }

                                    // Compute next delay
                                    if (rd.use_exponential) {
                                        current_delay = if (current_delay == 0) rd.initial_delay_ms else current_delay *| 2;
                                    } else {
                                        current_delay = current_delay *| rd.multiplier;
                                    }
                                    if (rd.max_delay_ms > 0 and current_delay > rd.max_delay_ms) {
                                        current_delay = rd.max_delay_ms;
                                    }
                                }

                                if (self.run(plan, pol.route, ex)) {
                                    last_err = null;
                                    break;
                                } else |err| {
                                    last_err = err;
                                }
                            }
                            if (last_err) |err| return err;
                        },
                    }
                },

                .Delay => |d| {
                    @import("time_util.zig").sleepMs(@as(u64, d.ms));
                },

                .Log => |l| {
                    const msg = try interpolateLog(ex, l.message);
                    defer if (msg.ptr != l.message.ptr) ex.allocator.free(msg);
                    switch (l.level) {
                        .debug => std.log.debug("{s}", .{msg}),
                        .info => std.log.info("{s}", .{msg}),
                        .warn => std.log.warn("{s}", .{msg}),
                        .err => std.log.err("{s}", .{msg}),
                    }
                },

                .RoutingSlip => |rs| {
                    if (ex.getHeader(rs.header)) |slip| {
                        var it = std.mem.splitScalar(u8, slip, ',');
                        while (it.next()) |uri_raw| {
                            const uri = std.mem.trim(u8, uri_raw, " ");
                            if (uri.len == 0) continue;
                            const reg = self.registry orelse return error.RegistryRequired;
                            const ep = try reg.resolve(ex.allocator, uri);
                            try self.sendTo(.{ .endpoint = ep }, ex);
                        }
                    }
                },

                .LoadBalancer => |lb| {
                    if (lb.routes.len == 0) return;
                    const idx = switch (lb.strategy) {
                        .round_robin => blk: {
                            const key = @intFromPtr(lb.routes.ptr);
                            const gop = try self.lb_counters.getOrPut(key);
                            if (!gop.found_existing) gop.value_ptr.* = 0;
                            const i = gop.value_ptr.* % lb.routes.len;
                            gop.value_ptr.* += 1;
                            break :blk i;
                        },
                        .random => blk: {
                            const now = self.services.clock.nowMillis();
                            break :blk @as(usize, @intCast(now % lb.routes.len));
                        },
                    };
                    try self.run(plan, lb.routes[idx], ex);
                },

                .Transform => |t| try t.call(ex),

                .DynamicRouter => |dr| {
                    if (ex.getHeader(dr.header)) |uri| {
                        const reg = self.registry orelse return error.RegistryRequired;
                        const ep = try reg.resolve(ex.allocator, uri);
                        try self.sendTo(.{ .endpoint = ep }, ex);
                    }
                },

                .Batch => |b| {
                    const gop = try self.batch_buffers.getOrPut(b.route);
                    if (!gop.found_existing) gop.value_ptr.* = BatchBuffer.init();
                    var buf = gop.value_ptr;

                    try buf.bodies.append(ex.allocator, try ex.allocator.dupe(u8, ex.body));

                    if (buf.bodies.items.len >= b.size) {
                        // Join all buffered bodies into one exchange
                        const joined = try std.mem.join(ex.allocator, b.separator, buf.bodies.items);
                        defer ex.allocator.free(joined);

                        // Clear buffer
                        for (buf.bodies.items) |body| ex.allocator.free(body);
                        buf.bodies.clearRetainingCapacity();

                        // Create batch exchange and run sub-route
                        var batch_ex = Exchange.init(ex.allocator);
                        defer batch_ex.deinit();
                        try batch_ex.setBody(joined);

                        // Copy headers from current exchange
                        var hit = ex.headers.iterator();
                        while (hit.next()) |e| try batch_ex.putHeader(e.key_ptr.*, e.value_ptr.*);

                        var size_buf: [16]u8 = undefined;
                        const size_str = std.fmt.bufPrint(&size_buf, "{d}", .{b.size}) catch "?";
                        try batch_ex.putHeader("CamelBatchSize", size_str);

                        try self.run(plan, b.route, &batch_ex);

                        // Copy result back to original exchange
                        try ex.setBody(batch_ex.body);
                    }
                },

                .RequestReply => |rr| {
                    // Generate correlation ID
                    self.correlation_counter += 1;
                    var id_buf: [32]u8 = undefined;
                    const id_str = std.fmt.bufPrint(&id_buf, "corr-{d}-{d}", .{ self.services.clock.nowMillis(), self.correlation_counter }) catch "corr-?";
                    try ex.putHeader(rr.correlation_header, id_str);

                    // Send to endpoint (response modifies exchange body)
                    try self.sendTo(rr.endpoint, ex);
                },

                .Resequencer => |r| {
                    const gop = try self.reseq_buffers.getOrPut(r.route);
                    if (!gop.found_existing) gop.value_ptr.* = ReseqBuffer.init();
                    var buf = gop.value_ptr;

                    // Parse sequence number from header
                    const seq_str = ex.getHeader(r.header) orelse return error.ResequencerKeyMissing;
                    const seq = std.fmt.parseUnsigned(u64, seq_str, 10) catch return error.InvalidSequenceNumber;

                    // Copy headers for buffering
                    var hdr_copy = std.StringHashMap([]u8).init(ex.allocator);
                    var hit = ex.headers.iterator();
                    while (hit.next()) |e| try hdr_copy.put(e.key_ptr.*, try ex.allocator.dupe(u8, e.value_ptr.*));

                    try buf.entries.append(ex.allocator, .{
                        .seq = seq,
                        .body = try ex.allocator.dupe(u8, ex.body),
                        .headers = hdr_copy,
                    });

                    if (buf.entries.items.len >= r.size) {
                        // Sort by sequence number
                        std.mem.sort(ReseqEntry, buf.entries.items, {}, struct {
                            fn lessThan(_: void, a: ReseqEntry, b_entry: ReseqEntry) bool {
                                return a.seq < b_entry.seq;
                            }
                        }.lessThan);

                        // Process each entry in order
                        for (buf.entries.items) |*entry| {
                            var child = Exchange.init(ex.allocator);
                            defer child.deinit();

                            // Transfer body ownership to child
                            child.body = entry.body;
                            entry.body = &[_]u8{};

                            // Transfer headers
                            var hdr_it = entry.headers.iterator();
                            while (hdr_it.next()) |e| {
                                try child.headers.put(e.key_ptr.*, e.value_ptr.*);
                            }
                            // Clear so deinit doesn't double-free
                            entry.headers.clearAndFree();

                            try self.run(plan, r.route, &child);
                        }

                        // Clear buffer
                        buf.entries.clearRetainingCapacity();
                    }
                },
            }

            // Message history tracking
            if (self.services.message_history) {
                const step_name = stepName(step);
                if (ex.getHeader("CamelMessageHistory")) |existing| {
                    const new = try std.fmt.allocPrint(ex.allocator, "{s},{s}", .{ existing, step_name });
                    ex.allocator.free(@constCast(existing));
                    try ex.headers.put("CamelMessageHistory", @constCast(new));
                } else {
                    const duped = try ex.allocator.dupe(u8, step_name);
                    try ex.headers.put("CamelMessageHistory", duped);
                }
            }
        }
        if (self.services.metrics) |m| {
            var buf: [64]u8 = undefined;
            const name = std.fmt.bufPrint(&buf, "route.{d}.messages", .{route_id}) catch "route.?.messages";
            m.increment(name);
        }
    }

    fn interpolateLog(ex: *Exchange, template: []const u8) ![]u8 {
        // Quick check if interpolation is needed
        if (std.mem.indexOf(u8, template, "${") == null) {
            // Return the original slice — caller checks ptr equality to avoid freeing
            return @constCast(template);
        }

        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(ex.allocator);

        var i: usize = 0;
        while (i < template.len) {
            if (i + 1 < template.len and template[i] == '$' and template[i + 1] == '{') {
                const close = std.mem.indexOfScalarPos(u8, template, i + 2, '}') orelse {
                    try result.append(ex.allocator, template[i]);
                    i += 1;
                    continue;
                };
                const key = template[i + 2 .. close];
                if (std.mem.eql(u8, key, "body")) {
                    try result.appendSlice(ex.allocator, ex.body);
                } else if (std.mem.startsWith(u8, key, "header.")) {
                    const hdr_key = key["header.".len..];
                    const val = ex.getHeader(hdr_key) orelse "";
                    try result.appendSlice(ex.allocator, val);
                } else {
                    // Unknown placeholder — output as-is
                    try result.appendSlice(ex.allocator, template[i .. close + 1]);
                }
                i = close + 1;
            } else {
                try result.append(ex.allocator, template[i]);
                i += 1;
            }
        }

        return try result.toOwnedSlice(ex.allocator);
    }

    fn stepName(step: Step) []const u8 {
        return switch (step) {
            .Process => "Process",
            .Filter => "Filter",
            .To => "To",
            .Choice => "Choice",
            .Multicast => "Multicast",
            .Split => "Split",
            .Aggregate => "Aggregate",
            .WireTap => "WireTap",
            .RecipientList => "RecipientList",
            .Throttle => "Throttle",
            .IdempotentConsumer => "IdempotentConsumer",
            .Enrich => "Enrich",
            .Policy => "Policy",
            .DoTry => "DoTry",
            .Marshal => "Marshal",
            .Unmarshal => "Unmarshal",
            .ClaimCheck => "ClaimCheck",
            .Delay => "Delay",
            .Log => "Log",
            .RoutingSlip => "RoutingSlip",
            .LoadBalancer => "LoadBalancer",
            .Transform => "Transform",
            .DynamicRouter => "DynamicRouter",
            .Batch => "Batch",
            .RequestReply => "RequestReply",
            .Resequencer => "Resequencer",
        };
    }

    /// Flush any pending batch buffer for the given route.
    pub fn flushBatch(self: *SyncExecutor, plan: *const RoutePlan, route_id: RouteId, separator: []const u8, sub_route: RouteId, ex: *Exchange) !void {
        if (self.batch_buffers.getPtr(sub_route)) |buf| {
            if (buf.bodies.items.len > 0) {
                _ = route_id;
                const joined = try std.mem.join(ex.allocator, separator, buf.bodies.items);
                defer ex.allocator.free(joined);

                for (buf.bodies.items) |body| ex.allocator.free(body);
                buf.bodies.clearRetainingCapacity();

                var batch_ex = Exchange.init(ex.allocator);
                defer batch_ex.deinit();
                try batch_ex.setBody(joined);

                var size_buf: [16]u8 = undefined;
                const size_str = std.fmt.bufPrint(&size_buf, "{d}", .{buf.bodies.items.len}) catch "?";
                try batch_ex.putHeader("CamelBatchSize", size_str);
                try batch_ex.putHeader("CamelBatchComplete", "true");

                try self.run(plan, sub_route, &batch_ex);
                try ex.setBody(batch_ex.body);
            }
        }
    }

    fn runParallelMulticast(self: *SyncExecutor, plan: *const RoutePlan, routes: []const RouteId, ex: *Exchange) !void {
        const ThreadCtx = struct {
            executor: *SyncExecutor,
            plan_ptr: *const RoutePlan,
            route_id: RouteId,
            ex_ptr: *Exchange,
            result: anyerror!void = {},
            done: sync.Event = .{},
        };

        // Clone exchange for each route (except last which uses original)
        var clones = try self.services.allocator.alloc(Exchange, routes.len - 1);
        defer {
            for (clones) |*c| c.deinit();
            self.services.allocator.free(clones);
        }

        for (0..routes.len - 1) |i| {
            clones[i] = Exchange.init(ex.allocator);
            try clones[i].setBody(ex.body);
            var hit = ex.headers.iterator();
            while (hit.next()) |e| try clones[i].putHeader(e.key_ptr.*, e.value_ptr.*);
        }

        var ctxs = try self.services.allocator.alloc(ThreadCtx, routes.len);
        defer self.services.allocator.free(ctxs);

        var threads = try self.services.allocator.alloc(std.Thread, routes.len);
        defer self.services.allocator.free(threads);

        var spawned: usize = 0;
        errdefer {
            for (0..spawned) |i| threads[i].join();
        }

        for (routes, 0..) |rid, i| {
            const ex_ptr: *Exchange = if (i < routes.len - 1) &clones[i] else ex;
            ctxs[i] = .{
                .executor = self,
                .plan_ptr = plan,
                .route_id = rid,
                .ex_ptr = ex_ptr,
            };
            threads[i] = try std.Thread.spawn(.{}, struct {
                fn run(c: *ThreadCtx) void {
                    c.result = c.executor.run(c.plan_ptr, c.route_id, c.ex_ptr);
                    c.done.set();
                }
            }.run, .{&ctxs[i]});
            spawned += 1;
        }

        // Join all threads
        for (0..spawned) |i| threads[i].join();

        // Check for errors (return first error found)
        for (ctxs[0..spawned]) |ctx| {
            try ctx.result;
        }
    }

    fn sendTo(self: *SyncExecutor, eref: EndpointRef, ex: *Exchange) !void {
        switch (eref) {
            .endpoint => |ep| {
                const prod = try ep.createProducer(ex.allocator);
                try prod.send(ex);
            },
            .uri => |uri| {
                const reg = self.registry orelse return error.RegistryNotSet;
                const ep = try reg.resolve(ex.allocator, uri);
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

test "Aggregate with custom strategy (join with comma)" {
    const alloc = std.testing.allocator;
    const aggregation = @import("aggregation.zig");
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    const strat = try aggregation.joinStrategy(alloc, ",");
    defer alloc.destroy(@as(*const struct { sep: []const u8 }, @ptrCast(@alignCast(strat.ctx.?))));

    var main_steps = [_]Step{.{ .Aggregate = .{ .separator = "\n", .route = sub_rid, .strategy = strat } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa\nbbb\nccc");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Custom strategy joins with comma instead of original separator
    try std.testing.expectEqualStrings("aaaX,bbbX,cccX", ex.body);
}

test "Aggregate with last strategy keeps only final part" {
    const alloc = std.testing.allocator;
    const aggregation = @import("aggregation.zig");
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Aggregate = .{ .separator = "\n", .route = sub_rid, .strategy = aggregation.lastStrategy() } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa\nbbb\nccc");

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqualStrings("cccX", ex.body);
}

var redelivery_fail_count: u32 = 0;

fn redeliveryFailingProcessor(_: ?*anyopaque, _: *Exchange) !void {
    if (redelivery_fail_count > 0) {
        redelivery_fail_count -= 1;
        return error.TransientFailure;
    }
}

test "Redelivery succeeds after transient failures" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    redelivery_fail_count = 2;
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(redeliveryFailingProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Redelivery = .{ .max_redeliveries = 3, .initial_delay_ms = 0 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(testServices());
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqual(@as(u32, 0), redelivery_fail_count);
    // Should have redelivery headers set
    try std.testing.expectEqualStrings("true", ex.getHeader("CamelRedelivered").?);
    try std.testing.expectEqualStrings("2", ex.getHeader("CamelRedeliveryCounter").?);
    try std.testing.expectEqualStrings("3", ex.getHeader("CamelRedeliveryMaxCounter").?);
}

test "Redelivery exhausted returns error" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    redelivery_fail_count = 100;
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(redeliveryFailingProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Redelivery = .{ .max_redeliveries = 2, .initial_delay_ms = 0 } },
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

test "IdempotentConsumer sets duplicate header" {
    const alloc = std.testing.allocator;
    const InMemoryStore = @import("state_store.zig").InMemoryStore;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .IdempotentConsumer = .{
        .key_header = "MessageId",
        .route = sub_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var store = InMemoryStore.init(alloc);
    defer store.deinit();
    var services = testServices();
    services.store = store.stateStore();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First run
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hi");
        try ex.putHeader("MessageId", "msg-1");
        try exec.run(&plan, main_rid, &ex);
        try std.testing.expectEqualStrings("hiX", ex.body);
        try std.testing.expect(ex.getHeader("CamelDuplicateMessage") == null);
    }

    // Second run (duplicate) — skipped, but header set
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hi");
        try ex.putHeader("MessageId", "msg-1");
        try exec.run(&plan, main_rid, &ex);
        try std.testing.expectEqualStrings("hi", ex.body); // not processed
        try std.testing.expectEqualStrings("true", ex.getHeader("CamelDuplicateMessage").?);
    }
}

test "IdempotentConsumer non-skip mode runs route on duplicate" {
    const alloc = std.testing.allocator;
    const InMemoryStore = @import("state_store.zig").InMemoryStore;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .IdempotentConsumer = .{
        .key_header = "MessageId",
        .route = sub_rid,
        .skip_duplicate = false,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var store = InMemoryStore.init(alloc);
    defer store.deinit();
    var services = testServices();
    services.store = store.stateStore();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First run
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hi");
        try ex.putHeader("MessageId", "msg-1");
        try exec.run(&plan, main_rid, &ex);
        try std.testing.expectEqualStrings("hiX", ex.body);
    }

    // Second run (duplicate) — still processed in non-skip mode
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hi");
        try ex.putHeader("MessageId", "msg-1");
        try exec.run(&plan, main_rid, &ex);
        try std.testing.expectEqualStrings("hiX", ex.body); // processed!
        try std.testing.expectEqualStrings("true", ex.getHeader("CamelDuplicateMessage").?);
    }
}

test "IdempotentConsumer non-eager stores after execution" {
    const alloc = std.testing.allocator;
    const InMemoryStore = @import("state_store.zig").InMemoryStore;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(appendProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .IdempotentConsumer = .{
        .key_header = "MessageId",
        .route = sub_rid,
        .eager = false,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var store = InMemoryStore.init(alloc);
    defer store.deinit();
    var services = testServices();
    services.store = store.stateStore();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First run — should execute and then store
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hi");
        try ex.putHeader("MessageId", "msg-1");
        try exec.run(&plan, main_rid, &ex);
        try std.testing.expectEqualStrings("hiX", ex.body);
    }

    // Key should now be stored
    if (try store.stateStore().get(alloc, "msg-1")) |val| {
        alloc.free(val);
    } else {
        return error.TestFailed;
    }
}
