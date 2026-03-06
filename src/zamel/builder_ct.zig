const std = @import("std");
const Step = @import("step.zig").Step;
const ChoiceBranch = @import("step.zig").ChoiceBranch;
const PolicyKind = @import("step.zig").PolicyKind;
const SplitKind = @import("step.zig").SplitKind;
const DataFormat = @import("step.zig").DataFormat;
const ClaimCheckAction = @import("step.zig").ClaimCheckAction;
const LogLevel = @import("step.zig").LogLevel;
const LoadBalancerStrategy = @import("step.zig").LoadBalancerStrategy;
const AggregationStrategy = @import("aggregation.zig").AggregationStrategy;
const RoutePlan = @import("plan.zig").RoutePlan;
const RouteId = @import("step.zig").RouteId;

const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const Splitter = @import("splitter.zig").Splitter;
const RecipientResolver = @import("recipient_resolver.zig").RecipientResolver;
const Endpoint = @import("endpoint.zig").Endpoint;
const EndpointRef = @import("endpoint.zig").EndpointRef;

const Registry = @import("registry.zig").Registry;

/// A compile-time branch for choiceOf: a predicate + the steps to run when matched.
pub const CtBranch = struct {
    pred: Predicate,
    steps: []const Step,
};

/// Compile-time route builder. Each method returns a new type with updated counts.
/// `num_steps` tracks main-route steps, `num_routes` tracks sub-routes.
pub fn CtBuilder(comptime num_steps: usize, comptime num_routes: usize) type {
    return struct {
        const Self = @This();

        steps: [num_steps]Step,
        routes: [num_routes][]const Step,
        source_uri: ?[]const u8,

        // ---- simple steps (append 1 step) ----

        pub fn process(comptime self: Self, comptime p: Processor) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Process = p });
        }

        pub fn filter(comptime self: Self, comptime pred: Predicate) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Filter = pred });
        }

        pub fn to(comptime self: Self, comptime ep: EndpointRef) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .To = ep });
        }

        pub fn toUri(comptime self: Self, comptime uri: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .To = .{ .uri = uri } });
        }

        pub fn transform(comptime self: Self, comptime p: Processor) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Transform = p });
        }

        pub fn wireTap(comptime self: Self, comptime ep: EndpointRef) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .WireTap = ep });
        }

        pub fn wireTapUri(comptime self: Self, comptime uri: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .WireTap = .{ .uri = uri } });
        }

        pub fn recipientList(comptime self: Self, comptime resolver: RecipientResolver) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .RecipientList = .{ .resolver = resolver } });
        }

        pub fn throttle(comptime self: Self, comptime interval_ms: u32) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Throttle = .{ .interval_ms = interval_ms } });
        }

        pub fn delay(comptime self: Self, comptime ms: u32) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Delay = .{ .ms = ms } });
        }

        pub fn log(comptime self: Self, comptime message: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Log = .{ .message = message } });
        }

        pub fn logAt(comptime self: Self, comptime level: LogLevel, comptime message: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Log = .{ .message = message, .level = level } });
        }

        pub fn marshal(comptime self: Self, comptime format: DataFormat) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Marshal = .{ .format = format } });
        }

        pub fn unmarshal(comptime self: Self, comptime format: DataFormat) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Unmarshal = .{ .format = format } });
        }

        pub fn marshalJson(comptime self: Self) CtBuilder(num_steps + 1, num_routes) {
            return self.marshal(.json);
        }

        pub fn unmarshalJson(comptime self: Self) CtBuilder(num_steps + 1, num_routes) {
            return self.unmarshal(.json);
        }

        pub fn claimCheckStore(comptime self: Self, comptime key: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .ClaimCheck = .{ .action = .store, .key = key } });
        }

        pub fn claimCheckRetrieve(comptime self: Self, comptime key: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        }

        pub fn routingSlip(comptime self: Self, comptime header: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .RoutingSlip = .{ .header = header } });
        }

        pub fn dynamicRouter(comptime self: Self, comptime header: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .DynamicRouter = .{ .header = header } });
        }

        pub fn enrich(comptime self: Self, comptime ep: EndpointRef, comptime merge: ?Processor) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        }

        pub fn enrichUri(comptime self: Self, comptime uri: []const u8, comptime merge: ?Processor) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .Enrich = .{ .endpoint = .{ .uri = uri }, .merge = merge } });
        }

        pub fn requestReply(comptime self: Self, comptime ep: EndpointRef) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .RequestReply = .{ .endpoint = ep } });
        }

        pub fn requestReplyUri(comptime self: Self, comptime uri: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .RequestReply = .{ .endpoint = .{ .uri = uri } } });
        }

        pub fn requestReplyWithHeader(comptime self: Self, comptime ep: EndpointRef, comptime correlation_header: []const u8) CtBuilder(num_steps + 1, num_routes) {
            return self.appendStep(.{ .RequestReply = .{ .endpoint = ep, .correlation_header = correlation_header } });
        }

        // ---- sub-route steps ----

        pub fn choiceOf(
            comptime self: Self,
            comptime branches: []const CtBranch,
            comptime otherwise_steps: ?[]const Step,
        ) CtBuilder(num_steps + 1, num_routes + branches.len + @as(usize, if (otherwise_steps != null) 1 else 0)) {
            const num_branch_routes = branches.len;
            const has_otherwise: usize = if (otherwise_steps != null) 1 else 0;
            const total_new_routes = num_branch_routes + has_otherwise;

            // Build sub-routes array: first the when-branches, then optional otherwise
            const new_routes = comptime buildChoiceRoutes(
                num_routes,
                total_new_routes,
                self.routes,
                branches,
                otherwise_steps,
            );

            // Build ChoiceBranch array for the Step via helper to get a const
            const choice_branches = comptime buildChoiceBranches(num_branch_routes, branches, num_routes);

            const otherwise_id: ?RouteId = if (otherwise_steps != null)
                @intCast(num_routes + num_branch_routes)
            else
                null;

            const step = Step{ .Choice = .{
                .branches = &choice_branches,
                .otherwise = otherwise_id,
            } };

            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;

            return .{
                .steps = new_steps,
                .routes = new_routes,
                .source_uri = self.source_uri,
            };
        }

        pub fn split(comptime self: Self, comptime delimiter: u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Split = .{ .kind = .{ .scalar = delimiter }, .route = route_id } };
                }
            }.make);
        }

        pub fn splitLines(comptime self: Self, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.split('\n', sub_steps);
        }

        pub fn splitSequence(comptime self: Self, comptime separator: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Split = .{ .kind = .{ .sequence = separator }, .route = route_id } };
                }
            }.make);
        }

        pub fn aggregate(comptime self: Self, comptime separator: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Aggregate = .{ .separator = separator, .route = route_id } };
                }
            }.make);
        }

        pub fn aggregateWithStrategy(
            comptime self: Self,
            comptime separator: []const u8,
            comptime strategy: AggregationStrategy,
            comptime sub_steps: []const Step,
        ) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Aggregate = .{ .separator = separator, .route = route_id, .strategy = strategy } };
                }
            }.make);
        }

        pub fn policy(comptime self: Self, comptime kind: PolicyKind, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Policy = .{ .kind = kind, .route = route_id } };
                }
            }.make);
        }

        pub fn retry(comptime self: Self, comptime max: u32, comptime backoff_ms: u32, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.policy(.{ .Retry = .{ .max = max, .backoff_ms = backoff_ms } }, sub_steps);
        }

        pub fn timeout(comptime self: Self, comptime ms: u32, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.policy(.{ .Timeout = .{ .ms = ms } }, sub_steps);
        }

        pub fn redelivery(comptime self: Self, comptime max: u32, comptime initial_delay_ms: u32, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.policy(.{ .Redelivery = .{
                .max_redeliveries = max,
                .initial_delay_ms = initial_delay_ms,
            } }, sub_steps);
        }

        pub fn redeliveryExponential(
            comptime self: Self,
            comptime max: u32,
            comptime initial_delay_ms: u32,
            comptime max_delay_ms: u32,
            comptime sub_steps: []const Step,
        ) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.policy(.{ .Redelivery = .{
                .max_redeliveries = max,
                .initial_delay_ms = initial_delay_ms,
                .max_delay_ms = max_delay_ms,
                .use_exponential = true,
            } }, sub_steps);
        }

        pub fn idempotent(comptime self: Self, comptime key_header: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .IdempotentConsumer = .{ .key_header = key_header, .route = route_id } };
                }
            }.make);
        }

        pub fn idempotentNonEager(comptime self: Self, comptime key_header: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .IdempotentConsumer = .{ .key_header = key_header, .route = route_id, .eager = false } };
                }
            }.make);
        }

        pub fn batchOf(comptime self: Self, comptime size: u32, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Batch = .{ .size = size, .route = route_id } };
                }
            }.make);
        }

        pub fn batchWithSeparator(comptime self: Self, comptime size: u32, comptime separator: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Batch = .{ .size = size, .separator = separator, .route = route_id } };
                }
            }.make);
        }

        pub fn resequence(comptime self: Self, comptime size: u32, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Resequencer = .{ .size = size, .route = route_id } };
                }
            }.make);
        }

        pub fn resequenceWithHeader(comptime self: Self, comptime size: u32, comptime header: []const u8, comptime sub_steps: []const Step) CtBuilder(num_steps + 1, num_routes + 1) {
            return self.addSubRouteStep(sub_steps, struct {
                fn make(route_id: RouteId) Step {
                    return .{ .Resequencer = .{ .size = size, .header = header, .route = route_id } };
                }
            }.make);
        }

        pub fn multicast(comptime self: Self, comptime branch_routes: []const []const Step) CtBuilder(num_steps + 1, num_routes + branch_routes.len) {
            return self.multicastImpl(branch_routes, false);
        }

        pub fn parallelMulticast(comptime self: Self, comptime branch_routes: []const []const Step) CtBuilder(num_steps + 1, num_routes + branch_routes.len) {
            return self.multicastImpl(branch_routes, true);
        }

        fn multicastImpl(comptime self: Self, comptime branch_routes: []const []const Step, comptime parallel: bool) CtBuilder(num_steps + 1, num_routes + branch_routes.len) {
            const new_routes = comptime buildMultiRoutes(num_routes, branch_routes.len, self.routes, branch_routes);
            const route_ids = comptime buildRouteIds(branch_routes.len, num_routes);

            const step = Step{ .Multicast = .{ .routes = &route_ids, .parallel = parallel } };

            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;

            return .{
                .steps = new_steps,
                .routes = new_routes,
                .source_uri = self.source_uri,
            };
        }

        pub fn loadBalancer(comptime self: Self, comptime strategy: LoadBalancerStrategy, comptime branch_routes: []const []const Step) CtBuilder(num_steps + 1, num_routes + branch_routes.len) {
            const new_routes = comptime buildMultiRoutes(num_routes, branch_routes.len, self.routes, branch_routes);
            const route_ids = comptime buildRouteIds(branch_routes.len, num_routes);

            const step = Step{ .LoadBalancer = .{ .strategy = strategy, .routes = &route_ids } };

            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;

            return .{
                .steps = new_steps,
                .routes = new_routes,
                .source_uri = self.source_uri,
            };
        }

        pub fn doTry(
            comptime self: Self,
            comptime try_steps: []const Step,
            comptime catch_steps: ?[]const Step,
            comptime finally_steps: ?[]const Step,
        ) CtBuilder(num_steps + 1, num_routes + 1 + @as(usize, if (catch_steps != null) 1 else 0) + @as(usize, if (finally_steps != null) 1 else 0)) {
            const has_catch: usize = if (catch_steps != null) 1 else 0;
            const has_finally: usize = if (finally_steps != null) 1 else 0;
            const total_new = 1 + has_catch + has_finally;

            var new_routes: [num_routes + total_new][]const Step = undefined;
            for (0..num_routes) |i| {
                new_routes[i] = self.routes[i];
            }
            new_routes[num_routes] = try_steps;
            const try_id: RouteId = @intCast(num_routes);

            var catch_id: ?RouteId = null;
            if (catch_steps) |cs| {
                new_routes[num_routes + 1] = cs;
                catch_id = @intCast(num_routes + 1);
            }

            var finally_id: ?RouteId = null;
            if (finally_steps) |fs| {
                new_routes[num_routes + 1 + has_catch] = fs;
                finally_id = @intCast(num_routes + 1 + has_catch);
            }

            const step = Step{ .DoTry = .{
                .try_route = try_id,
                .catch_route = catch_id,
                .finally_route = finally_id,
            } };

            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;

            return .{
                .steps = new_steps,
                .routes = new_routes,
                .source_uri = self.source_uri,
            };
        }

        // ---- source ----

        pub fn fromUri(comptime self: Self, comptime uri: []const u8) Self {
            var result = self;
            result.source_uri = uri;
            return result;
        }

        // ---- finalization ----

        /// Builds the compile-time plan. The main route is appended last.
        pub fn build(comptime self: Self) CtPlan(num_routes + 1) {
            var all_routes: [num_routes + 1][]const Step = undefined;
            for (0..num_routes) |i| {
                all_routes[i] = self.routes[i];
            }
            all_routes[num_routes] = &self.steps;
            return .{
                .routes = all_routes,
                .main_route_id = @intCast(num_routes),
                .source_uri = self.source_uri,
            };
        }

        // ---- internal helpers ----

        fn appendStep(comptime self: Self, comptime step: Step) CtBuilder(num_steps + 1, num_routes) {
            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;
            return .{
                .steps = new_steps,
                .routes = self.routes,
                .source_uri = self.source_uri,
            };
        }

        fn addSubRouteStep(
            comptime self: Self,
            comptime sub_steps: []const Step,
            comptime makeStepFn: fn (RouteId) Step,
        ) CtBuilder(num_steps + 1, num_routes + 1) {
            var new_routes: [num_routes + 1][]const Step = undefined;
            for (0..num_routes) |i| {
                new_routes[i] = self.routes[i];
            }
            new_routes[num_routes] = sub_steps;

            const route_id: RouteId = @intCast(num_routes);
            const step = makeStepFn(route_id);

            var new_steps: [num_steps + 1]Step = undefined;
            for (0..num_steps) |i| {
                new_steps[i] = self.steps[i];
            }
            new_steps[num_steps] = step;

            return .{
                .steps = new_steps,
                .routes = new_routes,
                .source_uri = self.source_uri,
            };
        }
    };
}

// ---- comptime helper functions (return const arrays, avoiding comptime var pointer issues) ----

fn buildChoiceBranches(
    comptime n: usize,
    comptime branches: []const CtBranch,
    comptime base_id: usize,
) [n]ChoiceBranch {
    var result: [n]ChoiceBranch = undefined;
    for (0..n) |i| {
        result[i] = .{
            .when = branches[i].pred,
            .route = @intCast(base_id + i),
        };
    }
    return result;
}

fn buildChoiceRoutes(
    comptime existing: usize,
    comptime new_count: usize,
    comptime old_routes: [existing][]const Step,
    comptime branches: []const CtBranch,
    comptime otherwise_steps: ?[]const Step,
) [existing + new_count][]const Step {
    var result: [existing + new_count][]const Step = undefined;
    for (0..existing) |i| {
        result[i] = old_routes[i];
    }
    for (0..branches.len) |i| {
        result[existing + i] = branches[i].steps;
    }
    if (otherwise_steps) |ow| {
        result[existing + branches.len] = ow;
    }
    return result;
}

fn buildMultiRoutes(
    comptime existing: usize,
    comptime new_count: usize,
    comptime old_routes: [existing][]const Step,
    comptime branch_routes: []const []const Step,
) [existing + new_count][]const Step {
    var result: [existing + new_count][]const Step = undefined;
    for (0..existing) |i| {
        result[i] = old_routes[i];
    }
    for (0..new_count) |i| {
        result[existing + i] = branch_routes[i];
    }
    return result;
}

fn buildRouteIds(comptime n: usize, comptime base_id: usize) [n]RouteId {
    var result: [n]RouteId = undefined;
    for (0..n) |i| {
        result[i] = @intCast(base_id + i);
    }
    return result;
}

/// Creates a fresh compile-time builder with no steps and no sub-routes.
pub fn create() CtBuilder(0, 0) {
    return .{
        .steps = .{},
        .routes = .{},
        .source_uri = null,
    };
}

/// Compile-time plan holding route arrays. Call `materialize` to produce a runtime `RoutePlan`.
pub fn CtPlan(comptime num_routes: usize) type {
    return struct {
        const Self = @This();

        routes: [num_routes][]const Step,
        main_route_id: RouteId,
        source_uri: ?[]const u8,

        /// Materialize into a runtime RoutePlan, resolving `.uri` EndpointRefs via registry.
        pub fn materialize(comptime self: Self, allocator: std.mem.Allocator, registry: ?*Registry) !RoutePlan {
            var plan = RoutePlan.init();
            errdefer {
                for (plan.routes.items) |r| allocator.free(r.steps);
                plan.deinit(allocator);
            }

            inline for (self.routes) |ct_route| {
                const steps = try allocator.alloc(Step, ct_route.len);
                errdefer allocator.free(steps);
                for (ct_route, 0..) |step, i| {
                    steps[i] = try resolveStep(step, allocator, registry);
                }
                _ = try plan.addRoute(allocator, steps);
            }

            return plan;
        }

        /// Materialize without a registry. Errors if any `.uri` EndpointRefs exist.
        pub fn materializeSimple(comptime self: Self, allocator: std.mem.Allocator) !RoutePlan {
            return self.materialize(allocator, null);
        }

        /// Free a materialized plan including its step slices.
        pub fn deinitPlan(_: Self, plan: *RoutePlan, allocator: std.mem.Allocator) void {
            for (plan.routes.items) |r| {
                allocator.free(r.steps);
            }
            plan.deinit(allocator);
        }
    };
}

/// Resolve `.uri` EndpointRefs in steps that contain them.
fn resolveStep(step: Step, allocator: std.mem.Allocator, registry: ?*Registry) !Step {
    switch (step) {
        .To => |eref| return .{ .To = try resolveRef(eref, allocator, registry) },
        .WireTap => |eref| return .{ .WireTap = try resolveRef(eref, allocator, registry) },
        .Enrich => |e| return .{ .Enrich = .{
            .endpoint = try resolveRef(e.endpoint, allocator, registry),
            .merge = e.merge,
        } },
        else => return step,
    }
}

fn resolveRef(eref: EndpointRef, allocator: std.mem.Allocator, registry: ?*Registry) !EndpointRef {
    switch (eref) {
        .endpoint => return eref,
        .uri => |uri| {
            const reg = registry orelse return error.RegistryNotSet;
            return .{ .endpoint = try reg.resolve(allocator, uri) };
        },
    }
}

// -------- tests --------

const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const ct_pred = @import("ct_predicates.zig");
const ct_proc = @import("ct_processors.zig");

fn noopProcessor(_: ?*anyopaque, _: *Exchange) anyerror!void {}

fn uppercaseProcessor(_: ?*anyopaque, ex: *Exchange) anyerror!void {
    for (ex.body) |*ch| {
        if (ch.* >= 'a' and ch.* <= 'z') ch.* -= 32;
    }
}

fn appendBangProcessor(_: ?*anyopaque, ex: *Exchange) anyerror!void {
    const new = try ex.allocator.alloc(u8, ex.body.len + 1);
    @memcpy(new[0..ex.body.len], ex.body);
    new[ex.body.len] = '!';
    if (ex.body.len != 0) ex.allocator.free(ex.body);
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

test "ct builder: simple pipeline with process" {
    const ct = comptime create()
        .process(Processor.fromFn(uppercaseProcessor, null))
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "ct builder: chained process steps" {
    const ct = comptime create()
        .process(Processor.fromFn(uppercaseProcessor, null))
        .process(Processor.fromFn(appendBangProcessor, null))
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("HELLO!", ex.body);
}

test "ct builder: choiceOf with otherwise" {
    const handle_a = comptime ct_proc.setBody("got-A");
    const handle_default = comptime ct_proc.setBody("got-default");

    const ct = comptime create()
        .choiceOf(
            &.{.{ .pred = ct_pred.headerEq("type", "A"), .steps = &.{Step{ .Process = handle_a }} }},
            &.{Step{ .Process = handle_default }},
        )
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    // Test branch A
    {
        var ex = Exchange.init(std.testing.allocator);
        defer ex.deinit();
        try ex.putHeader("type", "A");
        try ex.setBody("input");

        var executor = SyncExecutor.init(testServices());
        defer executor.deinit();
        try executor.run(&plan, ct.main_route_id, &ex);

        try std.testing.expectEqualStrings("got-A", ex.body);
    }

    // Test otherwise
    {
        var ex = Exchange.init(std.testing.allocator);
        defer ex.deinit();
        try ex.putHeader("type", "B");
        try ex.setBody("input");

        var executor = SyncExecutor.init(testServices());
        defer executor.deinit();
        try executor.run(&plan, ct.main_route_id, &ex);

        try std.testing.expectEqualStrings("got-default", ex.body);
    }
}

test "ct builder: choiceOf without otherwise" {
    const handle_a = comptime ct_proc.setBody("got-A");

    const ct = comptime create()
        .choiceOf(
            &.{.{ .pred = ct_pred.headerEq("type", "A"), .steps = &.{Step{ .Process = handle_a }} }},
            null,
        )
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    // No match, no otherwise — exchange unchanged
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.putHeader("type", "B");
    try ex.setBody("original");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("original", ex.body);
}

test "ct builder: split" {
    const ct = comptime create()
        .split(',', &.{Step{ .Process = Processor.fromFn(uppercaseProcessor, null) }})
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("a,b,c");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    // Split runs sub-routes on copies; parent body is unchanged
    try std.testing.expectEqualStrings("a,b,c", ex.body);
}

test "ct builder: aggregate" {
    const ct = comptime create()
        .aggregate("|", &.{Step{ .Process = Processor.fromFn(uppercaseProcessor, null) }})
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "ct builder: retry policy" {
    var call_count: u32 = 0;
    const Ctx = struct {
        count: *u32,
    };
    var ctx = Ctx{ .count = &call_count };

    const ct = comptime create()
        .retry(3, 0, &.{Step{ .Process = Processor.fromFn(noopProcessor, null) }})
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    // Patch the sub-route step to use our counting processor
    const counting_proc = Processor.fromFn(struct {
        fn call(c: ?*anyopaque, _: *Exchange) anyerror!void {
            const self: *Ctx = @ptrCast(@alignCast(c.?));
            self.count.* += 1;
            if (self.count.* < 2) return error.Retry;
        }
    }.call, &ctx);

    // The sub-route is route 0, replace its step
    const sub_route = plan.route(0);
    sub_route.steps[0] = .{ .Process = counting_proc };

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("test");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqual(@as(u32, 2), call_count);
}

test "ct builder: multicast" {
    const ct = comptime create()
        .multicast(&.{
            &.{Step{ .Process = ct_proc.setHeader("branch", "1") }},
            &.{Step{ .Process = ct_proc.setHeader("branch", "2") }},
        })
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("data");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    // Multicast runs both branches; last one's header survives on the exchange
    try std.testing.expectEqualStrings("2", ex.getHeader("branch").?);
}

fn failProcessor(_: ?*anyopaque, _: *Exchange) anyerror!void {
    return error.TestError;
}

test "ct builder: doTry with catch" {
    const ct = comptime create()
        .doTry(
            &.{Step{ .Process = Processor.fromFn(failProcessor, null) }},
            &.{Step{ .Process = ct_proc.setBody("caught") }},
            null,
        )
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("original");

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("caught", ex.body);
}

test "ct builder: comptime structure assertions" {
    const ct = comptime create()
        .process(Processor.fromFn(noopProcessor, null))
        .process(Processor.fromFn(noopProcessor, null))
        .build();

    // Main route has 2 steps, total 1 route (main only)
    comptime {
        try std.testing.expectEqual(@as(usize, 1), ct.routes.len);
        try std.testing.expectEqual(@as(usize, 2), ct.routes[ct.main_route_id].len);
        try std.testing.expectEqual(@as(RouteId, 0), ct.main_route_id);
    }
}

test "ct builder: comptime structure with sub-routes" {
    const ct = comptime create()
        .process(Processor.fromFn(noopProcessor, null))
        .choiceOf(
            &.{
                .{ .pred = ct_pred.headerEq("x", "1"), .steps = &.{Step{ .Process = Processor.fromFn(noopProcessor, null) }} },
                .{ .pred = ct_pred.headerEq("x", "2"), .steps = &.{Step{ .Process = Processor.fromFn(noopProcessor, null) }} },
            },
            &.{Step{ .Process = Processor.fromFn(noopProcessor, null) }},
        )
        .build();

    comptime {
        // 2 when branches + 1 otherwise + 1 main = 4 routes
        try std.testing.expectEqual(@as(usize, 4), ct.routes.len);
        try std.testing.expectEqual(@as(RouteId, 3), ct.main_route_id);
        // Main route has 2 steps: process + choice
        try std.testing.expectEqual(@as(usize, 2), ct.routes[ct.main_route_id].len);
    }
}

test "ct builder: fromUri sets source" {
    const ct = comptime create()
        .fromUri("timer:1000")
        .process(Processor.fromFn(noopProcessor, null))
        .build();

    comptime {
        try std.testing.expect(ct.source_uri != null);
        try std.testing.expectEqualStrings("timer:1000", ct.source_uri.?);
    }
}

test "ct builder: materialize + execute roundtrip" {
    const ct = comptime create()
        .process(ct_proc.setBody("step1"))
        .process(ct_proc.setHeader("processed", "true"))
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("step1", ex.body);
    try std.testing.expectEqualStrings("true", ex.getHeader("processed").?);
}

test "ct builder: toUri with registry" {
    const ct = comptime create()
        .process(ct_proc.setBody("routed"))
        .toUri("log:info")
        .build();

    var reg = Registry.init(testServices());
    defer reg.deinit();

    var plan = try ct.materialize(std.testing.allocator, &reg);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    var executor = SyncExecutor.init(testServices());
    defer executor.deinit();
    executor.registry = &reg;
    try executor.run(&plan, ct.main_route_id, &ex);

    try std.testing.expectEqualStrings("routed", ex.body);
}

test "ct builder: materializeSimple errors on uri ref" {
    const ct = comptime create()
        .toUri("log:info")
        .build();

    const result = ct.materializeSimple(std.testing.allocator);
    try std.testing.expectError(error.RegistryNotSet, result);
}

test "ct builder: filter step" {
    const ct = comptime create()
        .filter(ct_pred.bodyContains("keep"))
        .process(ct_proc.setHeader("filtered", "yes"))
        .build();

    var plan = try ct.materializeSimple(std.testing.allocator);
    defer ct.deinitPlan(&plan, std.testing.allocator);

    // Test: body matches filter — processing continues
    {
        var ex = Exchange.init(std.testing.allocator);
        defer ex.deinit();
        try ex.setBody("keep this");

        var executor = SyncExecutor.init(testServices());
        defer executor.deinit();
        try executor.run(&plan, ct.main_route_id, &ex);

        try std.testing.expectEqualStrings("yes", ex.getHeader("filtered").?);
    }

    // Test: body does not match filter — processing stops
    {
        var ex = Exchange.init(std.testing.allocator);
        defer ex.deinit();
        try ex.setBody("drop this");

        var executor = SyncExecutor.init(testServices());
        defer executor.deinit();
        try executor.run(&plan, ct.main_route_id, &ex);

        try std.testing.expect(ex.getHeader("filtered") == null);
    }
}
