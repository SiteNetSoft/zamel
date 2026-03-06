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
const processors = @import("processors.zig");

pub const RtBuilder = struct {
    gpa: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    plan: RoutePlan,

    registry: *Registry,

    // current route steps
    cur_steps: std.ArrayList(Step),

    // For sources
    source_uri: ?[]const u8 = null,

    pub fn init(gpa: std.mem.Allocator, registry: *Registry) RtBuilder {
        const arena = std.heap.ArenaAllocator.init(gpa);
        return .{
            .gpa = gpa,
            .arena = arena,
            .plan = RoutePlan.init(),
            .registry = registry,
            .cur_steps = .empty,
        };
    }

    pub fn deinit(self: *RtBuilder) void {
        // Routes were allocated from the arena, so pass the arena allocator
        self.plan.deinit(self.arena.allocator());
        self.arena.deinit();
    }

    // ---- basic steps ----

    pub fn filter(self: *RtBuilder, pred: Predicate) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Filter = pred });
        return self;
    }

    pub fn process(self: *RtBuilder, p: Processor) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Process = p });
        return self;
    }

    pub fn setBody(self: *RtBuilder, value: []const u8) !*RtBuilder {
        return try self.process(try processors.setBody(self.arena.allocator(), value));
    }

    pub fn setHeader(self: *RtBuilder, key: []const u8, value: []const u8) !*RtBuilder {
        return try self.process(try processors.setHeader(self.arena.allocator(), key, value));
    }

    pub fn removeHeader(self: *RtBuilder, key: []const u8) !*RtBuilder {
        return try self.process(try processors.removeHeader(self.arena.allocator(), key));
    }

    pub fn to(self: *RtBuilder, ep: EndpointRef) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *RtBuilder, uri: []const u8) !*RtBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    // ---- source ----

    pub fn fromUri(self: *RtBuilder, uri: []const u8) !*RtBuilder {
        // For now we only support timer as a source in demo (in start()).
        // Store URI; start() will interpret it.
        self.source_uri = try self.arena.allocator().dupe(u8, uri);
        return self;
    }

    // ---- choice DSL ----

    pub fn choice(self: *RtBuilder) ChoiceBuilder {
        return ChoiceBuilder.init(self);
    }

    // ---- multicast DSL ----

    pub fn multicast(self: *RtBuilder) MulticastBuilder {
        return MulticastBuilder.init(self);
    }

    pub fn parallelMulticast(self: *RtBuilder) MulticastBuilder {
        return MulticastBuilder.initParallel(self);
    }

    // ---- wire tap ----

    pub fn wireTap(self: *RtBuilder, ep: EndpointRef) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .WireTap = ep });
        return self;
    }

    pub fn wireTapUri(self: *RtBuilder, uri: []const u8) !*RtBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return try self.wireTap(.{ .endpoint = ep });
    }

    // ---- recipient list ----

    pub fn recipientList(self: *RtBuilder, resolver: RecipientResolver) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .RecipientList = .{ .resolver = resolver } });
        return self;
    }

    // ---- throttle ----

    pub fn throttle(self: *RtBuilder, interval_ms: u32) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Throttle = .{ .interval_ms = interval_ms } });
        return self;
    }

    // ---- split / aggregate DSL ----

    pub fn split(self: *RtBuilder, delimiter: u8) SplitBuilder {
        return SplitBuilder.init(self, .{ .scalar = delimiter });
    }

    pub fn splitLines(self: *RtBuilder) SplitBuilder {
        return SplitBuilder.init(self, .{ .scalar = '\n' });
    }

    pub fn splitSequence(self: *RtBuilder, separator: []const u8) SplitBuilder {
        return SplitBuilder.init(self, .{ .sequence = separator });
    }

    pub fn splitBy(self: *RtBuilder, splitter: Splitter) SplitBuilder {
        return SplitBuilder.init(self, .{ .custom = splitter });
    }

    pub fn aggregate(self: *RtBuilder, separator: []const u8) AggregateBuilder {
        return AggregateBuilder.init(self, separator);
    }

    pub fn aggregateWith(self: *RtBuilder, separator: []const u8, strategy: AggregationStrategy) AggregateBuilder {
        return AggregateBuilder.initWithStrategy(self, separator, strategy);
    }

    // ---- policy DSL ----

    pub fn retry(self: *RtBuilder, max: u32, backoff_ms: u32) !PolicyBuilder {
        if (max == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .Retry = .{ .max = max, .backoff_ms = backoff_ms } });
    }

    pub fn timeout(self: *RtBuilder, ms: u32) !PolicyBuilder {
        if (ms == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .Timeout = .{ .ms = ms } });
    }

    pub fn deadLetter(self: *RtBuilder, ep: Endpoint) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .DeadLetter = .{ .endpoint = .{ .endpoint = ep } } });
    }

    pub fn deadLetterUri(self: *RtBuilder, uri: []const u8) !PolicyBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return PolicyBuilder.init(self, .{ .DeadLetter = .{ .endpoint = .{ .endpoint = ep } } });
    }

    pub fn circuitBreaker(self: *RtBuilder, failure_threshold: u32, reset_ms: u32) !PolicyBuilder {
        if (failure_threshold == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .CircuitBreaker = .{ .failure_threshold = failure_threshold, .reset_ms = reset_ms } });
    }

    pub fn deadLetterWithRetry(self: *RtBuilder, ep: Endpoint, retries: u32, backoff_ms: u32) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .DeadLetter = .{
            .endpoint = .{ .endpoint = ep },
            .retries = retries,
            .retry_backoff_ms = backoff_ms,
        } });
    }

    pub fn deadLetterUriWithRetry(self: *RtBuilder, uri: []const u8, retries: u32, backoff_ms: u32) !PolicyBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return PolicyBuilder.init(self, .{ .DeadLetter = .{
            .endpoint = .{ .endpoint = ep },
            .retries = retries,
            .retry_backoff_ms = backoff_ms,
        } });
    }

    pub fn circuitBreakerAdvanced(self: *RtBuilder, failure_threshold: u32, reset_ms: u32, success_threshold: u32) !PolicyBuilder {
        if (failure_threshold == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .CircuitBreaker = .{
            .failure_threshold = failure_threshold,
            .reset_ms = reset_ms,
            .success_threshold = success_threshold,
        } });
    }

    pub fn redelivery(self: *RtBuilder, max: u32, initial_delay_ms: u32) !PolicyBuilder {
        if (max == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .Redelivery = .{
            .max_redeliveries = max,
            .initial_delay_ms = initial_delay_ms,
        } });
    }

    pub fn redeliveryExponential(self: *RtBuilder, max: u32, initial_delay_ms: u32, max_delay_ms: u32) !PolicyBuilder {
        if (max == 0) return error.InvalidPolicyConfig;
        return PolicyBuilder.init(self, .{ .Redelivery = .{
            .max_redeliveries = max,
            .initial_delay_ms = initial_delay_ms,
            .max_delay_ms = max_delay_ms,
            .use_exponential = true,
        } });
    }

    // ---- idempotent DSL ----

    pub fn idempotent(self: *RtBuilder, key_header: []const u8) IdempotentBuilder {
        return IdempotentBuilder.init(self, key_header);
    }

    pub fn idempotentNonEager(self: *RtBuilder, key_header: []const u8) IdempotentBuilder {
        var ib = IdempotentBuilder.init(self, key_header);
        ib.eager = false;
        return ib;
    }

    // ---- doTry DSL ----

    pub fn doTry(self: *RtBuilder) DoTryBuilder {
        return DoTryBuilder.init(self);
    }

    // ---- marshal/unmarshal ----

    pub fn marshal(self: *RtBuilder, format: DataFormat) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *RtBuilder, format: DataFormat) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn marshalJson(self: *RtBuilder) !*RtBuilder {
        return try self.marshal(.json);
    }

    pub fn unmarshalJson(self: *RtBuilder) !*RtBuilder {
        return try self.unmarshal(.json);
    }

    // ---- claim check ----

    pub fn claimCheckStore(self: *RtBuilder, key: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *RtBuilder, key: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    // ---- enrich ----

    pub fn enrich(self: *RtBuilder, ep: EndpointRef, merge: ?Processor) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *RtBuilder, uri: []const u8, merge: ?Processor) !*RtBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    // ---- delay ----

    pub fn delay(self: *RtBuilder, ms: u32) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    // ---- log ----

    pub fn log(self: *RtBuilder, message: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *RtBuilder, level: LogLevel, message: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    // ---- routing slip ----

    pub fn routingSlip(self: *RtBuilder, header: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    // ---- dynamic router ----

    pub fn dynamicRouter(self: *RtBuilder, header: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .DynamicRouter = .{ .header = header } });
        return self;
    }

    // ---- transform ----

    pub fn transform(self: *RtBuilder, p: Processor) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .Transform = p });
        return self;
    }

    // ---- batch DSL ----

    pub fn batch(self: *RtBuilder, size: u32) BatchBuilder {
        return BatchBuilder.init(self, size, "\n");
    }

    pub fn batchWithSeparator(self: *RtBuilder, size: u32, separator: []const u8) BatchBuilder {
        return BatchBuilder.init(self, size, separator);
    }

    // ---- request-reply ----

    pub fn requestReply(self: *RtBuilder, ep: EndpointRef) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .RequestReply = .{ .endpoint = ep } });
        return self;
    }

    pub fn requestReplyUri(self: *RtBuilder, uri: []const u8) !*RtBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return try self.requestReply(.{ .endpoint = ep });
    }

    pub fn requestReplyWithHeader(self: *RtBuilder, ep: EndpointRef, correlation_header: []const u8) !*RtBuilder {
        try self.cur_steps.append(self.arena.allocator(), .{ .RequestReply = .{
            .endpoint = ep,
            .correlation_header = correlation_header,
        } });
        return self;
    }

    // ---- resequencer DSL ----

    pub fn resequencer(self: *RtBuilder, size: u32) ResequencerBuilder {
        return ResequencerBuilder.init(self, size, "CamelSequenceNumber");
    }

    pub fn resequencerWithHeader(self: *RtBuilder, size: u32, header: []const u8) ResequencerBuilder {
        return ResequencerBuilder.init(self, size, header);
    }

    // ---- load balancer DSL ----

    pub fn loadBalancer(self: *RtBuilder, strategy: LoadBalancerStrategy) LoadBalancerBuilder {
        return LoadBalancerBuilder.init(self, strategy);
    }

    // ---- finalize ----

    pub fn build(self: *RtBuilder) !RouteId {
        const steps_slice = self.cur_steps.items;
        return try self.plan.addRoute(self.arena.allocator(), steps_slice);
    }

    pub fn register(self: *RtBuilder, name: []const u8) !void {
        const route_id = try self.build();
        try self.registry.registerRoute(name, &self.plan, route_id);
    }

    pub fn start(self: *RtBuilder) !void {
        const route_id = try self.build();
        const src = self.source_uri orelse return error.NoSource;

        // Only timer: supported in demo
        try self.registry.startSource(self.gpa, src, &self.plan, route_id);
    }
};

pub const ChoiceBuilder = struct {
    parent: *RtBuilder,
    branches: std.ArrayList(ChoiceBranch),
    otherwise_route: ?RouteId = null,

    pub fn init(parent: *RtBuilder) ChoiceBuilder {
        return .{
            .parent = parent,
            .branches = .empty,
        };
    }

    fn alloc(self: *ChoiceBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn when(self: *ChoiceBuilder, pred: Predicate) WhenBuilder {
        return WhenBuilder.init(self, pred);
    }

    pub fn otherwise(self: *ChoiceBuilder) OtherwiseBuilder {
        return OtherwiseBuilder.init(self);
    }

    pub fn end(self: *ChoiceBuilder) !*RtBuilder {
        try self.parent.cur_steps.append(self.alloc(), .{ .Choice = .{
            .branches = self.branches.items,
            .otherwise = self.otherwise_route,
        } });
        return self.parent;
    }
};

pub const WhenBuilder = struct {
    choice: *ChoiceBuilder,
    pred: Predicate,
    steps: std.ArrayList(Step),

    pub fn init(choice: *ChoiceBuilder, pred: Predicate) WhenBuilder {
        return .{
            .choice = choice,
            .pred = pred,
            .steps = .empty,
        };
    }

    fn alloc(self: *WhenBuilder) std.mem.Allocator {
        return self.choice.alloc();
    }

    pub fn to(self: *WhenBuilder, ep: EndpointRef) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *WhenBuilder, uri: []const u8) !*WhenBuilder {
        const ep = try self.choice.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn filter(self: *WhenBuilder, pred: Predicate) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn process(self: *WhenBuilder, p: Processor) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn setBody(self: *WhenBuilder, value: []const u8) !*WhenBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *WhenBuilder, key: []const u8, value: []const u8) !*WhenBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *WhenBuilder, key: []const u8) !*WhenBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *WhenBuilder, ep: EndpointRef, merge: ?Processor) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *WhenBuilder, uri: []const u8, merge: ?Processor) !*WhenBuilder {
        const ep = try self.choice.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *WhenBuilder, format: DataFormat) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *WhenBuilder, format: DataFormat) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *WhenBuilder, key: []const u8) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *WhenBuilder, key: []const u8) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *WhenBuilder, ms: u32) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *WhenBuilder, message: []const u8) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *WhenBuilder, level: LogLevel, message: []const u8) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *WhenBuilder, header: []const u8) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *WhenBuilder, p: Processor) !*WhenBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endWhen(self: *WhenBuilder) !*ChoiceBuilder {
        const a = self.alloc();
        const rid = try self.choice.parent.plan.addRoute(a, self.steps.items);
        try self.choice.branches.append(a, .{ .when = self.pred, .route = rid });
        return self.choice;
    }
};

pub const OtherwiseBuilder = struct {
    choice: *ChoiceBuilder,
    steps: std.ArrayList(Step),

    pub fn init(choice: *ChoiceBuilder) OtherwiseBuilder {
        return .{
            .choice = choice,
            .steps = .empty,
        };
    }

    fn alloc(self: *OtherwiseBuilder) std.mem.Allocator {
        return self.choice.alloc();
    }

    pub fn to(self: *OtherwiseBuilder, ep: EndpointRef) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *OtherwiseBuilder, uri: []const u8) !*OtherwiseBuilder {
        const ep = try self.choice.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn process(self: *OtherwiseBuilder, p: Processor) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *OtherwiseBuilder, pred: Predicate) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn setBody(self: *OtherwiseBuilder, value: []const u8) !*OtherwiseBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *OtherwiseBuilder, key: []const u8, value: []const u8) !*OtherwiseBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *OtherwiseBuilder, key: []const u8) !*OtherwiseBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *OtherwiseBuilder, ep: EndpointRef, merge: ?Processor) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *OtherwiseBuilder, uri: []const u8, merge: ?Processor) !*OtherwiseBuilder {
        const ep = try self.choice.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *OtherwiseBuilder, format: DataFormat) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *OtherwiseBuilder, format: DataFormat) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *OtherwiseBuilder, key: []const u8) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *OtherwiseBuilder, key: []const u8) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *OtherwiseBuilder, ms: u32) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *OtherwiseBuilder, message: []const u8) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *OtherwiseBuilder, level: LogLevel, message: []const u8) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *OtherwiseBuilder, header: []const u8) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *OtherwiseBuilder, p: Processor) !*OtherwiseBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endOtherwise(self: *OtherwiseBuilder) !*ChoiceBuilder {
        const rid = try self.choice.parent.plan.addRoute(self.alloc(), self.steps.items);
        self.choice.otherwise_route = rid;
        return self.choice;
    }
};

pub const PolicyBuilder = struct {
    parent: *RtBuilder,
    kind: PolicyKind,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, kind: PolicyKind) PolicyBuilder {
        return .{
            .parent = parent,
            .kind = kind,
            .steps = .empty,
        };
    }

    fn alloc(self: *PolicyBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *PolicyBuilder, p: Processor) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *PolicyBuilder, pred: Predicate) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *PolicyBuilder, ep: EndpointRef) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *PolicyBuilder, uri: []const u8) !*PolicyBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *PolicyBuilder, value: []const u8) !*PolicyBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *PolicyBuilder, key: []const u8, value: []const u8) !*PolicyBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *PolicyBuilder, key: []const u8) !*PolicyBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *PolicyBuilder, ep: EndpointRef, merge: ?Processor) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *PolicyBuilder, uri: []const u8, merge: ?Processor) !*PolicyBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *PolicyBuilder, format: DataFormat) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *PolicyBuilder, format: DataFormat) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *PolicyBuilder, key: []const u8) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *PolicyBuilder, key: []const u8) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *PolicyBuilder, ms: u32) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *PolicyBuilder, message: []const u8) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *PolicyBuilder, level: LogLevel, message: []const u8) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *PolicyBuilder, header: []const u8) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *PolicyBuilder, p: Processor) !*PolicyBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endPolicy(self: *PolicyBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Policy = .{
            .kind = self.kind,
            .route = rid,
        } });
        return self.parent;
    }
};

pub const SplitBuilder = struct {
    parent: *RtBuilder,
    kind: SplitKind,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, kind: SplitKind) SplitBuilder {
        return .{
            .parent = parent,
            .kind = kind,
            .steps = .empty,
        };
    }

    fn alloc(self: *SplitBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *SplitBuilder, p: Processor) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *SplitBuilder, pred: Predicate) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *SplitBuilder, ep: EndpointRef) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *SplitBuilder, uri: []const u8) !*SplitBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *SplitBuilder, value: []const u8) !*SplitBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *SplitBuilder, key: []const u8, value: []const u8) !*SplitBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *SplitBuilder, key: []const u8) !*SplitBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *SplitBuilder, ep: EndpointRef, merge: ?Processor) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *SplitBuilder, uri: []const u8, merge: ?Processor) !*SplitBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *SplitBuilder, format: DataFormat) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *SplitBuilder, format: DataFormat) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *SplitBuilder, key: []const u8) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *SplitBuilder, key: []const u8) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *SplitBuilder, ms: u32) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *SplitBuilder, message: []const u8) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *SplitBuilder, level: LogLevel, message: []const u8) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *SplitBuilder, header: []const u8) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *SplitBuilder, p: Processor) !*SplitBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endSplit(self: *SplitBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Split = .{
            .kind = self.kind,
            .route = rid,
        } });
        return self.parent;
    }
};

pub const AggregateBuilder = struct {
    parent: *RtBuilder,
    separator: []const u8,
    strategy: ?AggregationStrategy = null,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, separator: []const u8) AggregateBuilder {
        return .{
            .parent = parent,
            .separator = separator,
            .steps = .empty,
        };
    }

    pub fn initWithStrategy(parent: *RtBuilder, separator: []const u8, strategy: AggregationStrategy) AggregateBuilder {
        return .{
            .parent = parent,
            .separator = separator,
            .strategy = strategy,
            .steps = .empty,
        };
    }

    fn alloc(self: *AggregateBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *AggregateBuilder, p: Processor) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *AggregateBuilder, pred: Predicate) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *AggregateBuilder, ep: EndpointRef) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *AggregateBuilder, uri: []const u8) !*AggregateBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *AggregateBuilder, value: []const u8) !*AggregateBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *AggregateBuilder, key: []const u8, value: []const u8) !*AggregateBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *AggregateBuilder, key: []const u8) !*AggregateBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *AggregateBuilder, ep: EndpointRef, merge: ?Processor) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *AggregateBuilder, uri: []const u8, merge: ?Processor) !*AggregateBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *AggregateBuilder, format: DataFormat) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *AggregateBuilder, format: DataFormat) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *AggregateBuilder, key: []const u8) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *AggregateBuilder, key: []const u8) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *AggregateBuilder, ms: u32) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *AggregateBuilder, message: []const u8) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *AggregateBuilder, level: LogLevel, message: []const u8) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *AggregateBuilder, header: []const u8) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *AggregateBuilder, p: Processor) !*AggregateBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endAggregate(self: *AggregateBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Aggregate = .{
            .separator = self.separator,
            .route = rid,
            .strategy = self.strategy,
        } });
        return self.parent;
    }
};

pub const MulticastBuilder = struct {
    parent: *RtBuilder,
    routes: std.ArrayList(RouteId),
    parallel: bool = false,

    pub fn init(parent: *RtBuilder) MulticastBuilder {
        return .{
            .parent = parent,
            .routes = .empty,
        };
    }

    pub fn initParallel(parent: *RtBuilder) MulticastBuilder {
        return .{
            .parent = parent,
            .routes = .empty,
            .parallel = true,
        };
    }

    fn alloc(self: *MulticastBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn branch(self: *MulticastBuilder) MulticastBranchBuilder {
        return MulticastBranchBuilder.init(self);
    }

    pub fn endMulticast(self: *MulticastBuilder) !*RtBuilder {
        try self.parent.cur_steps.append(self.alloc(), .{ .Multicast = .{
            .routes = self.routes.items,
            .parallel = self.parallel,
        } });
        return self.parent;
    }
};

pub const MulticastBranchBuilder = struct {
    mc: *MulticastBuilder,
    steps: std.ArrayList(Step),

    pub fn init(mc: *MulticastBuilder) MulticastBranchBuilder {
        return .{
            .mc = mc,
            .steps = .empty,
        };
    }

    fn alloc(self: *MulticastBranchBuilder) std.mem.Allocator {
        return self.mc.alloc();
    }

    pub fn process(self: *MulticastBranchBuilder, p: Processor) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *MulticastBranchBuilder, pred: Predicate) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *MulticastBranchBuilder, ep: EndpointRef) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *MulticastBranchBuilder, uri: []const u8) !*MulticastBranchBuilder {
        const ep = try self.mc.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *MulticastBranchBuilder, value: []const u8) !*MulticastBranchBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *MulticastBranchBuilder, key: []const u8, value: []const u8) !*MulticastBranchBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *MulticastBranchBuilder, key: []const u8) !*MulticastBranchBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn enrich(self: *MulticastBranchBuilder, ep: EndpointRef, merge: ?Processor) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Enrich = .{ .endpoint = ep, .merge = merge } });
        return self;
    }

    pub fn enrichUri(self: *MulticastBranchBuilder, uri: []const u8, merge: ?Processor) !*MulticastBranchBuilder {
        const ep = try self.mc.parent.registry.resolve(self.alloc(), uri);
        return try self.enrich(.{ .endpoint = ep }, merge);
    }

    pub fn marshal(self: *MulticastBranchBuilder, format: DataFormat) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *MulticastBranchBuilder, format: DataFormat) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *MulticastBranchBuilder, key: []const u8) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *MulticastBranchBuilder, key: []const u8) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *MulticastBranchBuilder, ms: u32) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *MulticastBranchBuilder, message: []const u8) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *MulticastBranchBuilder, level: LogLevel, message: []const u8) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *MulticastBranchBuilder, header: []const u8) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *MulticastBranchBuilder, p: Processor) !*MulticastBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endBranch(self: *MulticastBranchBuilder) !*MulticastBuilder {
        const rid = try self.mc.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.mc.routes.append(self.alloc(), rid);
        return self.mc;
    }
};

pub const IdempotentBuilder = struct {
    parent: *RtBuilder,
    key_header: []const u8,
    skip_duplicate: bool = true,
    eager: bool = true,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, key_header: []const u8) IdempotentBuilder {
        return .{
            .parent = parent,
            .key_header = key_header,
            .steps = .empty,
        };
    }

    pub fn skipDuplicate(self: *IdempotentBuilder, value: bool) *IdempotentBuilder {
        self.skip_duplicate = value;
        return self;
    }

    fn alloc(self: *IdempotentBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *IdempotentBuilder, p: Processor) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *IdempotentBuilder, pred: Predicate) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *IdempotentBuilder, ep: EndpointRef) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *IdempotentBuilder, uri: []const u8) !*IdempotentBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *IdempotentBuilder, value: []const u8) !*IdempotentBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *IdempotentBuilder, key: []const u8, value: []const u8) !*IdempotentBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *IdempotentBuilder, key: []const u8) !*IdempotentBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn marshal(self: *IdempotentBuilder, format: DataFormat) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Marshal = .{ .format = format } });
        return self;
    }

    pub fn unmarshal(self: *IdempotentBuilder, format: DataFormat) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Unmarshal = .{ .format = format } });
        return self;
    }

    pub fn claimCheckStore(self: *IdempotentBuilder, key: []const u8) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .store, .key = key } });
        return self;
    }

    pub fn claimCheckRetrieve(self: *IdempotentBuilder, key: []const u8) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .ClaimCheck = .{ .action = .retrieve, .key = key } });
        return self;
    }

    pub fn delay(self: *IdempotentBuilder, ms: u32) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *IdempotentBuilder, message: []const u8) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *IdempotentBuilder, level: LogLevel, message: []const u8) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *IdempotentBuilder, header: []const u8) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *IdempotentBuilder, p: Processor) !*IdempotentBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endIdempotent(self: *IdempotentBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .IdempotentConsumer = .{
            .key_header = self.key_header,
            .route = rid,
            .skip_duplicate = self.skip_duplicate,
            .eager = self.eager,
        } });
        return self.parent;
    }
};

pub const DoTryBuilder = struct {
    parent: *RtBuilder,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder) DoTryBuilder {
        return .{
            .parent = parent,
            .steps = .empty,
        };
    }

    fn alloc(self: *DoTryBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *DoTryBuilder, p: Processor) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *DoTryBuilder, pred: Predicate) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *DoTryBuilder, ep: EndpointRef) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *DoTryBuilder, uri: []const u8) !*DoTryBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *DoTryBuilder, value: []const u8) !*DoTryBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *DoTryBuilder, key: []const u8, value: []const u8) !*DoTryBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *DoTryBuilder, key: []const u8) !*DoTryBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn delay(self: *DoTryBuilder, ms: u32) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *DoTryBuilder, message: []const u8) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *DoTryBuilder, level: LogLevel, message: []const u8) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *DoTryBuilder, header: []const u8) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *DoTryBuilder, p: Processor) !*DoTryBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn doCatch(self: *DoTryBuilder) DoCatchBuilder {
        return DoCatchBuilder.init(self);
    }

    pub fn doFinally(self: *DoTryBuilder) DoFinallyBuilder {
        return DoFinallyBuilder.init(self, null);
    }

    pub fn endDoTry(self: *DoTryBuilder) !*RtBuilder {
        return self.finalize(null, null);
    }

    fn finalize(self: *DoTryBuilder, catch_steps: ?[]Step, finally_steps: ?[]Step) !*RtBuilder {
        const a = self.alloc();
        const try_rid = try self.parent.plan.addRoute(a, self.steps.items);
        var catch_rid: ?RouteId = null;
        var finally_rid: ?RouteId = null;
        if (catch_steps) |cs| catch_rid = try self.parent.plan.addRoute(a, cs);
        if (finally_steps) |fs| finally_rid = try self.parent.plan.addRoute(a, fs);
        try self.parent.cur_steps.append(a, .{ .DoTry = .{
            .try_route = try_rid,
            .catch_route = catch_rid,
            .finally_route = finally_rid,
        } });
        return self.parent;
    }
};

pub const DoCatchBuilder = struct {
    try_builder: *DoTryBuilder,
    steps: std.ArrayList(Step),

    pub fn init(try_builder: *DoTryBuilder) DoCatchBuilder {
        return .{
            .try_builder = try_builder,
            .steps = .empty,
        };
    }

    fn alloc(self: *DoCatchBuilder) std.mem.Allocator {
        return self.try_builder.alloc();
    }

    pub fn process(self: *DoCatchBuilder, p: Processor) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *DoCatchBuilder, pred: Predicate) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *DoCatchBuilder, ep: EndpointRef) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *DoCatchBuilder, uri: []const u8) !*DoCatchBuilder {
        const ep = try self.try_builder.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *DoCatchBuilder, value: []const u8) !*DoCatchBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *DoCatchBuilder, key: []const u8, value: []const u8) !*DoCatchBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *DoCatchBuilder, key: []const u8) !*DoCatchBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn delay(self: *DoCatchBuilder, ms: u32) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *DoCatchBuilder, message: []const u8) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *DoCatchBuilder, level: LogLevel, message: []const u8) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *DoCatchBuilder, header: []const u8) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *DoCatchBuilder, p: Processor) !*DoCatchBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn doFinally(self: *DoCatchBuilder) DoFinallyBuilder {
        return DoFinallyBuilder.init(self.try_builder, self.steps.items);
    }

    pub fn endDoTry(self: *DoCatchBuilder) !*RtBuilder {
        return self.try_builder.finalize(self.steps.items, null);
    }
};

pub const DoFinallyBuilder = struct {
    try_builder: *DoTryBuilder,
    catch_steps: ?[]Step,
    steps: std.ArrayList(Step),

    pub fn init(try_builder: *DoTryBuilder, catch_steps: ?[]Step) DoFinallyBuilder {
        return .{
            .try_builder = try_builder,
            .catch_steps = catch_steps,
            .steps = .empty,
        };
    }

    fn alloc(self: *DoFinallyBuilder) std.mem.Allocator {
        return self.try_builder.alloc();
    }

    pub fn process(self: *DoFinallyBuilder, p: Processor) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *DoFinallyBuilder, pred: Predicate) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *DoFinallyBuilder, ep: EndpointRef) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *DoFinallyBuilder, uri: []const u8) !*DoFinallyBuilder {
        const ep = try self.try_builder.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *DoFinallyBuilder, value: []const u8) !*DoFinallyBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *DoFinallyBuilder, key: []const u8, value: []const u8) !*DoFinallyBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *DoFinallyBuilder, key: []const u8) !*DoFinallyBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn delay(self: *DoFinallyBuilder, ms: u32) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *DoFinallyBuilder, message: []const u8) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *DoFinallyBuilder, level: LogLevel, message: []const u8) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn routingSlip(self: *DoFinallyBuilder, header: []const u8) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .RoutingSlip = .{ .header = header } });
        return self;
    }

    pub fn transform(self: *DoFinallyBuilder, p: Processor) !*DoFinallyBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endDoTry(self: *DoFinallyBuilder) !*RtBuilder {
        return self.try_builder.finalize(self.catch_steps, self.steps.items);
    }
};

pub const LoadBalancerBuilder = struct {
    parent: *RtBuilder,
    strategy: LoadBalancerStrategy,
    routes: std.ArrayList(RouteId),

    pub fn init(parent: *RtBuilder, strategy: LoadBalancerStrategy) LoadBalancerBuilder {
        return .{
            .parent = parent,
            .strategy = strategy,
            .routes = .empty,
        };
    }

    fn alloc(self: *LoadBalancerBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn branch(self: *LoadBalancerBuilder) LoadBalancerBranchBuilder {
        return LoadBalancerBranchBuilder.init(self);
    }

    pub fn endLoadBalancer(self: *LoadBalancerBuilder) !*RtBuilder {
        try self.parent.cur_steps.append(self.alloc(), .{ .LoadBalancer = .{
            .strategy = self.strategy,
            .routes = self.routes.items,
        } });
        return self.parent;
    }
};

pub const LoadBalancerBranchBuilder = struct {
    lb: *LoadBalancerBuilder,
    steps: std.ArrayList(Step),

    pub fn init(lb: *LoadBalancerBuilder) LoadBalancerBranchBuilder {
        return .{
            .lb = lb,
            .steps = .empty,
        };
    }

    fn alloc(self: *LoadBalancerBranchBuilder) std.mem.Allocator {
        return self.lb.alloc();
    }

    pub fn process(self: *LoadBalancerBranchBuilder, p: Processor) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *LoadBalancerBranchBuilder, pred: Predicate) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *LoadBalancerBranchBuilder, ep: EndpointRef) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *LoadBalancerBranchBuilder, uri: []const u8) !*LoadBalancerBranchBuilder {
        const ep = try self.lb.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *LoadBalancerBranchBuilder, value: []const u8) !*LoadBalancerBranchBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *LoadBalancerBranchBuilder, key: []const u8, value: []const u8) !*LoadBalancerBranchBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn removeHeader(self: *LoadBalancerBranchBuilder, key: []const u8) !*LoadBalancerBranchBuilder {
        return try self.process(try processors.removeHeader(self.alloc(), key));
    }

    pub fn delay(self: *LoadBalancerBranchBuilder, ms: u32) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn log(self: *LoadBalancerBranchBuilder, message: []const u8) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn logAt(self: *LoadBalancerBranchBuilder, level: LogLevel, message: []const u8) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message, .level = level } });
        return self;
    }

    pub fn transform(self: *LoadBalancerBranchBuilder, p: Processor) !*LoadBalancerBranchBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn endBranch(self: *LoadBalancerBranchBuilder) !*LoadBalancerBuilder {
        const rid = try self.lb.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.lb.routes.append(self.alloc(), rid);
        return self.lb;
    }
};

pub const BatchBuilder = struct {
    parent: *RtBuilder,
    size: u32,
    separator: []const u8,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, size: u32, separator: []const u8) BatchBuilder {
        return .{
            .parent = parent,
            .size = size,
            .separator = separator,
            .steps = .empty,
        };
    }

    fn alloc(self: *BatchBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *BatchBuilder, p: Processor) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *BatchBuilder, pred: Predicate) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *BatchBuilder, ep: EndpointRef) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *BatchBuilder, uri: []const u8) !*BatchBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *BatchBuilder, value: []const u8) !*BatchBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *BatchBuilder, key: []const u8, value: []const u8) !*BatchBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn transform(self: *BatchBuilder, p: Processor) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn log(self: *BatchBuilder, message: []const u8) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn delay(self: *BatchBuilder, ms: u32) !*BatchBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn endBatch(self: *BatchBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Batch = .{
            .size = self.size,
            .separator = self.separator,
            .route = rid,
        } });
        return self.parent;
    }
};

pub const ResequencerBuilder = struct {
    parent: *RtBuilder,
    size: u32,
    header: []const u8,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, size: u32, header: []const u8) ResequencerBuilder {
        return .{
            .parent = parent,
            .size = size,
            .header = header,
            .steps = .empty,
        };
    }

    fn alloc(self: *ResequencerBuilder) std.mem.Allocator {
        return self.parent.arena.allocator();
    }

    pub fn process(self: *ResequencerBuilder, p: Processor) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *ResequencerBuilder, pred: Predicate) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .Filter = pred });
        return self;
    }

    pub fn to(self: *ResequencerBuilder, ep: EndpointRef) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *ResequencerBuilder, uri: []const u8) !*ResequencerBuilder {
        const ep = try self.parent.registry.resolve(self.alloc(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn setBody(self: *ResequencerBuilder, value: []const u8) !*ResequencerBuilder {
        return try self.process(try processors.setBody(self.alloc(), value));
    }

    pub fn setHeader(self: *ResequencerBuilder, key: []const u8, value: []const u8) !*ResequencerBuilder {
        return try self.process(try processors.setHeader(self.alloc(), key, value));
    }

    pub fn transform(self: *ResequencerBuilder, p: Processor) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .Transform = p });
        return self;
    }

    pub fn log(self: *ResequencerBuilder, message: []const u8) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .Log = .{ .message = message } });
        return self;
    }

    pub fn delay(self: *ResequencerBuilder, ms: u32) !*ResequencerBuilder {
        try self.steps.append(self.alloc(), .{ .Delay = .{ .ms = ms } });
        return self;
    }

    pub fn endResequencer(self: *ResequencerBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Resequencer = .{
            .size = self.size,
            .header = self.header,
            .route = rid,
        } });
        return self.parent;
    }
};
