const std = @import("std");
const Step = @import("step.zig").Step;
const ChoiceBranch = @import("step.zig").ChoiceBranch;
const PolicyKind = @import("step.zig").PolicyKind;
const SplitKind = @import("step.zig").SplitKind;
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

    // ---- idempotent DSL ----

    pub fn idempotent(self: *RtBuilder, key_header: []const u8) IdempotentBuilder {
        return IdempotentBuilder.init(self, key_header);
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
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, separator: []const u8) AggregateBuilder {
        return .{
            .parent = parent,
            .separator = separator,
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

    pub fn endAggregate(self: *AggregateBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Aggregate = .{
            .separator = self.separator,
            .route = rid,
        } });
        return self.parent;
    }
};

pub const MulticastBuilder = struct {
    parent: *RtBuilder,
    routes: std.ArrayList(RouteId),

    pub fn init(parent: *RtBuilder) MulticastBuilder {
        return .{
            .parent = parent,
            .routes = .empty,
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

    pub fn endBranch(self: *MulticastBranchBuilder) !*MulticastBuilder {
        const rid = try self.mc.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.mc.routes.append(self.alloc(), rid);
        return self.mc;
    }
};

pub const IdempotentBuilder = struct {
    parent: *RtBuilder,
    key_header: []const u8,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, key_header: []const u8) IdempotentBuilder {
        return .{
            .parent = parent,
            .key_header = key_header,
            .steps = .empty,
        };
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

    pub fn endIdempotent(self: *IdempotentBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .IdempotentConsumer = .{
            .key_header = self.key_header,
            .route = rid,
        } });
        return self.parent;
    }
};
