const std = @import("std");
const Step = @import("step.zig").Step;
const ChoiceBranch = @import("step.zig").ChoiceBranch;
const PolicyKind = @import("step.zig").PolicyKind;
const RoutePlan = @import("plan.zig").RoutePlan;
const RouteId = @import("step.zig").RouteId;

const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const Endpoint = @import("endpoint.zig").Endpoint;
const EndpointRef = @import("endpoint.zig").EndpointRef;

const Registry = @import("registry.zig").Registry;

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

    // ---- split / aggregate DSL ----

    pub fn split(self: *RtBuilder, delimiter: u8) SplitBuilder {
        return SplitBuilder.init(self, delimiter);
    }

    pub fn splitLines(self: *RtBuilder) SplitBuilder {
        return SplitBuilder.init(self, '\n');
    }

    pub fn aggregate(self: *RtBuilder, separator: []const u8) AggregateBuilder {
        return AggregateBuilder.init(self, separator);
    }

    // ---- policy DSL ----

    pub fn retry(self: *RtBuilder, max: u32, backoff_ms: u32) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .Retry = .{ .max = max, .backoff_ms = backoff_ms } });
    }

    pub fn timeout(self: *RtBuilder, ms: u32) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .Timeout = .{ .ms = ms } });
    }

    pub fn deadLetter(self: *RtBuilder, ep: Endpoint) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .DeadLetter = .{ .endpoint = .{ .endpoint = ep } } });
    }

    pub fn deadLetterUri(self: *RtBuilder, uri: []const u8) !PolicyBuilder {
        const ep = try self.registry.resolve(self.arena.allocator(), uri);
        return PolicyBuilder.init(self, .{ .DeadLetter = .{ .endpoint = .{ .endpoint = ep } } });
    }

    pub fn circuitBreaker(self: *RtBuilder, failure_threshold: u32, reset_ms: u32) PolicyBuilder {
        return PolicyBuilder.init(self, .{ .CircuitBreaker = .{ .failure_threshold = failure_threshold, .reset_ms = reset_ms } });
    }

    // ---- finalize ----

    pub fn build(self: *RtBuilder) !RouteId {
        const steps_slice = self.cur_steps.items;
        return try self.plan.addRoute(self.arena.allocator(), steps_slice);
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

    pub fn when(self: *ChoiceBuilder, pred: Predicate) WhenBuilder {
        return WhenBuilder.init(self, pred);
    }

    pub fn otherwise(self: *ChoiceBuilder) OtherwiseBuilder {
        return OtherwiseBuilder.init(self);
    }

    pub fn end(self: *ChoiceBuilder) !*RtBuilder {
        const branches_slice = self.branches.items;
        try self.parent.cur_steps.append(self.parent.arena.allocator(), .{ .Choice = .{
            .branches = branches_slice,
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

    pub fn to(self: *WhenBuilder, ep: EndpointRef) !*WhenBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *WhenBuilder, uri: []const u8) !*WhenBuilder {
        const ep = try self.choice.parent.registry.resolve(self.choice.parent.arena.allocator(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn filter(self: *WhenBuilder, pred: Predicate) !*WhenBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .Filter = pred });
        return self;
    }

    pub fn process(self: *WhenBuilder, p: Processor) !*WhenBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .Process = p });
        return self;
    }

    pub fn endWhen(self: *WhenBuilder) !*ChoiceBuilder {
        const rid = try self.choice.parent.plan.addRoute(
            self.choice.parent.arena.allocator(),
            self.steps.items,
        );
        try self.choice.branches.append(
            self.choice.parent.arena.allocator(),
            .{ .when = self.pred, .route = rid },
        );
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

    pub fn to(self: *OtherwiseBuilder, ep: EndpointRef) !*OtherwiseBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .To = ep });
        return self;
    }

    pub fn toUri(self: *OtherwiseBuilder, uri: []const u8) !*OtherwiseBuilder {
        const ep = try self.choice.parent.registry.resolve(self.choice.parent.arena.allocator(), uri);
        return try self.to(.{ .endpoint = ep });
    }

    pub fn process(self: *OtherwiseBuilder, p: Processor) !*OtherwiseBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .Process = p });
        return self;
    }

    pub fn filter(self: *OtherwiseBuilder, pred: Predicate) !*OtherwiseBuilder {
        try self.steps.append(self.choice.parent.arena.allocator(), .{ .Filter = pred });
        return self;
    }

    pub fn endOtherwise(self: *OtherwiseBuilder) !*ChoiceBuilder {
        const rid = try self.choice.parent.plan.addRoute(
            self.choice.parent.arena.allocator(),
            self.steps.items,
        );
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
    delimiter: u8,
    steps: std.ArrayList(Step),

    pub fn init(parent: *RtBuilder, delimiter: u8) SplitBuilder {
        return .{
            .parent = parent,
            .delimiter = delimiter,
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

    pub fn endSplit(self: *SplitBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Split = .{
            .delimiter = self.delimiter,
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

    pub fn endAggregate(self: *AggregateBuilder) !*RtBuilder {
        const rid = try self.parent.plan.addRoute(self.alloc(), self.steps.items);
        try self.parent.cur_steps.append(self.alloc(), .{ .Aggregate = .{
            .separator = self.separator,
            .route = rid,
        } });
        return self.parent;
    }
};
