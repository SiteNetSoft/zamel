const std = @import("std");
const Step = @import("step.zig").Step;
const ChoiceBranch = @import("step.zig").ChoiceBranch;
const RoutePlan = @import("plan.zig").RoutePlan;
const RouteId = @import("step.zig").RouteId;

const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
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

    pub fn endOtherwise(self: *OtherwiseBuilder) !*ChoiceBuilder {
        const rid = try self.choice.parent.plan.addRoute(
            self.choice.parent.arena.allocator(),
            self.steps.items,
        );
        self.choice.otherwise_route = rid;
        return self.choice;
    }
};
