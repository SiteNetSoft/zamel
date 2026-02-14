const std = @import("std");
const Step = @import("step.zig").Step;

pub const Route = struct {
    steps: []Step,
};

pub const RoutePlan = struct {
    routes: std.ArrayList(Route),

    pub fn init() RoutePlan {
        return .{
            .routes = .empty,
        };
    }

    pub fn deinit(self: *RoutePlan, allocator: std.mem.Allocator) void {
        self.routes.deinit(allocator);
    }

    /// Adds a route built from a slice of steps, returns its RouteId (index)
    pub fn addRoute(self: *RoutePlan, allocator: std.mem.Allocator, steps: []Step) !u32 {
        const id: u32 = @intCast(self.routes.items.len);
        try self.routes.append(allocator, .{ .steps = steps });
        return id;
    }

    /// Get a route by id
    pub fn route(self: *const RoutePlan, id: u32) *const Route {
        return &self.routes.items[id];
    }
};

// -------- tests --------

const Processor = @import("processor.zig").Processor;

fn noopProcessor(_: ?*anyopaque, _: *@import("exchange.zig").Exchange) !void {}

test "addRoute returns sequential ids" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps1 = [_]Step{.{ .Process = Processor.fromFn(noopProcessor, null) }};
    const id0 = try plan.addRoute(alloc, &steps1);
    try std.testing.expectEqual(@as(u32, 0), id0);

    var steps2 = [_]Step{.{ .Process = Processor.fromFn(noopProcessor, null) }};
    const id1 = try plan.addRoute(alloc, &steps2);
    try std.testing.expectEqual(@as(u32, 1), id1);
}

test "route retrieval by id" {
    const alloc = std.testing.allocator;
    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(noopProcessor, null) }};
    const id = try plan.addRoute(alloc, &steps);
    const r = plan.route(id);
    try std.testing.expectEqual(@as(usize, 1), r.steps.len);
}
