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
