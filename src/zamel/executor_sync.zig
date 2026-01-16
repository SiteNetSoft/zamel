const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Services = @import("services.zig").Services;
const RoutePlan = @import("plan.zig").RoutePlan;
const Step = @import("step.zig").Step;
const RouteId = @import("step.zig").RouteId;

pub const SyncExecutor = struct {
    services: Services,

    pub fn init(services: Services) SyncExecutor {
        return .{ .services = services };
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

                .Split => {
                    return error.NotImplemented;
                },

                .Aggregate => {
                    return error.NotImplemented;
                },

                .Policy => {
                    return error.NotImplemented;
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
