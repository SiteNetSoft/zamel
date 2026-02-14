const std = @import("std");
const zamel = @import("zamel/lib.zig");

fn nowMillis(_: ?*anyopaque) u64 {
    // TODO: use a real clock if I need it later
    return 0;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const services = zamel.Services{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = nowMillis, .ctx = null },
        .scheduler = null,
        .store = null,
    };

    var registry = zamel.Registry.init(services);
    defer registry.deinit();

    var r = zamel.RtBuilder.init(alloc, &registry);
    defer r.deinit();

    // In-line choice usage:
    var route = try r.fromUri("timer:500?repeat=10");
    var c2 = route.choice();

    // branch 1
    var w1 = c2.when(zamel.pred.headerEq(r.arena.allocator(), "type", "A"));
    _ = try (try w1.toUri("log:info")).endWhen();

    // branch 2
    var w2 = c2.when(zamel.pred.bodyContains(r.arena.allocator(), "tick 3"));
    _ = try (try w2.toUri("log:debug")).endWhen();

    // otherwise
    var ow = c2.otherwise();
    _ = try (try ow.toUri("log:warn")).endOtherwise();

    _ = try c2.end();

    _ = try route.toUri("log:info");
    try route.start();
}

