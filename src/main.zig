const std = @import("std");
const zamel = @import("zamel/lib.zig");

fn nowMillis(_: ?*anyopaque) u64 {
    return 0;
}

// -------- bean functions --------

fn greetBean(ex: *zamel.Exchange) !void {
    const name = ex.getHeader("name") orelse "World";
    const greeting = try std.fmt.allocPrint(ex.allocator, "Hello, {s}!", .{name});
    if (ex.body.len != 0) ex.allocator.free(ex.body);
    ex.body = greeting;
}

fn failBean(_: *zamel.Exchange) !void {
    return error.BeanFailed;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // ---- Setup InMemoryMetrics ----
    var mem_metrics = zamel.InMemoryMetrics.init(alloc);
    defer mem_metrics.deinit();

    // ---- Setup StateStore (for claim check) ----
    var store = zamel.InMemoryStore.init(alloc);
    defer store.deinit();

    const services = zamel.Services{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = nowMillis, .ctx = null },
        .store = store.stateStore(),
        .metrics = mem_metrics.metrics(),
    };

    var registry = zamel.Registry.init(services);
    defer registry.deinit();

    // ---- Register beans ----
    try registry.registerBean("greet", greetBean);
    try registry.registerBean("fail", failBean);

    // ---- Route 1: Bean endpoint + marshal ----
    std.log.info("=== Route 1: Bean + Marshal JSON ===", .{});
    {
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();

        _ = try (try (try (try r.setHeader("name", "Zamel")).toUri("bean:greet")).marshalJson()).build();

        var ex = zamel.Exchange.init(alloc);
        defer ex.deinit();

        var exec = zamel.SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        std.log.info("  Body after bean+marshal: {s}", .{ex.body});
    }

    // ---- Route 2: Unmarshal JSON ----
    std.log.info("=== Route 2: Unmarshal JSON ===", .{});
    {
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();

        _ = try (try r.unmarshalJson()).build();

        var ex = zamel.Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("{\"fruit\":\"apple\",\"color\":\"red\"}");

        var exec = zamel.SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        std.log.info("  fruit={s}, color={s}", .{
            ex.getHeader("fruit") orelse "?",
            ex.getHeader("color") orelse "?",
        });
    }

    // ---- Route 3: doTry/doCatch/doFinally ----
    std.log.info("=== Route 3: doTry/doCatch/doFinally ===", .{});
    {
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();

        var dt = r.doTry();
        _ = try dt.toUri("bean:fail");
        var dc = dt.doCatch();
        _ = try dc.setBody("error was caught!");
        var df = dc.doFinally();
        _ = try df.toUri("log:info");
        _ = try df.endDoTry();

        const rid = try r.build();

        var ex = zamel.Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("will-fail");

        var exec = zamel.SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        std.log.info("  Body after doTry: {s}", .{ex.body});
        std.log.info("  Exception: {s}", .{ex.getHeader("CamelExceptionMessage") orelse "none"});
    }

    // ---- Route 4: Claim check store/retrieve ----
    std.log.info("=== Route 4: Claim Check ===", .{});
    {
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();

        // Store body under key "payload-1", set body to something else, then retrieve
        _ = try (try (try (try r.claimCheckStore("payload-1")).setBody("temporary")).claimCheckRetrieve("payload-1")).build();

        var ex = zamel.Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("important payload");

        var exec = zamel.SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        std.log.info("  Body after claim check: {s}", .{ex.body});
    }

    // ---- Route 5: Direct route linking ----
    std.log.info("=== Route 5: Direct Route Linking ===", .{});
    {
        // Register a reusable route
        var r_upper = zamel.RtBuilder.init(alloc, &registry);
        defer r_upper.deinit();
        _ = try (try r_upper.process(zamel.Processor.fromFn(struct {
            fn call(_: ?*anyopaque, ex: *zamel.Exchange) !void {
                const old = ex.body;
                const new = try ex.allocator.alloc(u8, old.len);
                for (old, 0..) |ch, i| {
                    new[i] = if (ch >= 'a' and ch <= 'z') ch - 32 else ch;
                }
                if (old.len != 0) ex.allocator.free(old);
                ex.body = new;
            }
        }.call, null))).build();
        try registry.registerRoute("to-upper", &r_upper.plan, 0);

        // Main route sends to direct:to-upper
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();
        _ = try (try r.toUri("direct:to-upper")).build();

        var ex = zamel.Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello from direct");

        var exec = zamel.SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        std.log.info("  Body after direct: {s}", .{ex.body});
    }

    // ---- Route 6: Timer source with choice ----
    std.log.info("=== Route 6: Timer source (3 ticks) ===", .{});
    {
        var r = zamel.RtBuilder.init(alloc, &registry);
        defer r.deinit();

        var route = try r.fromUri("timer:100?repeat=3");
        _ = try route.toUri("log:info");
        try route.start();
    }

    // ---- Metrics report ----
    std.log.info("=== Metrics Report ===", .{});
    inline for (.{ "route.0.messages", "route.0.errors" }) |name| {
        if (mem_metrics.getCounter(name)) |v| {
            std.log.info("  {s} = {d}", .{ name, v });
        }
    }

    std.log.info("Demo complete.", .{});
}
