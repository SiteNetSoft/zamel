const std = @import("std");
const zamel = @import("lib.zig");
const Exchange = zamel.Exchange;
const Services = zamel.Services;
const Registry = zamel.Registry;
const RtBuilder = zamel.RtBuilder;
const Processor = zamel.Processor;
const Predicate = zamel.Predicate;
const SyncExecutor = zamel.SyncExecutor;
const RoutePlan = zamel.RoutePlan;
const Step = zamel.Step;

fn testClock(_: ?*anyopaque) u64 {
    return 0;
}

fn testServices(alloc: std.mem.Allocator) Services {
    return .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
    };
}

// -------- helpers --------

var capture_buf: [1024]u8 = undefined;
var capture_len: usize = 0;

fn captureProcessor(_: ?*anyopaque, ex: *Exchange) !void {
    @memcpy(capture_buf[0..ex.body.len], ex.body);
    capture_len = ex.body.len;
}

fn captured() []const u8 {
    return capture_buf[0..capture_len];
}

fn uppercaseProcessor(_: ?*anyopaque, ex: *Exchange) !void {
    const old = ex.body;
    const new = try ex.allocator.alloc(u8, old.len);
    for (old, 0..) |ch, i| {
        new[i] = if (ch >= 'a' and ch <= 'z') ch - 32 else ch;
    }
    if (old.len != 0) ex.allocator.free(old);
    ex.body = new;
}

// -------- integration tests --------

test "builder: process then log endpoint" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.process(Processor.fromFn(uppercaseProcessor, null))).toUri("log:info")).build();

    // Verify plan was built correctly (2 steps: Process + To)
    const route = r.plan.route(0);
    try std.testing.expectEqual(@as(usize, 2), route.steps.len);
}

test "builder: choice with when and otherwise" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var route = try r.process(Processor.fromFn(captureProcessor, null));

    var c = route.choice();
    var w1 = c.when(try zamel.pred.headerEq(r.arena.allocator(), "type", "A"));
    _ = try (try w1.toUri("log:info")).endWhen();

    var ow = c.otherwise();
    _ = try (try ow.toUri("log:warn")).endOtherwise();

    _ = try (try c.end()).build();

    // Verify plan structure: main route has 2 steps (Process + Choice)
    const main_route = r.plan.route(2); // 0=when, 1=otherwise, 2=main
    try std.testing.expectEqual(@as(usize, 2), main_route.steps.len);
}

test "builder + executor: full pipeline process → filter → to" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;
    _ = try (try (try r.process(Processor.fromFn(uppercaseProcessor, null))).process(
        Processor.fromFn(captureProcessor, null),
    )).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello world");

    var exec = SyncExecutor.init(services);
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO WORLD", captured());
}

test "builder + executor: choice routes correctly" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var route = &r;
    var c = route.choice();

    // when header type=A: uppercase
    var w1 = c.when(try zamel.pred.headerEq(r.arena.allocator(), "type", "A"));
    _ = try (try w1.process(Processor.fromFn(uppercaseProcessor, null))).endWhen();

    // otherwise: capture as-is
    var ow = c.otherwise();
    _ = try (try ow.process(Processor.fromFn(captureProcessor, null))).endOtherwise();

    const rid = try (try c.end()).build();

    // Test with type=A
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello");
        try ex.putHeader("type", "A");

        var exec = SyncExecutor.init(services);
        try exec.run(&r.plan, rid, &ex);

        // Body should be uppercased
        try std.testing.expectEqualStrings("HELLO", ex.body);
    }

    // Test with type=B (hits otherwise)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello");
        try ex.putHeader("type", "B");

        var exec = SyncExecutor.init(services);
        try exec.run(&r.plan, rid, &ex);

        // Capture should have the body
        try std.testing.expectEqualStrings("hello", captured());
    }
}

test "builder + executor: retry wraps inner steps" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var pol = r.retry(3, 0);
    _ = try (try pol.process(Processor.fromFn(uppercaseProcessor, null))).endPolicy();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("TEST", ex.body);
}

test "builder + executor: split processes each line" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    // Split by newline, uppercase each part
    var sp = r.splitLines();
    _ = try (try sp.process(Processor.fromFn(captureProcessor, null))).endSplit();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa\nbbb\nccc");

    var exec = SyncExecutor.init(services);
    try exec.run(&r.plan, rid, &ex);

    // Last captured part should be "ccc"
    try std.testing.expectEqualStrings("ccc", captured());
}

test "builder + executor: aggregate transforms and rejoins" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var agg = r.aggregate(",");
    _ = try (try agg.process(Processor.fromFn(uppercaseProcessor, null))).endAggregate();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("foo,bar,baz");

    var exec = SyncExecutor.init(services);
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("FOO,BAR,BAZ", ex.body);
}

test "registry resolves log endpoint" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const ep = try registry.resolve(arena.allocator(), "log:info");
    const producer = try ep.createProducer(alloc);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test message");

    // Should not error (just logs)
    try producer.send(&ex);
}

test "registry returns error for unknown scheme" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const result = registry.resolve(alloc, "kafka:topic");
    try std.testing.expectError(error.UnknownEndpointScheme, result);
}

test "registry resolves file endpoint" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    _ = try registry.resolve(arena.allocator(), "file:/tmp/zamel_test_out.txt");
}

test "file endpoint write and read roundtrip" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const path = "/tmp/zamel_integration_test.txt";

    const ep = try registry.resolve(arena.allocator(), "file:" ++ path);
    const producer = try ep.createProducer(alloc);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello from zamel");

    try producer.send(&ex);

    // Verify by reading back via posix
    const posix = std.posix;
    const fd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0);
    defer posix.close(fd);

    var buf: [256]u8 = undefined;
    const rc = posix.system.read(fd, &buf, buf.len);
    const content = buf[0..rc];

    try std.testing.expectEqualStrings("hello from zamel", content);

    // Cleanup
    _ = posix.system.unlink(path);
}

test "file endpoint append mode" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const path = "/tmp/zamel_append_test.txt";

    const ep_write = try registry.resolve(arena.allocator(), "file:" ++ path);
    const prod_write = try ep_write.createProducer(alloc);

    var ex1 = Exchange.init(alloc);
    defer ex1.deinit();
    try ex1.setBody("line1");
    try prod_write.send(&ex1);

    const ep_append = try registry.resolve(arena.allocator(), "file:" ++ path ++ "?append=true");
    const prod_append = try ep_append.createProducer(alloc);

    var ex2 = Exchange.init(alloc);
    defer ex2.deinit();
    try ex2.setBody("line2");
    try prod_append.send(&ex2);

    // Read back
    const posix = std.posix;
    const fd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0);
    defer posix.close(fd);

    var buf: [256]u8 = undefined;
    const rc = posix.system.read(fd, &buf, buf.len);
    const content = buf[0..rc];

    try std.testing.expectEqualStrings("line1line2\n", content);

    // Cleanup
    _ = posix.system.unlink(path);
}
