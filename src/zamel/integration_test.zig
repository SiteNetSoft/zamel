const std = @import("std");
const zamel = @import("lib.zig");
const Exchange = zamel.Exchange;
const Services = zamel.Services;
const Registry = zamel.Registry;
const RtBuilder = zamel.RtBuilder;
const Processor = zamel.Processor;
const Predicate = zamel.Predicate;
const Splitter = zamel.Splitter;
const RecipientResolver = zamel.RecipientResolver;
const EndpointRef = zamel.EndpointRef;
const SyncExecutor = zamel.SyncExecutor;
const RoutePlan = zamel.RoutePlan;
const Step = zamel.Step;
const PolicyKind = @import("step.zig").PolicyKind;
const ChoiceBranch = @import("step.zig").ChoiceBranch;

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
    defer exec.deinit();
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
        defer exec.deinit();
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
        defer exec.deinit();
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

    var pol = try r.retry(3, 0);
    _ = try (try pol.process(Processor.fromFn(uppercaseProcessor, null))).endPolicy();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
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
    defer exec.deinit();
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
    defer exec.deinit();
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

// -------- additional helpers --------

var mock_clock_val: u64 = 0;

fn mockClock(_: ?*anyopaque) u64 {
    const v = mock_clock_val;
    mock_clock_val += 500;
    return v;
}

fn mockServices(alloc: std.mem.Allocator) Services {
    return .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = mockClock, .ctx = null },
    };
}

fn alwaysFailProcessor(_: ?*anyopaque, _: *Exchange) !void {
    return error.ProcessorFailed;
}

var dl_captured_body: [256]u8 = undefined;
var dl_captured_len: usize = 0;

fn dlCaptureSend(_: ?*anyopaque, ex: *Exchange) !void {
    @memcpy(dl_captured_body[0..ex.body.len], ex.body);
    dl_captured_len = ex.body.len;
}

fn dlCaptureCreateProducer(_: ?*anyopaque, _: std.mem.Allocator) !zamel.Producer {
    return .{ .ctx = null, .sendFn = dlCaptureSend };
}

// -------- timeout policy tests --------

test "timeout policy fires when execution exceeds limit" {
    const alloc = std.testing.allocator;
    mock_clock_val = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Inner route: just a no-op processor
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(captureProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    // Main route: timeout of 100ms (clock advances 500ms per call, so elapsed=500 > 100)
    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Timeout = .{ .ms = 100 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("data");

    var exec = SyncExecutor.init(mockServices(alloc));
    defer exec.deinit();
    try std.testing.expectError(error.TimeoutExceeded, exec.run(&plan, main_rid, &ex));
}

test "timeout policy passes when within limit" {
    const alloc = std.testing.allocator;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    // Timeout of 99999ms with clock that always returns 0 => elapsed=0 < 99999
    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .Timeout = .{ .ms = 99999 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(testServices(alloc));
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// -------- dead letter policy tests --------

test "dead letter catches failure and sends to endpoint" {
    const alloc = std.testing.allocator;
    dl_captured_len = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(alwaysFailProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    const dl_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .DeadLetter = .{ .endpoint = .{ .endpoint = dl_ep } } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("failed msg");

    var exec = SyncExecutor.init(testServices(alloc));
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Dead letter endpoint should have received the exchange body
    try std.testing.expectEqualStrings("failed msg", dl_captured_body[0..dl_captured_len]);
}

test "dead letter does not fire on success" {
    const alloc = std.testing.allocator;
    dl_captured_len = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    const dl_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .DeadLetter = .{ .endpoint = .{ .endpoint = dl_ep } } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("good msg");

    var exec = SyncExecutor.init(testServices(alloc));
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Dead letter should NOT have been called
    try std.testing.expectEqual(@as(usize, 0), dl_captured_len);
    // Body should be uppercased normally
    try std.testing.expectEqualStrings("GOOD MSG", ex.body);
}

// -------- circuit breaker policy tests --------

test "circuit breaker opens after threshold failures" {
    const alloc = std.testing.allocator;
    mock_clock_val = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(alwaysFailProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .CircuitBreaker = .{ .failure_threshold = 2, .reset_ms = 50000 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var exec = SyncExecutor.init(mockServices(alloc));
    defer exec.deinit();

    // First failure: failures=1
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }

    // Second failure: failures=2, circuit opens
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }

    // Third call: circuit is open, should get CircuitOpen without running inner route
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.CircuitOpen, exec.run(&plan, main_rid, &ex));
    }
}

// -------- multicast tests --------

test "builder + executor: multicast runs all branches" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    // Multicast: branch1 uppercases, branch2 captures
    var mc = r.multicast();

    var b1 = mc.branch();
    _ = try (try b1.process(Processor.fromFn(uppercaseProcessor, null))).endBranch();

    var b2 = mc.branch();
    _ = try (try b2.process(Processor.fromFn(captureProcessor, null))).endBranch();

    const rid = try (try mc.endMulticast()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    // Branch 1 uppercased the body, branch 2 captured it
    try std.testing.expectEqualStrings("HELLO", captured());
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// -------- custom splitter tests --------

fn csvSplitter(_: ?*anyopaque, body: []const u8, out: *std.ArrayList([]const u8), allocator: std.mem.Allocator) !void {
    var it = std.mem.splitScalar(u8, body, ',');
    while (it.next()) |part| try out.append(allocator, part);
}

test "split with custom splitter function" {
    const alloc = std.testing.allocator;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(captureProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Split = .{
        .kind = .{ .custom = Splitter.fromFn(csvSplitter, null) },
        .route = sub_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    capture_len = 0;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("aaa,bbb,ccc");

    var exec = SyncExecutor.init(testServices(alloc));
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Last captured part should be "ccc"
    try std.testing.expectEqualStrings("ccc", captured());
}

test "split with sequence separator" {
    const alloc = std.testing.allocator;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(captureProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Split = .{
        .kind = .{ .sequence = "::" },
        .route = sub_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    capture_len = 0;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("one::two::three");

    var exec = SyncExecutor.init(testServices(alloc));
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    try std.testing.expectEqualStrings("three", captured());
}

test "builder: splitBy with custom splitter" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var sp = r.splitBy(Splitter.fromFn(csvSplitter, null));
    _ = try (try sp.process(Processor.fromFn(captureProcessor, null))).endSplit();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("x,y,z");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("z", captured());
}

// -------- custom endpoint registration tests --------

fn customEndpointFactory(_: std.mem.Allocator, _: []const u8) !zamel.Endpoint {
    return .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };
}

test "registry: custom endpoint registration" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    try registry.registerEndpoint("custom", customEndpointFactory);

    const ep = try registry.resolve(alloc, "custom:anything");
    const producer = try ep.createProducer(alloc);

    dl_captured_len = 0;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("custom data");

    try producer.send(&ex);

    try std.testing.expectEqualStrings("custom data", dl_captured_body[0..dl_captured_len]);
}

test "registry: custom endpoint overrides unknown scheme error" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Without registration, "myscheme" should fail
    try std.testing.expectError(error.UnknownEndpointScheme, registry.resolve(alloc, "myscheme:test"));

    // After registration, it should succeed
    try registry.registerEndpoint("myscheme", customEndpointFactory);
    _ = try registry.resolve(alloc, "myscheme:test");
}

// -------- builder: timeout and deadLetter via DSL --------

test "builder + executor: timeout via DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var pol = try r.timeout(99999);
    _ = try (try pol.process(Processor.fromFn(uppercaseProcessor, null))).endPolicy();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("TEST", ex.body);
}

test "builder + executor: deadLetter via DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    dl_captured_len = 0;

    const dl_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };

    var pol = r.deadLetter(dl_ep);
    _ = try (try pol.process(Processor.fromFn(alwaysFailProcessor, null))).endPolicy();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("oops");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("oops", dl_captured_body[0..dl_captured_len]);
}

test "builder + executor: circuitBreaker via DSL" {
    const alloc = std.testing.allocator;
    mock_clock_val = 0;
    const services = mockServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var pol = try r.circuitBreaker(1, 50000);
    _ = try (try pol.process(Processor.fromFn(alwaysFailProcessor, null))).endPolicy();

    const rid = try r.build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First call: fails, opens circuit (threshold=1)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&r.plan, rid, &ex));
    }

    // Second call: circuit is open
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.CircuitOpen, exec.run(&r.plan, rid, &ex));
    }
}

test "builder: splitSequence via DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var sp = r.splitSequence("::");
    _ = try (try sp.process(Processor.fromFn(captureProcessor, null))).endSplit();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("a::b::c");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("c", captured());
}

// -------- wire tap tests --------

test "wire tap sends copy without affecting main route" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    dl_captured_len = 0;
    capture_len = 0;

    const tap_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };

    // Wire tap then uppercase
    _ = try (try (try r.wireTap(.{ .endpoint = tap_ep })).process(
        Processor.fromFn(uppercaseProcessor, null),
    )).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    // Tap should have received original body
    try std.testing.expectEqualStrings("hello", dl_captured_body[0..dl_captured_len]);
    // Main route should have uppercased
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "wire tap error does not affect main route" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Tap endpoint that always fails
    const bad_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = struct {
            fn create(_: ?*anyopaque, _: std.mem.Allocator) !zamel.Producer {
                return .{ .ctx = null, .sendFn = struct {
                    fn send(_: ?*anyopaque, _: *Exchange) !void {
                        return error.TapFailed;
                    }
                }.send };
            }
        }.create,
    };

    var steps = [_]Step{
        .{ .WireTap = .{ .endpoint = bad_ep } },
        .{ .Process = Processor.fromFn(uppercaseProcessor, null) },
    };
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    // Main route should still succeed despite tap failure
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// -------- recipient list tests --------

fn testRecipientResolver(_: ?*anyopaque, _: *Exchange, out: *std.ArrayList(EndpointRef), allocator: std.mem.Allocator) !void {
    const ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };
    try out.append(allocator, .{ .endpoint = ep });
}

test "recipient list routes to dynamically resolved endpoints" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    dl_captured_len = 0;

    var steps = [_]Step{
        .{ .RecipientList = .{ .resolver = RecipientResolver.fromFn(testRecipientResolver, null) } },
    };
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("routed msg");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("routed msg", dl_captured_body[0..dl_captured_len]);
}

test "builder: recipientList via DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    dl_captured_len = 0;

    _ = try (try r.recipientList(RecipientResolver.fromFn(testRecipientResolver, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("dsl recipient");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("dsl recipient", dl_captured_body[0..dl_captured_len]);
}

// -------- throttle tests --------

test "throttle step with zero delay succeeds" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{
        .{ .Throttle = .{ .interval_ms = 0 } },
        .{ .Process = Processor.fromFn(uppercaseProcessor, null) },
    };
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("throttled");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("THROTTLED", ex.body);
}

test "builder: throttle via DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.throttle(0)).process(
        Processor.fromFn(uppercaseProcessor, null),
    )).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("TEST", ex.body);
}

// -------- policy validation tests --------

test "retry rejects max=0" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    try std.testing.expectError(error.InvalidPolicyConfig, r.retry(0, 100));
}

test "timeout rejects ms=0" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    try std.testing.expectError(error.InvalidPolicyConfig, r.timeout(0));
}

test "circuitBreaker rejects threshold=0" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    try std.testing.expectError(error.InvalidPolicyConfig, r.circuitBreaker(0, 1000));
}

// -------- predicate combinator integration tests --------

test "predNot with choice routing" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    const not_a = try zamel.pred.predNot(r.arena.allocator(), try zamel.pred.headerEq(r.arena.allocator(), "type", "A"));

    var c = r.choice();
    var w1 = c.when(not_a);
    _ = try (try w1.process(Processor.fromFn(captureProcessor, null))).endWhen();
    _ = try (try c.end()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("not-A message");
    try ex.putHeader("type", "B");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 1, &ex);

    try std.testing.expectEqualStrings("not-A message", captured());
}

test "predAnd in filter step" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    const has_type_a = try zamel.pred.headerEq(r.arena.allocator(), "type", "A");
    const has_hello = try zamel.pred.bodyContains(r.arena.allocator(), "hello");
    const both = try zamel.pred.predAnd(r.arena.allocator(), has_type_a, has_hello);

    _ = try (try (try r.filter(both)).process(
        Processor.fromFn(uppercaseProcessor, null),
    )).build();

    // Matches both predicates
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello world");
        try ex.putHeader("type", "A");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        try std.testing.expectEqualStrings("HELLO WORLD", ex.body);
    }

    // Fails one predicate — filter stops route
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello world");
        try ex.putHeader("type", "B");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, 0, &ex);

        // Body unchanged — processor never ran
        try std.testing.expectEqualStrings("hello world", ex.body);
    }
}
