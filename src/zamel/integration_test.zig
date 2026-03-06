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
const InMemoryStore = zamel.InMemoryStore;
const http = @import("http.zig");

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

// ======== Processor Factories (Phase 1) ========

test "proc.setBody via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.setBody("replaced")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("original");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("replaced", ex.body);
}

test "proc.setHeader via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.setHeader("Content-Type", "text/plain")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("text/plain", ex.getHeader("Content-Type").?);
}

test "proc.removeHeader via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.removeHeader("temp")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.putHeader("temp", "value");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expect(ex.getHeader("temp") == null);
}

test "proc.transformBody via processor factory" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    const proc = try zamel.proc.transformBody(r.arena.allocator(), struct {
        fn transform(body: []const u8, a: std.mem.Allocator) ![]u8 {
            return try std.fmt.allocPrint(a, "[{s}]", .{body});
        }
    }.transform);
    _ = try (try r.process(proc)).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("[hello]", ex.body);
}

test "processor factories in nested WhenBuilder" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var c = r.choice();
    var w = c.when(try zamel.pred.headerEq(r.arena.allocator(), "type", "A"));
    _ = try (try (try w.setBody("when-body")).process(Processor.fromFn(captureProcessor, null))).endWhen();
    _ = try (try c.end()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.putHeader("type", "A");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 1, &ex);

    try std.testing.expectEqualStrings("when-body", captured());
}

// ======== Idempotent Consumer (Phase 2) ========

fn testServicesWithStore(alloc: std.mem.Allocator, store: *InMemoryStore) Services {
    return .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
        .store = store.stateStore(),
    };
}

test "idempotent consumer suppresses duplicates" {
    const alloc = std.testing.allocator;

    var store = InMemoryStore.init(alloc);
    defer store.deinit();

    const services = testServicesWithStore(alloc, &store);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var idem = r.idempotent("MessageId");
    _ = try (try idem.process(Processor.fromFn(captureProcessor, null))).endIdempotent();
    const rid = try r.build();

    // First message
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("first");
        try ex.putHeader("MessageId", "msg-001");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        try std.testing.expectEqualStrings("first", captured());
    }

    // Duplicate (same MessageId) — should NOT process
    capture_len = 0;
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("duplicate");
        try ex.putHeader("MessageId", "msg-001");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        // Capture should still be empty — processor was skipped
        try std.testing.expectEqual(@as(usize, 0), capture_len);
    }

    // Different MessageId — should process
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("second");
        try ex.putHeader("MessageId", "msg-002");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        try std.testing.expectEqualStrings("second", captured());
    }
}

test "idempotent consumer returns error when key header missing" {
    const alloc = std.testing.allocator;

    var store = InMemoryStore.init(alloc);
    defer store.deinit();

    const services = testServicesWithStore(alloc, &store);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(captureProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .IdempotentConsumer = .{ .key_header = "MissingHeader", .route = inner_rid } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.IdempotentKeyMissing, exec.run(&plan, main_rid, &ex));
}

test "idempotent consumer returns error when store missing" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc); // No store

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(captureProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .IdempotentConsumer = .{ .key_header = "Id", .route = inner_rid } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");
    try ex.putHeader("Id", "1");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.StateStoreRequired, exec.run(&plan, main_rid, &ex));
}

test "idempotent consumer via builder DSL" {
    const alloc = std.testing.allocator;

    var store = InMemoryStore.init(alloc);
    defer store.deinit();

    const services = testServicesWithStore(alloc, &store);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var idem = r.idempotent("Id");
    _ = try (try idem.process(Processor.fromFn(captureProcessor, null))).endIdempotent();
    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("data");
    try ex.putHeader("Id", "unique");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("data", captured());
}

// ======== Content Enricher (Phase 3) ========

fn enrichEndpointSend(_: ?*anyopaque, ex: *Exchange) !void {
    try ex.setBody("enriched-data");
}

fn enrichEndpointCreate(_: ?*anyopaque, _: std.mem.Allocator) !zamel.Producer {
    return .{ .ctx = null, .sendFn = enrichEndpointSend };
}

test "enrich replaces body with endpoint response" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    const enrich_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = enrichEndpointCreate,
    };

    var steps = [_]Step{.{ .Enrich = .{ .endpoint = .{ .endpoint = enrich_ep }, .merge = null } }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("original");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("enriched-data", ex.body);
}

fn mergeProcessor(_: ?*anyopaque, ex: *Exchange) !void {
    // Merge: combine original + enriched
    const original = ex.getHeader("CamelEnrichOriginalBody") orelse return;
    const enriched = ex.body;
    const merged = try std.fmt.allocPrint(ex.allocator, "{s}+{s}", .{ original, enriched });
    if (enriched.len != 0) ex.allocator.free(ex.body);
    ex.body = merged;
}

test "enrich with merge processor" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    const enrich_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = enrichEndpointCreate,
    };

    var steps = [_]Step{.{ .Enrich = .{
        .endpoint = .{ .endpoint = enrich_ep },
        .merge = Processor.fromFn(mergeProcessor, null),
    } }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("original");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    try std.testing.expectEqualStrings("original+enriched-data", ex.body);
}

test "enrich via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    const enrich_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = enrichEndpointCreate,
    };

    _ = try (try r.enrich(.{ .endpoint = enrich_ep }, null)).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("original");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("enriched-data", ex.body);
}

// ======== Direct Endpoint (Phase 4) ========

test "direct endpoint routes to named route" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Register a named route that uppercases
    var r1 = RtBuilder.init(alloc, &registry);
    defer r1.deinit();

    _ = try (try r1.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("uppercase", &r1.plan, 0);

    // Build main route that sends to direct:uppercase
    var r2 = RtBuilder.init(alloc, &registry);
    defer r2.deinit();

    _ = try (try r2.toUri("direct:uppercase")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r2.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "direct endpoint returns error for unknown route" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.toUri("direct:nonexistent")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.DirectRouteNotFound, exec.run(&r.plan, 0, &ex));
}

test "direct endpoint chaining" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Route A: uppercases
    var r_a = RtBuilder.init(alloc, &registry);
    defer r_a.deinit();
    _ = try (try r_a.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("step-a", &r_a.plan, 0);

    // Route B: calls step-a then captures
    var r_b = RtBuilder.init(alloc, &registry);
    defer r_b.deinit();
    _ = try (try (try r_b.toUri("direct:step-a")).process(Processor.fromFn(captureProcessor, null))).build();
    try registry.registerRoute("step-b", &r_b.plan, 0);

    // Main: calls step-b
    var r_main = RtBuilder.init(alloc, &registry);
    defer r_main.deinit();
    _ = try (try r_main.toUri("direct:step-b")).build();

    capture_len = 0;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("chain test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r_main.plan, 0, &ex);

    try std.testing.expectEqualStrings("CHAIN TEST", captured());
}

test "builder register method" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try r.process(Processor.fromFn(uppercaseProcessor, null));
    try r.register("my-route");

    // Route should be registered
    const named = registry.lookupRoute("my-route");
    try std.testing.expect(named != null);
}

// ======== SEDA Endpoint (Phase 5) ========

test "seda queue isolation between names" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Resolve two different seda endpoints
    const ep1 = try registry.resolve(alloc, "seda:queue1");
    const ep2 = try registry.resolve(alloc, "seda:queue2");

    // They should be different endpoints
    const prod1 = try ep1.createProducer(alloc);
    const prod2 = try ep2.createProducer(alloc);

    var ex1 = Exchange.init(alloc);
    defer ex1.deinit();
    try ex1.setBody("msg-q1");
    try prod1.send(&ex1);

    var ex2 = Exchange.init(alloc);
    defer ex2.deinit();
    try ex2.setBody("msg-q2");
    try prod2.send(&ex2);

    // Queue1 should have 1 message
    const q1 = registry.seda_queues.get("queue1").?;
    try std.testing.expectEqual(@as(usize, 1), q1.len());

    const q2 = registry.seda_queues.get("queue2").?;
    try std.testing.expectEqual(@as(usize, 1), q2.len());
}

test "seda enqueue and drain via endpoint" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const ep = try registry.resolve(alloc, "seda:test-queue");
    const prod = try ep.createProducer(alloc);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("seda-msg");
    try ex.putHeader("key", "val");

    try prod.send(&ex);

    const q = registry.seda_queues.get("test-queue").?;
    try std.testing.expectEqual(@as(usize, 1), q.len());

    const messages = try q.drain(alloc);
    defer alloc.free(messages);
    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqualStrings("seda-msg", messages[0].body);

    // Clean up drained messages
    alloc.free(messages[0].body);
    var hdr = messages[0].headers;
    var hit = hdr.iterator();
    while (hit.next()) |e| alloc.free(e.value_ptr.*);
    hdr.deinit();
}

// ======== HTTP Endpoint (Phase 6) ========

test "http endpoint resolution" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Should resolve without error
    const ep = try registry.resolve(alloc, "http://127.0.0.1:8080/api");
    _ = try ep.createProducer(alloc);
}

test "https endpoint resolution" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    const ep = try registry.resolve(alloc, "https://127.0.0.1:443/secure");
    _ = try ep.createProducer(alloc);
}

test "http URL parsing" {
    const p1 = http.parseHttpUrl("http://127.0.0.1:8080/api/test").?;
    try std.testing.expectEqualStrings("127.0.0.1", p1.host);
    try std.testing.expectEqual(@as(u16, 8080), p1.port);
    try std.testing.expectEqualStrings("/api/test", p1.path);

    const p2 = http.parseHttpUrl("https://10.0.0.1/").?;
    try std.testing.expectEqual(@as(u16, 443), p2.port);

    try std.testing.expect(http.parseHttpUrl("ftp://bad") == null);
}

// ======== Combined patterns ========

test "setBody + setHeader + enrich combined pipeline" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    const enrich_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = enrichEndpointCreate,
    };

    _ = try (try (try (try r.setHeader("stage", "prepared")).setBody("initial")).enrich(.{ .endpoint = enrich_ep }, null)).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    // After enrich, body should be enriched-data
    try std.testing.expectEqualStrings("enriched-data", ex.body);
    try std.testing.expectEqualStrings("prepared", ex.getHeader("stage").?);
}

test "idempotent + direct combined" {
    const alloc = std.testing.allocator;

    var store = InMemoryStore.init(alloc);
    defer store.deinit();

    const services = testServicesWithStore(alloc, &store);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Register a direct route that uppercases
    var r_direct = RtBuilder.init(alloc, &registry);
    defer r_direct.deinit();
    _ = try (try r_direct.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("upper", &r_direct.plan, 0);

    // Main route: idempotent then send to direct:upper
    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var idem = r.idempotent("Id");
    _ = try (try idem.toUri("direct:upper")).endIdempotent();
    const rid = try r.build();

    // First call
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello");
        try ex.putHeader("Id", "unique-1");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        try std.testing.expectEqualStrings("HELLO", ex.body);
    }

    // Duplicate — should be skipped (body unchanged)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello again");
        try ex.putHeader("Id", "unique-1");

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(&r.plan, rid, &ex);

        // Body stays as "hello again" — not uppercased
        try std.testing.expectEqualStrings("hello again", ex.body);
    }
}

// ======== DoTry/DoCatch/DoFinally (Error Handler DSL) ========

test "doTry catches error and runs catch route" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var dt = r.doTry();
    _ = try dt.process(Processor.fromFn(alwaysFailProcessor, null));
    var dc = dt.doCatch();
    _ = try dc.process(Processor.fromFn(captureProcessor, null));
    _ = try dc.endDoTry();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("try-data");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    // Catch route should have captured the body
    try std.testing.expectEqualStrings("try-data", captured());
    // Exception header should be set
    try std.testing.expect(ex.getHeader("CamelExceptionMessage") != null);
}

test "doTry runs finally on success" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var dt = r.doTry();
    _ = try dt.process(Processor.fromFn(uppercaseProcessor, null));
    var df = dt.doFinally();
    _ = try df.process(Processor.fromFn(captureProcessor, null));
    _ = try df.endDoTry();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    // Finally should have captured the uppercased body
    try std.testing.expectEqualStrings("HELLO", captured());
}

test "doTry runs catch then finally on error" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var dt = r.doTry();
    _ = try dt.process(Processor.fromFn(alwaysFailProcessor, null));
    var dc = dt.doCatch();
    _ = try dc.setBody("caught");
    var df = dc.doFinally();
    _ = try df.process(Processor.fromFn(captureProcessor, null));
    _ = try df.endDoTry();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("original");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    // Catch set body to "caught", finally captured it
    try std.testing.expectEqualStrings("caught", captured());
}

test "doTry without catch propagates no error when finally succeeds" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var dt = r.doTry();
    _ = try dt.process(Processor.fromFn(uppercaseProcessor, null));
    _ = try dt.endDoTry();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("TEST", ex.body);
}

// ======== Bean Endpoint ========

test "bean endpoint via registry" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    try registry.registerBean("myBean", struct {
        fn call(ex: *Exchange) !void {
            try ex.setBody("bean-output");
        }
    }.call);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.toUri("bean:myBean")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("input");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("bean-output", ex.body);
}

test "bean endpoint not found" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    try std.testing.expectError(error.BeanNotFound, registry.resolve(alloc, "bean:unknown"));
}

// ======== JSON Marshal/Unmarshal ========

test "marshal json via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.setHeader("color", "red")).marshalJson()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    // Body should contain JSON with the header
    try std.testing.expect(std.mem.indexOf(u8, ex.body, "\"color\":\"red\"") != null);
}

test "unmarshal json via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.unmarshalJson()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("{\"fruit\":\"apple\"}");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("apple", ex.getHeader("fruit").?);
}

test "marshal then unmarshal roundtrip via executor" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Route 1: marshal
    var r1 = RtBuilder.init(alloc, &registry);
    defer r1.deinit();
    _ = try (try r1.marshalJson()).build();

    // Route 2: unmarshal
    var r2 = RtBuilder.init(alloc, &registry);
    defer r2.deinit();
    _ = try (try r2.unmarshalJson()).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.putHeader("key", "value");

    // Marshal
    var exec1 = SyncExecutor.init(services);
    defer exec1.deinit();
    try exec1.run(&r1.plan, 0, &ex);

    // Body now has JSON
    try std.testing.expect(ex.body.len > 0);

    // Unmarshal into new exchange
    var ex2 = Exchange.init(alloc);
    defer ex2.deinit();
    try ex2.setBody(ex.body);

    var exec2 = SyncExecutor.init(services);
    defer exec2.deinit();
    try exec2.run(&r2.plan, 0, &ex2);

    try std.testing.expectEqualStrings("value", ex2.getHeader("key").?);
}

// ======== Claim Check ========

test "claim check store and retrieve" {
    const alloc = std.testing.allocator;

    var store = InMemoryStore.init(alloc);
    defer store.deinit();

    const services = testServicesWithStore(alloc, &store);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try (try r.claimCheckStore("claim-1")).setBody("replaced")).claimCheckRetrieve("claim-1")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("important-data");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    // Store saved "important-data", setBody replaced with "replaced",
    // then retrieve restored "important-data"
    try std.testing.expectEqualStrings("important-data", ex.body);
}

test "claim check store requires state store" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc); // No store

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .ClaimCheck = .{ .action = .store, .key = "k" } }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("data");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.StateStoreRequired, exec.run(&plan, rid, &ex));
}

// ======== Route Lifecycle ========

test "route lifecycle: stop prevents direct endpoint" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    // Register a route
    var r1 = RtBuilder.init(alloc, &registry);
    defer r1.deinit();
    _ = try (try r1.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("stoppable", &r1.plan, 0);

    // Stop the route
    try registry.stopRoute("stoppable");

    // Try to send to it
    var r2 = RtBuilder.init(alloc, &registry);
    defer r2.deinit();
    _ = try (try r2.toUri("direct:stoppable")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.RouteStopped, exec.run(&r2.plan, 0, &ex));
}

test "route lifecycle: suspend prevents direct endpoint" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r1 = RtBuilder.init(alloc, &registry);
    defer r1.deinit();
    _ = try (try r1.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("suspendable", &r1.plan, 0);

    try registry.suspendRoute("suspendable");

    var r2 = RtBuilder.init(alloc, &registry);
    defer r2.deinit();
    _ = try (try r2.toUri("direct:suspendable")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try std.testing.expectError(error.RouteSuspended, exec.run(&r2.plan, 0, &ex));
}

test "route lifecycle: resume after suspend" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r1 = RtBuilder.init(alloc, &registry);
    defer r1.deinit();
    _ = try (try r1.process(Processor.fromFn(uppercaseProcessor, null))).build();
    try registry.registerRoute("resumable", &r1.plan, 0);

    try registry.suspendRoute("resumable");
    try registry.resumeRoute("resumable");

    var r2 = RtBuilder.init(alloc, &registry);
    defer r2.deinit();
    _ = try (try r2.toUri("direct:resumable")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r2.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "route lifecycle: default state is started" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    try std.testing.expectEqual(zamel.RouteState.started, registry.getRouteState("any-route"));
}

// ======== Metrics ========

test "metrics increment on successful route" {
    const alloc = std.testing.allocator;

    var mem_metrics = zamel.InMemoryMetrics.init(alloc);
    defer mem_metrics.deinit();

    const services: Services = .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
        .metrics = mem_metrics.metrics(),
    };

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqual(@as(u64, 1), mem_metrics.getCounter("route.0.messages").?);
}

test "metrics increment errors on failure" {
    const alloc = std.testing.allocator;

    var mem_metrics = zamel.InMemoryMetrics.init(alloc);
    defer mem_metrics.deinit();

    const services: Services = .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = testClock, .ctx = null },
        .metrics = mem_metrics.metrics(),
    };

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(alwaysFailProcessor, null) }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    _ = exec.run(&plan, rid, &ex) catch {};

    try std.testing.expectEqual(@as(u64, 1), mem_metrics.getCounter("route.0.errors").?);
}

// ======== Bean + doTry combined ========

test "bean in doTry catch" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    try registry.registerBean("errorHandler", struct {
        fn call(ex: *Exchange) !void {
            try ex.setBody("handled");
        }
    }.call);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var dt = r.doTry();
    _ = try dt.process(Processor.fromFn(alwaysFailProcessor, null));
    var dc = dt.doCatch();
    _ = try dc.toUri("bean:errorHandler");
    _ = try dc.endDoTry();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("will-fail");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("handled", ex.body);
}

// ======== Delay Step ========

test "delay step passes message through" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.delay(0)).process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// ======== Log Step ========

test "log step does not error" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.log("test message")).process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "log step with interpolation runs without error" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.setHeader("user", "alice")).log("Processing ${body} for ${header.user}")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("order-123");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    // Body should be unchanged (log is side-effect only)
    try std.testing.expectEqualStrings("order-123", ex.body);
}

// ======== Message History ========

test "message history disabled by default" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expect(ex.getHeader("CamelMessageHistory") == null);
}

test "message history enabled tracks steps" {
    const alloc = std.testing.allocator;
    var services = testServices(alloc);
    services.message_history = true;

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.process(Processor.fromFn(uppercaseProcessor, null))).log("done")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    const history = ex.getHeader("CamelMessageHistory").?;
    try std.testing.expectEqualStrings("Process,Log", history);
}

// ======== Mock Endpoint ========

test "mock captures exchange body" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.toUri("mock:result")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("captured-data");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    const captures = mock_store.getCaptures("result").?;
    try std.testing.expectEqual(@as(usize, 1), captures.len);
    try std.testing.expectEqualStrings("captured-data", captures[0].body);
}

test "mock captures multiple exchanges" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.toUri("mock:multi")).build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("msg1");
        try exec.run(&r.plan, 0, &ex);
    }
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("msg2");
        try exec.run(&r.plan, 0, &ex);
    }

    try std.testing.expectEqual(@as(usize, 2), mock_store.captureCount("multi"));
    const captures = mock_store.getCaptures("multi").?;
    try std.testing.expectEqualStrings("msg1", captures[0].body);
    try std.testing.expectEqualStrings("msg2", captures[1].body);
}

test "mock captureCount works" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    // No captures yet
    try std.testing.expectEqual(@as(usize, 0), mock_store.captureCount("test"));

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.toUri("mock:test")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("data");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("test"));
}

// ======== Routing Slip ========

test "routing slip routes through multiple endpoints" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.routingSlip("slip")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("routed");
    try ex.putHeader("slip", "mock:dest1,mock:dest2");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &registry;
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("dest1"));
    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("dest2"));
}

test "routing slip with missing header is no-op" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.routingSlip("missing-header")).process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &registry;
    try exec.run(&r.plan, 0, &ex);

    // Routing slip was no-op but subsequent process step still ran
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// ======== Load Balancer ========

test "round-robin distributes across routes" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var lb = r.loadBalancer(.round_robin);
    var b1 = lb.branch();
    _ = try (try b1.toUri("mock:lb1")).endBranch();
    var b2 = lb.branch();
    _ = try (try b2.toUri("mock:lb2")).endBranch();
    const rid = try (try lb.endLoadBalancer()).build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First message goes to lb1
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("msg1");
        try exec.run(&r.plan, rid, &ex);
    }

    // Second message goes to lb2
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("msg2");
        try exec.run(&r.plan, rid, &ex);
    }

    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("lb1"));
    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("lb2"));
}

test "round-robin cycles back" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var lb = r.loadBalancer(.round_robin);
    var b1 = lb.branch();
    _ = try (try b1.toUri("mock:cyc1")).endBranch();
    var b2 = lb.branch();
    _ = try (try b2.toUri("mock:cyc2")).endBranch();
    const rid = try (try lb.endLoadBalancer()).build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // Send 3 messages: should go lb1, lb2, lb1
    for (0..3) |_| {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("msg");
        try exec.run(&r.plan, rid, &ex);
    }

    try std.testing.expectEqual(@as(usize, 2), mock_store.captureCount("cyc1"));
    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("cyc2"));
}

test "load balancer with single route always uses it" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    capture_len = 0;

    var lb = r.loadBalancer(.round_robin);
    var b1 = lb.branch();
    _ = try (try b1.process(Processor.fromFn(captureProcessor, null))).endBranch();
    const rid = try (try lb.endLoadBalancer()).build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    for (0..3) |_| {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("single");
        try exec.run(&r.plan, rid, &ex);
    }

    try std.testing.expectEqualStrings("single", captured());
}

// ======== Transform Step ========

test "transform modifies body" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.transform(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// ======== Dynamic Router ========

test "dynamic router routes by header value" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var mock_store = zamel.MockStore.init(alloc);
    defer mock_store.deinit();

    var registry = Registry.init(services);
    defer registry.deinit();
    registry.setMockStore(&mock_store);

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try r.dynamicRouter("destination")).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("routed-msg");
    try ex.putHeader("destination", "mock:dynamic-dest");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &registry;
    try exec.run(&r.plan, 0, &ex);

    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("dynamic-dest"));
}

test "dynamic router no-op when header missing" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    _ = try (try (try r.dynamicRouter("destination")).process(Processor.fromFn(uppercaseProcessor, null))).build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &registry;
    try exec.run(&r.plan, 0, &ex);

    // Dynamic router was no-op but subsequent process step still ran
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// ======== Circuit Breaker Half-Open ========

var fixed_clock_val: u64 = 0;

fn fixedClock(_: ?*anyopaque) u64 {
    return fixed_clock_val;
}

fn fixedClockServices(alloc: std.mem.Allocator) Services {
    return .{
        .allocator = alloc,
        .clock = .{ .nowMillisFn = fixedClock, .ctx = null },
    };
}

test "circuit breaker half-open probe" {
    const alloc = std.testing.allocator;
    fixed_clock_val = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(alwaysFailProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .CircuitBreaker = .{ .failure_threshold = 1, .reset_ms = 1000 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var exec = SyncExecutor.init(fixedClockServices(alloc));
    defer exec.deinit();

    // First failure at t=0: opens circuit, open_until_ms=1000
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }

    // Circuit is open at t=0 (0 < 1000)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.CircuitOpen, exec.run(&plan, main_rid, &ex));
    }

    // Advance clock past reset period -> half-open
    fixed_clock_val = 2000;
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        // Should allow probe in half-open, but still fails
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }

    // Should be back to open after half-open failure (open_until=2000+1000=3000)
    // Still at t=2000, so 2000 < 3000 → CircuitOpen
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.CircuitOpen, exec.run(&plan, main_rid, &ex));
    }
}

test "circuit breaker success threshold" {
    const alloc = std.testing.allocator;
    fixed_clock_val = 0;

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // A processor that fails based on global counter
    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(conditionalFailProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .CircuitBreaker = .{ .failure_threshold = 1, .reset_ms = 1000, .success_threshold = 2 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var exec = SyncExecutor.init(fixedClockServices(alloc));
    defer exec.deinit();

    // Fail to open circuit at t=0
    conditional_fail = true;
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }

    // Advance past reset
    fixed_clock_val = 2000;
    conditional_fail = false;

    // First success in half-open — not enough to close (need 2)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try exec.run(&plan, main_rid, &ex);
    }

    // Second success — should close circuit
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try exec.run(&plan, main_rid, &ex);
    }

    // Circuit should be closed now — can handle failures again normally
    conditional_fail = true;
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&plan, main_rid, &ex));
    }
}

var conditional_fail: bool = false;

fn conditionalFailProcessor(_: ?*anyopaque, _: *Exchange) !void {
    if (conditional_fail) return error.ProcessorFailed;
}

// ======== Dead Letter with Retry ========

test "dead letter with retry" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    dl_captured_len = 0;

    var inner_steps = [_]Step{.{ .Process = Processor.fromFn(alwaysFailProcessor, null) }};
    const inner_rid = try plan.addRoute(alloc, &inner_steps);

    const dl_ep: zamel.Endpoint = .{
        .ctx = null,
        .createProducerFn = dlCaptureCreateProducer,
    };

    var main_steps = [_]Step{.{ .Policy = .{
        .kind = .{ .DeadLetter = .{ .endpoint = .{ .endpoint = dl_ep }, .retries = 2, .retry_backoff_ms = 0 } },
        .route = inner_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("retry-then-dl");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // Should have sent to dead letter after retries exhausted
    try std.testing.expectEqualStrings("retry-then-dl", dl_captured_body[0..dl_captured_len]);
}

test "dead letter enriches error headers" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    dl_captured_len = 0;

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
    try ex.setBody("error-enriched");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, main_rid, &ex);

    // CamelExceptionMessage should be set
    try std.testing.expect(ex.getHeader("CamelExceptionMessage") != null);
    try std.testing.expectEqualStrings("ProcessorFailed", ex.getHeader("CamelExceptionMessage").?);
    try std.testing.expect(ex.getHeader("CamelExceptionTimestamp") != null);
    try std.testing.expect(ex.getHeader("CamelFailedRouteId") != null);
}

// ======== Dead Letter with Retry via Builder ========

test "deadLetterWithRetry builder DSL" {
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

    var pol = r.deadLetterWithRetry(dl_ep, 1, 0);
    _ = try (try pol.process(Processor.fromFn(alwaysFailProcessor, null))).endPolicy();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("builder-dl-retry");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("builder-dl-retry", dl_captured_body[0..dl_captured_len]);
}

// ======== Circuit Breaker Advanced via Builder ========

test "circuitBreakerAdvanced builder DSL" {
    const alloc = std.testing.allocator;
    fixed_clock_val = 0;
    const services = fixedClockServices(alloc);

    var registry = Registry.init(services);
    defer registry.deinit();

    var r = RtBuilder.init(alloc, &registry);
    defer r.deinit();

    var pol = try r.circuitBreakerAdvanced(1, 50000, 2);
    _ = try (try pol.process(Processor.fromFn(alwaysFailProcessor, null))).endPolicy();

    const rid = try r.build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // Should fail and open circuit at t=0
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.ProcessorFailed, exec.run(&r.plan, rid, &ex));
    }

    // Circuit should be open (0 < 50000)
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try std.testing.expectError(error.CircuitOpen, exec.run(&r.plan, rid, &ex));
    }
}

// -------- Feature: Parallel Multicast --------

test "parallel multicast executes all branches" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);
    var reg = Registry.init(services);
    defer reg.deinit();

    var r = RtBuilder.init(alloc, &reg);
    defer r.deinit();

    var mc = r.multicast();
    var b1 = mc.branch();
    _ = try b1.process(Processor.fromFn(uppercaseProcessor, null));
    const mc2 = try b1.endBranch();
    var b2 = mc2.branch();
    _ = try b2.process(Processor.fromFn(uppercaseProcessor, null));
    _ = try b2.endBranch();
    _ = try mc2.endMulticast();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    try std.testing.expectEqualStrings("HELLO", ex.body);
}

test "parallel multicast with parallel flag" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);
    var reg = Registry.init(services);
    defer reg.deinit();

    var r = RtBuilder.init(alloc, &reg);
    defer r.deinit();

    var mc = r.parallelMulticast();
    var b1 = mc.branch();
    _ = try b1.process(Processor.fromFn(uppercaseProcessor, null));
    const mc2 = try b1.endBranch();
    var b2 = mc2.branch();
    _ = try b2.process(Processor.fromFn(uppercaseProcessor, null));
    _ = try b2.endBranch();
    _ = try mc2.endMulticast();

    const rid = try r.build();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("hello");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&r.plan, rid, &ex);

    // With parallel multicast, clones are used; original exchange runs last branch
    try std.testing.expectEqualStrings("HELLO", ex.body);
}

// -------- Feature: Batch Processing --------

test "batch collects N messages then processes" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Batch = .{
        .size = 3,
        .separator = ",",
        .route = sub_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // Send 3 messages; only the third triggers the batch
    for ([_][]const u8{ "alpha", "beta", "gamma" }) |body| {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody(body);
        try exec.run(&plan, main_rid, &ex);
    }
}

test "batch via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);
    var reg = Registry.init(services);
    defer reg.deinit();

    var r = RtBuilder.init(alloc, &reg);
    defer r.deinit();

    var batch = r.batchWithSeparator(2, "|");
    _ = try batch.process(Processor.fromFn(uppercaseProcessor, null));
    _ = try batch.endBatch();

    const rid = try r.build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First message - buffered, no processing yet
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello");
        try exec.run(&r.plan, rid, &ex);
        // Body unchanged (still buffered)
        try std.testing.expectEqualStrings("hello", ex.body);
    }

    // Second message - batch fires
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("world");
        try exec.run(&r.plan, rid, &ex);
        // Body should be the processed batch result
        try std.testing.expectEqualStrings("HELLO|WORLD", ex.body);
    }
}

// -------- Feature: Request-Reply --------

fn echoEndpointSend(_: ?*anyopaque, ex: *Exchange) !void {
    // Echo: prepend "reply:" to body
    const old = ex.body;
    const prefix = "reply:";
    const new = try ex.allocator.alloc(u8, prefix.len + old.len);
    @memcpy(new[0..prefix.len], prefix);
    @memcpy(new[prefix.len..], old);
    if (old.len != 0) ex.allocator.free(old);
    ex.body = new;
}

fn echoCreateProducer(_: ?*anyopaque, _: std.mem.Allocator) !@import("endpoint.zig").Producer {
    return .{ .ctx = null, .sendFn = echoEndpointSend };
}

test "request-reply sets correlation ID and gets response" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    const echo_ep = @import("endpoint.zig").Endpoint{
        .ctx = null,
        .createProducerFn = echoCreateProducer,
        .deinitFn = null,
    };

    var steps = [_]Step{.{ .RequestReply = .{
        .endpoint = .{ .endpoint = echo_ep },
    } }};
    const rid = try plan.addRoute(alloc, &steps);

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("ping");

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    try exec.run(&plan, rid, &ex);

    // Body should be the reply
    try std.testing.expectEqualStrings("reply:ping", ex.body);
    // Correlation ID header should be set
    try std.testing.expect(ex.getHeader("CamelCorrelationId") != null);
}

// -------- Feature: Resequencer --------

test "resequencer reorders messages by sequence number" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Sub-route: just pass through (noop)
    var sub_steps = [_]Step{.{ .Process = Processor.fromFn(uppercaseProcessor, null) }};
    const sub_rid = try plan.addRoute(alloc, &sub_steps);

    var main_steps = [_]Step{.{ .Resequencer = .{
        .size = 3,
        .header = "SeqNum",
        .route = sub_rid,
    } }};
    const main_rid = try plan.addRoute(alloc, &main_steps);

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // Send messages out of order: 2, 0, 1
    const bodies = [_][]const u8{ "c", "a", "b" };
    const seqs = [_][]const u8{ "2", "0", "1" };

    for (bodies, seqs) |body, seq| {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody(body);
        try ex.putHeader("SeqNum", seq);
        try exec.run(&plan, main_rid, &ex);
    }
    // The resequencer buffers 3 messages and processes them in order (0, 1, 2)
}

test "resequencer via builder DSL" {
    const alloc = std.testing.allocator;
    const services = testServices(alloc);
    var reg = Registry.init(services);
    defer reg.deinit();

    var r = RtBuilder.init(alloc, &reg);
    defer r.deinit();

    var reseq = r.resequencerWithHeader(2, "SeqNum");
    _ = try reseq.process(Processor.fromFn(uppercaseProcessor, null));
    _ = try reseq.endResequencer();

    const rid = try r.build();

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    // First message - buffered
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("world");
        try ex.putHeader("SeqNum", "1");
        try exec.run(&r.plan, rid, &ex);
    }

    // Second message - triggers processing in order
    {
        var ex = Exchange.init(alloc);
        defer ex.deinit();
        try ex.setBody("hello");
        try ex.putHeader("SeqNum", "0");
        try exec.run(&r.plan, rid, &ex);
    }
}

// -------- Feature: Global Error Handler --------

fn failingProcessor(_: ?*anyopaque, _: *Exchange) anyerror!void {
    return error.ProcessorFailed;
}

test "global error handler catches route errors" {
    const alloc = std.testing.allocator;

    const MockStore = zamel.MockStore;
    var mock_store = MockStore.init(alloc);
    defer mock_store.deinit();

    var reg = Registry.init(testServices(alloc));
    defer reg.deinit();
    reg.setMockStore(&mock_store);

    const error_ep = try reg.resolve(alloc, "mock:errors");

    var services = testServices(alloc);
    services.error_handler = .{
        .endpoint = .{ .endpoint = error_ep },
        .max_retries = 0,
    };

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    var steps = [_]Step{.{ .Process = Processor.fromFn(failingProcessor, null) }};
    const rid = try plan.addRoute(alloc, &steps);

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &reg;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    // runRoute uses the global error handler
    try exec.runRoute(&plan, rid, &ex);

    // Error metadata headers should be set
    try std.testing.expectEqualStrings("ProcessorFailed", ex.getHeader("CamelExceptionMessage").?);
    try std.testing.expect(ex.getHeader("CamelExceptionTimestamp") != null);
    try std.testing.expect(ex.getHeader("CamelFailedRouteId") != null);

    // Mock endpoint should have received the exchange
    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("errors"));
}

test "global error handler with retries" {
    const alloc = std.testing.allocator;

    var call_count: u32 = 0;
    const CountCtx = struct { count: *u32 };
    var ctx = CountCtx{ .count = &call_count };

    const MockStore = zamel.MockStore;
    var mock_store = MockStore.init(alloc);
    defer mock_store.deinit();

    var reg = Registry.init(testServices(alloc));
    defer reg.deinit();
    reg.setMockStore(&mock_store);

    const error_ep = try reg.resolve(alloc, "mock:dead-letters");

    var services = testServices(alloc);
    services.error_handler = .{
        .endpoint = .{ .endpoint = error_ep },
        .max_retries = 2,
    };

    var plan = RoutePlan.init();
    defer plan.deinit(alloc);

    // Processor that fails 3 times then succeeds
    const counting_proc = Processor.fromFn(struct {
        fn call(c: ?*anyopaque, _: *Exchange) anyerror!void {
            const self: *CountCtx = @ptrCast(@alignCast(c.?));
            self.count.* += 1;
            return error.ProcessorFailed;
        }
    }.call, &ctx);

    var steps = [_]Step{.{ .Process = counting_proc }};
    const rid = try plan.addRoute(alloc, &steps);

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    exec.registry = &reg;

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("test");

    try exec.runRoute(&plan, rid, &ex);

    // Should have been called 3 times total (1 original + 2 retries)
    try std.testing.expectEqual(@as(u32, 3), call_count);
    // Should have been sent to dead letter
    try std.testing.expectEqual(@as(usize, 1), mock_store.captureCount("dead-letters"));
}

// -------- Feature: Cron Scheduling --------

test "cron expression parsing and matching" {
    const cron = zamel.cron;

    // Every 5 minutes
    const expr1 = try cron.parseCron("*/5 * * * *");
    try std.testing.expect(expr1.matches(0, 12, 1, 1, 0));
    try std.testing.expect(expr1.matches(15, 12, 1, 1, 0));
    try std.testing.expect(!expr1.matches(3, 12, 1, 1, 0));

    // Specific time: 9:30 on weekdays (Mon-Fri = 1-5)
    const expr2 = try cron.parseCron("30 9 * * 1");
    try std.testing.expect(expr2.matches(30, 9, 1, 1, 1));
    try std.testing.expect(!expr2.matches(30, 9, 1, 1, 0)); // Sunday
    try std.testing.expect(!expr2.matches(0, 9, 1, 1, 1)); // Wrong minute
}
