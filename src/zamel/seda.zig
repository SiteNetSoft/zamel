const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;
const Registry = @import("registry.zig").Registry;

pub const SedaMessage = struct {
    body: []u8,
    headers: std.StringHashMap([]u8),
};

pub const SedaQueue = struct {
    allocator: std.mem.Allocator,
    messages: std.ArrayList(SedaMessage),

    pub fn init(allocator: std.mem.Allocator) SedaQueue {
        return .{
            .allocator = allocator,
            .messages = .empty,
        };
    }

    pub fn deinit(self: *SedaQueue) void {
        for (self.messages.items) |*msg| {
            self.allocator.free(msg.body);
            var hit = msg.headers.iterator();
            while (hit.next()) |e| self.allocator.free(e.value_ptr.*);
            msg.headers.deinit();
        }
        self.messages.deinit(self.allocator);
    }

    pub fn enqueue(self: *SedaQueue, ex: *Exchange) !void {
        var headers = std.StringHashMap([]u8).init(self.allocator);
        var hit = ex.headers.iterator();
        while (hit.next()) |e| {
            const v = try self.allocator.dupe(u8, e.value_ptr.*);
            try headers.put(e.key_ptr.*, v);
        }
        try self.messages.append(self.allocator, .{
            .body = try self.allocator.dupe(u8, ex.body),
            .headers = headers,
        });
    }

    pub fn drain(self: *SedaQueue, allocator: std.mem.Allocator) ![]SedaMessage {
        if (self.messages.items.len == 0) return &[_]SedaMessage{};
        const items = try allocator.dupe(SedaMessage, self.messages.items);
        self.messages.clearRetainingCapacity();
        return items;
    }

    pub fn len(self: *const SedaQueue) usize {
        return self.messages.items.len;
    }
};

// -------- endpoint / producer --------

const SedaEpCtx = struct {
    queue: *SedaQueue,
};

pub fn makeSedaEndpoint(allocator: std.mem.Allocator, queue: *SedaQueue) !Endpoint {
    const ctx = try allocator.create(SedaEpCtx);
    ctx.* = .{ .queue = queue };
    return .{
        .ctx = ctx,
        .createProducerFn = sedaCreateProducer,
        .deinitFn = sedaEpDeinit,
    };
}

fn sedaEpDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *SedaEpCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn sedaCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = sedaSend };
}

fn sedaSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const SedaEpCtx = @ptrCast(@alignCast(ctx.?));
    try c.queue.enqueue(ex);
}

// -------- consumer --------

const SedaConsumerCtx = struct {
    queue: *SedaQueue,
    registry: *Registry,
};

pub fn makeSedaConsumer(allocator: std.mem.Allocator, queue: *SedaQueue, registry: *Registry) !Consumer {
    const ctx = try allocator.create(SedaConsumerCtx);
    ctx.* = .{ .queue = queue, .registry = registry };
    return .{
        .ctx = ctx,
        .startFn = sedaConsumerStart,
        .deinitFn = sedaConsumerDeinit,
    };
}

fn sedaConsumerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *SedaConsumerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn sedaConsumerStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
    const c: *const SedaConsumerCtx = @ptrCast(@alignCast(ctx.?));
    const messages = try c.queue.drain(allocator);
    defer allocator.free(messages);

    for (messages) |*msg| {
        var ex = Exchange.init(allocator);
        defer ex.deinit();

        // Transfer body (ownership moves to exchange via setBody which dupes)
        try ex.setBody(msg.body);
        allocator.free(msg.body);

        // Transfer headers
        var hit = msg.headers.iterator();
        while (hit.next()) |e| {
            try ex.putHeader(e.key_ptr.*, e.value_ptr.*);
            allocator.free(e.value_ptr.*);
        }
        msg.headers.deinit();

        var exec = SyncExecutor.init(services);
        defer exec.deinit();
        try exec.run(plan, route_id, &ex);
    }
}

// -------- tests --------

test "SedaQueue enqueue and drain" {
    const alloc = std.testing.allocator;
    var queue = SedaQueue.init(alloc);
    defer queue.deinit();

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("msg1");
    try ex.putHeader("key", "val");

    try queue.enqueue(&ex);
    try std.testing.expectEqual(@as(usize, 1), queue.len());

    const messages = try queue.drain(alloc);
    defer alloc.free(messages);
    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqualStrings("msg1", messages[0].body);

    // Clean up drained message contents
    alloc.free(messages[0].body);
    var hdr = messages[0].headers;
    var hit = hdr.iterator();
    while (hit.next()) |e| alloc.free(e.value_ptr.*);
    hdr.deinit();

    // Queue should be empty
    try std.testing.expectEqual(@as(usize, 0), queue.len());
}

test "SedaQueue empty drain" {
    const alloc = std.testing.allocator;
    var queue = SedaQueue.init(alloc);
    defer queue.deinit();

    const messages = try queue.drain(alloc);
    try std.testing.expectEqual(@as(usize, 0), messages.len);
}
