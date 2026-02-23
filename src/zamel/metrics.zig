const std = @import("std");

pub const Metrics = struct {
    ctx: ?*anyopaque = null,
    incrementFn: *const fn (ctx: ?*anyopaque, name: []const u8) void,
    recordFn: *const fn (ctx: ?*anyopaque, name: []const u8, value: u64) void,

    pub fn increment(self: Metrics, name: []const u8) void {
        self.incrementFn(self.ctx, name);
    }

    pub fn record(self: Metrics, name: []const u8, value: u64) void {
        self.recordFn(self.ctx, name, value);
    }
};

pub const InMemoryMetrics = struct {
    allocator: std.mem.Allocator,
    counters: std.StringHashMap(u64),
    gauges: std.StringHashMap(u64),

    pub fn init(allocator: std.mem.Allocator) InMemoryMetrics {
        return .{
            .allocator = allocator,
            .counters = std.StringHashMap(u64).init(allocator),
            .gauges = std.StringHashMap(u64).init(allocator),
        };
    }

    pub fn deinit(self: *InMemoryMetrics) void {
        {
            var it = self.counters.iterator();
            while (it.next()) |e| self.allocator.free(e.key_ptr.*);
            self.counters.deinit();
        }
        {
            var it = self.gauges.iterator();
            while (it.next()) |e| self.allocator.free(e.key_ptr.*);
            self.gauges.deinit();
        }
    }

    pub fn metrics(self: *InMemoryMetrics) Metrics {
        return .{
            .ctx = self,
            .incrementFn = incrementFn,
            .recordFn = recordFn,
        };
    }

    pub fn getCounter(self: *InMemoryMetrics, name: []const u8) ?u64 {
        return self.counters.get(name);
    }

    pub fn getGauge(self: *InMemoryMetrics, name: []const u8) ?u64 {
        return self.gauges.get(name);
    }
};

fn incrementFn(ctx: ?*anyopaque, name: []const u8) void {
    const self: *InMemoryMetrics = @ptrCast(@alignCast(ctx.?));
    if (self.counters.getEntry(name)) |entry| {
        entry.value_ptr.* += 1;
    } else {
        const key = self.allocator.dupe(u8, name) catch return;
        self.counters.put(key, 1) catch {
            self.allocator.free(key);
        };
    }
}

fn recordFn(ctx: ?*anyopaque, name: []const u8, value: u64) void {
    const self: *InMemoryMetrics = @ptrCast(@alignCast(ctx.?));
    if (self.gauges.getEntry(name)) |entry| {
        entry.value_ptr.* = value;
    } else {
        const key = self.allocator.dupe(u8, name) catch return;
        self.gauges.put(key, value) catch {
            self.allocator.free(key);
        };
    }
}

// -------- tests --------

test "increment counter" {
    var m = InMemoryMetrics.init(std.testing.allocator);
    defer m.deinit();

    const iface = m.metrics();
    iface.increment("test.count");
    iface.increment("test.count");

    try std.testing.expectEqual(@as(u64, 2), m.getCounter("test.count").?);
}

test "record gauge" {
    var m = InMemoryMetrics.init(std.testing.allocator);
    defer m.deinit();

    const iface = m.metrics();
    iface.record("test.gauge", 42);

    try std.testing.expectEqual(@as(u64, 42), m.getGauge("test.gauge").?);
}

test "get missing counter returns null" {
    var m = InMemoryMetrics.init(std.testing.allocator);
    defer m.deinit();

    try std.testing.expect(m.getCounter("nonexistent") == null);
}

test "record overwrites gauge" {
    var m = InMemoryMetrics.init(std.testing.allocator);
    defer m.deinit();

    const iface = m.metrics();
    iface.record("test.gauge", 10);
    iface.record("test.gauge", 20);

    try std.testing.expectEqual(@as(u64, 20), m.getGauge("test.gauge").?);
}
