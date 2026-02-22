const std = @import("std");
const StateStore = @import("services.zig").StateStore;

pub const InMemoryStore = struct {
    allocator: std.mem.Allocator,
    map: std.StringHashMap([]u8),

    pub fn init(allocator: std.mem.Allocator) InMemoryStore {
        return .{
            .allocator = allocator,
            .map = std.StringHashMap([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *InMemoryStore) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit();
    }

    pub fn stateStore(self: *InMemoryStore) StateStore {
        return .{
            .ctx = self,
            .putFn = putFn,
            .getFn = getFn,
            .delFn = delFn,
        };
    }
};

fn putFn(ctx: ?*anyopaque, key: []const u8, value: []const u8) !void {
    const self: *InMemoryStore = @ptrCast(@alignCast(ctx.?));
    const v = try self.allocator.dupe(u8, value);

    if (self.map.getEntry(key)) |entry| {
        self.allocator.free(entry.value_ptr.*);
        entry.value_ptr.* = v;
    } else {
        const k = try self.allocator.dupe(u8, key);
        try self.map.put(k, v);
    }
}

fn getFn(ctx: ?*anyopaque, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
    const self: *InMemoryStore = @ptrCast(@alignCast(ctx.?));
    const val = self.map.get(key) orelse return null;
    return try allocator.dupe(u8, val);
}

fn delFn(ctx: ?*anyopaque, key: []const u8) !void {
    const self: *InMemoryStore = @ptrCast(@alignCast(ctx.?));
    if (self.map.getEntry(key)) |entry| {
        const k = entry.key_ptr.*;
        const v = entry.value_ptr.*;
        self.map.removeByPtr(entry.key_ptr);
        self.allocator.free(k);
        self.allocator.free(v);
    }
}

// -------- tests --------

test "put and get value" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const ss = store.stateStore();
    try ss.put("key1", "value1");

    const val = try ss.get(std.testing.allocator, "key1");
    defer std.testing.allocator.free(val.?);
    try std.testing.expectEqualStrings("value1", val.?);
}

test "get missing key returns null" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const ss = store.stateStore();
    const val = try ss.get(std.testing.allocator, "missing");
    try std.testing.expect(val == null);
}

test "put overwrites existing value" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const ss = store.stateStore();
    try ss.put("key", "old");
    try ss.put("key", "new");

    const val = try ss.get(std.testing.allocator, "key");
    defer std.testing.allocator.free(val.?);
    try std.testing.expectEqualStrings("new", val.?);
}

test "del removes key" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const ss = store.stateStore();
    try ss.put("key", "val");
    try ss.del("key");

    const val = try ss.get(std.testing.allocator, "key");
    try std.testing.expect(val == null);
}

test "del on missing key is no-op" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const ss = store.stateStore();
    try ss.del("nonexistent");
}
