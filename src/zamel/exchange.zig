const std = @import("std");

pub const Exchange = struct {
    allocator: std.mem.Allocator,
    body: []u8 = &[_]u8{},
    headers: std.StringHashMap([]u8),

    pub fn init(allocator: std.mem.Allocator) Exchange {
        return .{
            .allocator = allocator,
            .headers = std.StringHashMap([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Exchange) void {
        var it = self.headers.iterator();
        while (it.next()) |e| self.allocator.free(e.value_ptr.*);
        self.headers.deinit();
        if (self.body.len != 0) self.allocator.free(self.body);
    }

    pub fn setBody(self: *Exchange, bytes: []const u8) !void {
        if (self.body.len != 0) self.allocator.free(self.body);
        self.body = try self.allocator.dupe(u8, bytes);
    }

    pub fn putHeader(self: *Exchange, key: []const u8, value: []const u8) !void {
        const v = try self.allocator.dupe(u8, value);
        if (self.headers.get(key)) |old| self.allocator.free(old);
        try self.headers.put(key, v);
    }

    pub fn getHeader(self: *Exchange, key: []const u8) ?[]const u8 {
        return self.headers.get(key);
    }
};

// -------- tests --------

test "setBody stores and replaces body" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    try ex.setBody("hello");
    try std.testing.expectEqualStrings("hello", ex.body);

    try ex.setBody("world");
    try std.testing.expectEqualStrings("world", ex.body);
}

test "getHeader returns null for missing key" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    try std.testing.expect(ex.getHeader("missing") == null);
}

test "putHeader stores and retrieves value" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    try ex.putHeader("key", "value");
    try std.testing.expectEqualStrings("value", ex.getHeader("key").?);
}

test "putHeader replaces existing value" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();

    try ex.putHeader("key", "old");
    try ex.putHeader("key", "new");
    try std.testing.expectEqualStrings("new", ex.getHeader("key").?);
}