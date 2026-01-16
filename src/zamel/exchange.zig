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