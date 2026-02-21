const std = @import("std");

pub const Splitter = struct {
    ctx: ?*anyopaque = null,
    splitFn: *const fn (ctx: ?*anyopaque, body: []const u8, out: *std.ArrayList([]const u8), allocator: std.mem.Allocator) anyerror!void,

    pub fn call(self: Splitter, body: []const u8, out: *std.ArrayList([]const u8), allocator: std.mem.Allocator) !void {
        try self.splitFn(self.ctx, body, out, allocator);
    }

    pub fn fromFn(
        f: *const fn (?*anyopaque, []const u8, *std.ArrayList([]const u8), std.mem.Allocator) anyerror!void,
        ctx: ?*anyopaque,
    ) Splitter {
        return .{ .ctx = ctx, .splitFn = f };
    }
};
