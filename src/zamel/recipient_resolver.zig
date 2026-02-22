const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const EndpointRef = @import("endpoint.zig").EndpointRef;

pub const RecipientResolver = struct {
    ctx: ?*anyopaque = null,
    resolveFn: *const fn (ctx: ?*anyopaque, ex: *Exchange, out: *std.ArrayList(EndpointRef), allocator: std.mem.Allocator) anyerror!void,

    pub fn call(self: RecipientResolver, ex: *Exchange, out: *std.ArrayList(EndpointRef), allocator: std.mem.Allocator) !void {
        try self.resolveFn(self.ctx, ex, out, allocator);
    }

    pub fn fromFn(
        f: *const fn (?*anyopaque, *Exchange, *std.ArrayList(EndpointRef), std.mem.Allocator) anyerror!void,
        ctx: ?*anyopaque,
    ) RecipientResolver {
        return .{ .ctx = ctx, .resolveFn = f };
    }
};
