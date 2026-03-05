const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;

pub const Producer = struct {
    ctx: ?*anyopaque,
    sendFn: *const fn (ctx: ?*anyopaque, ex: *Exchange) anyerror!void,

    pub fn send(self: Producer, ex: *Exchange) !void {
        try self.sendFn(self.ctx, ex);
    }
};

pub const Endpoint = struct {
    ctx: ?*anyopaque,
    createProducerFn: *const fn (ctx: ?*anyopaque, allocator: std.mem.Allocator) anyerror!Producer,
    deinitFn: ?*const fn (ctx: ?*anyopaque, allocator: std.mem.Allocator) void = null,

    pub fn createProducer(self: Endpoint, allocator: std.mem.Allocator) !Producer {
        return try self.createProducerFn(self.ctx, allocator);
    }

    pub fn deinit(self: Endpoint, allocator: std.mem.Allocator) void {
        if (self.deinitFn) |f| f(self.ctx, allocator);
    }
};

/// "EndpointRef" is what Steps reference. Later you can add variants:
/// - typed endpoint
/// - parsed-from-URI endpoint
/// - lazy-resolved by registry
pub const EndpointRef = union(enum) {
    endpoint: Endpoint,
    uri: []const u8,
};
