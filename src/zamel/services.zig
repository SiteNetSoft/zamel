const std = @import("std");

pub const Clock = struct {
    nowMillisFn: *const fn (ctx: ?*anyopaque) u64,
    ctx: ?*anyopaque = null,

    pub fn nowMillis(self: Clock) u64 {
        return self.nowMillisFn(self.ctx);
    }
};

pub const Scheduler = struct {
    // MVP: you can stub this. Later: schedule timers, delayed actions, etc.
    ctx: ?*anyopaque = null,
    // scheduleFn: ...
};

pub const StateStore = struct {
    // Generic KV store for aggregator/idempotent/saga/etc.
    ctx: ?*anyopaque = null,

    putFn: *const fn (ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!void,
    getFn: *const fn (ctx: ?*anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8,
    delFn: *const fn (ctx: ?*anyopaque, key: []const u8) anyerror!void,

    pub fn put(self: StateStore, key: []const u8, value: []const u8) !void {
        try self.putFn(self.ctx, key, value);
    }
    pub fn get(self: StateStore, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        return try self.getFn(self.ctx, allocator, key);
    }
    pub fn del(self: StateStore, key: []const u8) !void {
        try self.delFn(self.ctx, key);
    }
};

pub const Metrics = @import("metrics.zig").Metrics;

pub const Services = struct {
    allocator: std.mem.Allocator,
    clock: Clock,
    scheduler: ?Scheduler = null,
    store: ?StateStore = null,
    metrics: ?Metrics = null,
};
