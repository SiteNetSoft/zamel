const std = @import("std");

const Consumer = @import("consumer.zig").Consumer;
const consumers = @import("consumers.zig");
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Exchange = @import("exchange.zig").Exchange;

const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

pub const Registry = struct {
    services: Services,
    cache: std.StringHashMap(Endpoint),

    pub fn init(services: Services) Registry {
        return .{
            .services = services,
            .cache = std.StringHashMap(Endpoint).init(services.allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.cache.keyIterator();
        while (it.next()) |key_ptr| self.services.allocator.free(key_ptr.*);
        self.cache.deinit();
    }

    pub fn resolve(self: *Registry, allocator: std.mem.Allocator, uri: []const u8) !Endpoint {
        if (self.cache.get(uri)) |cached| return cached;

        const parsed = try parseUri(uri);
        const ep = if (std.mem.eql(u8, parsed.scheme, "log"))
            makeLogEndpoint(allocator, parsed.rest)
        else
            return error.UnknownEndpointScheme;

        const key = try self.services.allocator.dupe(u8, uri);
        try self.cache.put(key, ep);
        return ep;
    }

    /// Resolves a source URI to a Consumer.
    pub fn resolveConsumer(_: *Registry, allocator: std.mem.Allocator, uri: []const u8) !Consumer {
        const parsed = try parseUri(uri);
        if (std.mem.eql(u8, parsed.scheme, "timer")) {
            const cfg = try parseTimer(parsed.rest);
            return consumers.timer(allocator, cfg.interval_ms, cfg.repeat);
        }
        return error.UnsupportedSource;
    }

    /// Starts a source endpoint that drives the route.
    pub fn startSource(self: *Registry, gpa: std.mem.Allocator, uri: []const u8, plan: *const RoutePlan, route_id: u32) !void {
        const consumer = try self.resolveConsumer(gpa, uri);
        defer consumer.deinit(gpa);
        try consumer.start(gpa, self.services, plan, route_id);
    }
};

const ParsedUri = struct {
    scheme: []const u8,
    rest: []const u8,
};

fn parseUri(uri: []const u8) !ParsedUri {
    // Minimal: "scheme:rest"
    const idx = std.mem.indexOfScalar(u8, uri, ':') orelse return error.InvalidUri;
    return .{
        .scheme = uri[0..idx],
        .rest = uri[idx + 1 ..],
    };
}

// -------- log endpoint --------

const LogLevel = enum { debug, info, warn, err };

fn makeLogEndpoint(allocator: std.mem.Allocator, rest: []const u8) Endpoint {
    const lvl = parseLogLevel(rest);

    const Ctx = struct { lvl: LogLevel };
    const ctx_ptr = blk: {
        const p = allocator.create(Ctx) catch unreachable;
        p.* = .{ .lvl = lvl };
        break :blk p;
    };

    return .{
        .ctx = ctx_ptr,
        .createProducerFn = logCreateProducer,
    };
}

fn parseLogLevel(rest: []const u8) LogLevel {
    if (std.mem.eql(u8, rest, "debug")) return .debug;
    if (std.mem.eql(u8, rest, "warn")) return .warn;
    if (std.mem.eql(u8, rest, "error")) return .err;
    return .info;
}

fn logCreateProducer(ctx: ?*anyopaque, allocator: std.mem.Allocator) !Producer {
    _ = allocator;
    return .{ .ctx = ctx, .sendFn = logSend };
}

fn logSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const Ctx = struct { lvl: LogLevel };
    const c: *const Ctx = @ptrCast(@alignCast(ctx.?));

    switch (c.lvl) {
        .debug => std.log.debug("{s}", .{ex.body}),
        .info => std.log.info("{s}", .{ex.body}),
        .warn => std.log.warn("{s}", .{ex.body}),
        .err => std.log.err("{s}", .{ex.body}),
    }
}

// -------- timer parsing --------

const TimerCfg = struct {
    interval_ms: u64,
    repeat: u64, // 0 => forever
};

fn parseTimer(rest: []const u8) !TimerCfg {
    // Accept:
    // "500"
    // "500?repeat=10"
    var interval_part = rest;
    var repeat: u64 = 0;

    if (std.mem.indexOfScalar(u8, rest, '?')) |q| {
        interval_part = rest[0..q];
        const query = rest[q + 1 ..];
        // minimal: only repeat=
        if (std.mem.startsWith(u8, query, "repeat=")) {
            repeat = try std.fmt.parseUnsigned(u64, query["repeat=".len..], 10);
        }
    }

    const interval_ms = try std.fmt.parseUnsigned(u64, interval_part, 10);
    return .{ .interval_ms = interval_ms, .repeat = repeat };
}
