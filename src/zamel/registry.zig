const std = @import("std");

const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Exchange = @import("exchange.zig").Exchange;

const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

pub const Registry = struct {
    services: Services,

    pub fn init(services: Services) Registry {
        return .{ .services = services };
    }

    pub fn resolve(self: *Registry, allocator: std.mem.Allocator, uri: []const u8) !Endpoint {
        _ = self;
        _ = allocator;

        const parsed = try parseUri(uri);
        if (std.mem.eql(u8, parsed.scheme, "log")) {
            return makeLogEndpoint(parsed.rest);
        }
        return error.UnknownEndpointScheme;
    }

    /// Starts a source endpoint (timer demo) that drives the route.
    pub fn startSource(self: *Registry, gpa: std.mem.Allocator, uri: []const u8, plan: *const RoutePlan, route_id: u32) !void {
        const parsed = try parseUri(uri);
        if (!std.mem.eql(u8, parsed.scheme, "timer")) return error.UnsupportedSource;

        const cfg = try parseTimer(parsed.rest);

        var exec = SyncExecutor.init(self.services);

        var i: u64 = 0;
        while (cfg.repeat == 0 or i < cfg.repeat) : (i += 1) {
            var ex = Exchange.init(gpa);
            defer ex.deinit();

            // format tick header into a stack buffer
            var tick_buf: [32]u8 = undefined;
            const tick_str = try std.fmt.bufPrint(&tick_buf, "{d}", .{i});
            try ex.putHeader("tick", tick_str);

            // format body into a stack buffer
            var body_buf: [64]u8 = undefined;
            const body_str = try std.fmt.bufPrint(&body_buf, "tick {d}", .{i});
            try ex.setBody(body_str);

            // Demo: mark some messages as type A / B
            if (i % 2 == 0)
                try ex.putHeader("type", "A")
            else
                try ex.putHeader("type", "B");

            try exec.run(plan, @intCast(route_id), &ex);

            // TODO: implement proper sleeping for your Zig version.
            // For now, we don't sleep, so timer messages run back-to-back.
            // std.time.sleep(cfg.interval_ms * std.time.ns_per_ms);
        }
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

fn makeLogEndpoint(rest: []const u8) Endpoint {
    const lvl = parseLogLevel(rest);

    const Ctx = struct { lvl: LogLevel };
    // Allocate ctx once per endpoint; in a real registry you'd own/free it.
    // For demo we keep it static-like by embedding in a global; simplest: capture as pointer to a constant.
    // We'll do a tiny heap alloc from page allocator:
    const ctx_ptr = blk: {
        const p = std.heap.page_allocator.create(Ctx) catch unreachable;
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
