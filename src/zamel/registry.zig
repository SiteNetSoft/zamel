const std = @import("std");

const Consumer = @import("consumer.zig").Consumer;
const consumers = @import("consumers.zig");
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;
const Exchange = @import("exchange.zig").Exchange;

const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;
const direct = @import("direct.zig");
const seda = @import("seda.zig");
const bean = @import("bean.zig");
const mock = @import("mock.zig");

pub const BeanFn = bean.BeanFn;

pub const RouteState = enum { started, stopped, suspended };

pub const EndpointFactory = *const fn (allocator: std.mem.Allocator, rest: []const u8) anyerror!Endpoint;
pub const ConsumerFactory = *const fn (allocator: std.mem.Allocator, rest: []const u8) anyerror!Consumer;

pub const NamedRoute = struct {
    plan: *const RoutePlan,
    route_id: u32,
};

pub const Registry = struct {
    services: Services,
    cache: std.StringHashMap(Endpoint),
    endpoint_factories: std.StringHashMap(EndpointFactory),
    consumer_factories: std.StringHashMap(ConsumerFactory),
    named_routes: std.StringHashMap(NamedRoute),
    seda_queues: std.StringHashMap(*seda.SedaQueue),
    bean_functions: std.StringHashMap(BeanFn),
    route_states: std.StringHashMap(RouteState),
    mock_store: ?*mock.MockStore = null,

    pub fn init(services: Services) Registry {
        return .{
            .services = services,
            .cache = std.StringHashMap(Endpoint).init(services.allocator),
            .endpoint_factories = std.StringHashMap(EndpointFactory).init(services.allocator),
            .consumer_factories = std.StringHashMap(ConsumerFactory).init(services.allocator),
            .named_routes = std.StringHashMap(NamedRoute).init(services.allocator),
            .seda_queues = std.StringHashMap(*seda.SedaQueue).init(services.allocator),
            .bean_functions = std.StringHashMap(BeanFn).init(services.allocator),
            .route_states = std.StringHashMap(RouteState).init(services.allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        // Free cached endpoint contexts and keys
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.services.allocator);
            self.services.allocator.free(entry.key_ptr.*);
        }
        self.cache.deinit();
        self.endpoint_factories.deinit();
        self.consumer_factories.deinit();
        self.named_routes.deinit();
        self.bean_functions.deinit();
        self.route_states.deinit();

        // Clean up seda queues
        var sq_it = self.seda_queues.iterator();
        while (sq_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.services.allocator.destroy(entry.value_ptr.*);
        }
        self.seda_queues.deinit();
    }

    /// Register a custom endpoint scheme.
    pub fn registerEndpoint(self: *Registry, scheme: []const u8, factory: EndpointFactory) !void {
        try self.endpoint_factories.put(scheme, factory);
    }

    /// Register a custom consumer (source) scheme.
    pub fn registerConsumer(self: *Registry, scheme: []const u8, factory: ConsumerFactory) !void {
        try self.consumer_factories.put(scheme, factory);
    }

    pub fn registerRoute(self: *Registry, name: []const u8, plan: *const RoutePlan, route_id: u32) !void {
        try self.named_routes.put(name, .{ .plan = plan, .route_id = route_id });
    }

    pub fn lookupRoute(self: *Registry, name: []const u8) ?NamedRoute {
        return self.named_routes.get(name);
    }

    pub fn registerBean(self: *Registry, name: []const u8, func: BeanFn) !void {
        try self.bean_functions.put(name, func);
    }

    // ---- route lifecycle ----

    pub fn startRoute(self: *Registry, name: []const u8) !void {
        try self.route_states.put(name, .started);
    }

    pub fn stopRoute(self: *Registry, name: []const u8) !void {
        try self.route_states.put(name, .stopped);
    }

    pub fn suspendRoute(self: *Registry, name: []const u8) !void {
        try self.route_states.put(name, .suspended);
    }

    pub fn resumeRoute(self: *Registry, name: []const u8) !void {
        try self.route_states.put(name, .started);
    }

    pub fn getRouteState(self: *Registry, name: []const u8) RouteState {
        return self.route_states.get(name) orelse .started;
    }

    pub fn setMockStore(self: *Registry, store: *mock.MockStore) void {
        self.mock_store = store;
    }

    pub fn getOrCreateSedaQueue(self: *Registry) !*seda.SedaQueue {
        const q = try self.services.allocator.create(seda.SedaQueue);
        q.* = seda.SedaQueue.init(self.services.allocator);
        return q;
    }

    pub fn resolve(self: *Registry, allocator: std.mem.Allocator, uri: []const u8) !Endpoint {
        _ = allocator;
        if (self.cache.get(uri)) |cached| return cached;

        const alloc = self.services.allocator;
        const parsed = try parseUri(uri);

        const ep = if (std.mem.eql(u8, parsed.scheme, "log"))
            try makeLogEndpoint(alloc, parsed.rest)
        else if (std.mem.eql(u8, parsed.scheme, "file"))
            try makeFileEndpoint(alloc, parsed.rest)
        else if (std.mem.eql(u8, parsed.scheme, "direct"))
            try direct.makeDirectEndpoint(alloc, parsed.rest, self)
        else if (std.mem.eql(u8, parsed.scheme, "seda"))
            try self.resolveSedaEndpoint(alloc, parsed.rest)
        else if (std.mem.eql(u8, parsed.scheme, "http") or std.mem.eql(u8, parsed.scheme, "https"))
            try @import("http.zig").makeHttpEndpoint(alloc, uri)
        else if (std.mem.eql(u8, parsed.scheme, "bean"))
            try self.resolveBeanEndpoint(alloc, parsed.rest)
        else if (std.mem.eql(u8, parsed.scheme, "mock"))
            try self.resolveMockEndpoint(alloc, parsed.rest)
        else if (self.endpoint_factories.get(parsed.scheme)) |factory|
            try factory(alloc, parsed.rest)
        else
            return error.UnknownEndpointScheme;

        const key = try alloc.dupe(u8, uri);
        try self.cache.put(key, ep);
        return ep;
    }

    fn resolveMockEndpoint(self: *Registry, allocator: std.mem.Allocator, name: []const u8) !Endpoint {
        const store = self.mock_store orelse return error.MockStoreNotSet;
        return try mock.makeMockEndpoint(allocator, name, store);
    }

    fn resolveBeanEndpoint(self: *Registry, allocator: std.mem.Allocator, name: []const u8) !Endpoint {
        const func = self.bean_functions.get(name) orelse return error.BeanNotFound;
        return try bean.makeBeanEndpoint(allocator, func);
    }

    fn resolveSedaEndpoint(self: *Registry, allocator: std.mem.Allocator, name: []const u8) !Endpoint {
        const gop = try self.seda_queues.getOrPut(name);
        if (!gop.found_existing) {
            const q = try allocator.create(seda.SedaQueue);
            q.* = seda.SedaQueue.init(allocator);
            gop.value_ptr.* = q;
        }
        return try seda.makeSedaEndpoint(allocator, gop.value_ptr.*);
    }

    /// Resolves a source URI to a Consumer.
    pub fn resolveConsumer(self: *Registry, allocator: std.mem.Allocator, uri: []const u8) !Consumer {
        const parsed = try parseUri(uri);
        if (std.mem.eql(u8, parsed.scheme, "timer")) {
            const cfg = try parseTimer(parsed.rest);
            return try consumers.timer(allocator, cfg.interval_ms, cfg.repeat);
        }
        if (std.mem.eql(u8, parsed.scheme, "file")) {
            return try consumers.fileReader(allocator, parsed.rest);
        }
        if (std.mem.eql(u8, parsed.scheme, "direct")) {
            return try direct.makeDirectConsumer(allocator, parsed.rest, self);
        }
        if (std.mem.eql(u8, parsed.scheme, "seda")) {
            const gop = try self.seda_queues.getOrPut(parsed.rest);
            if (!gop.found_existing) {
                const q = try allocator.create(seda.SedaQueue);
                q.* = seda.SedaQueue.init(allocator);
                gop.value_ptr.* = q;
            }
            return try seda.makeSedaConsumer(allocator, gop.value_ptr.*, self);
        }
        if (self.consumer_factories.get(parsed.scheme)) |factory| {
            return try factory(allocator, parsed.rest);
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

fn makeLogEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    const lvl = parseLogLevel(rest);

    const Ctx = struct { lvl: LogLevel };
    const p = try allocator.create(Ctx);
    p.* = .{ .lvl = lvl };

    return .{
        .ctx = p,
        .createProducerFn = logCreateProducer,
        .deinitFn = logDeinit,
    };
}

fn logDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const Ctx = struct { lvl: LogLevel };
    const p: *Ctx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
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

// -------- file endpoint --------
// URI: "file:path/to/file" or "file:/absolute/path"
// Query: ?append=true (default: overwrite)

const posix = std.posix;

const FileCtx = struct {
    path: []const u8,
    append: bool,
};

fn makeFileEndpoint(allocator: std.mem.Allocator, rest: []const u8) !Endpoint {
    var path = rest;
    var append = false;

    if (std.mem.indexOfScalar(u8, rest, '?')) |q| {
        path = rest[0..q];
        const query = rest[q + 1 ..];
        if (std.mem.eql(u8, query, "append=true")) append = true;
    }

    const p = try allocator.create(FileCtx);
    p.* = .{ .path = path, .append = append };
    return .{ .ctx = p, .createProducerFn = fileCreateProducer, .deinitFn = fileDeinit };
}

fn fileDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const p: *FileCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
}

fn fileCreateProducer(ctx: ?*anyopaque, allocator: std.mem.Allocator) !Producer {
    _ = allocator;
    return .{ .ctx = ctx, .sendFn = fileSend };
}

fn fileSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *const FileCtx = @ptrCast(@alignCast(ctx.?));

    const flags: posix.O = if (c.append)
        .{ .ACCMODE = .WRONLY, .APPEND = true, .CREAT = true }
    else
        .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true };

    const fd = try posix.openat(posix.AT.FDCWD, c.path, flags, 0o644);
    defer posix.close(fd);

    try writeAll(fd, ex.body);
    if (c.append) try writeAll(fd, "\n");
}

fn writeAll(fd: posix.fd_t, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = posix.system.write(fd, data.ptr + written, data.len - written);
        switch (posix.errno(rc)) {
            .SUCCESS => written += rc,
            .INTR => continue,
            else => return error.WriteFailed,
        }
    }
}
