const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Endpoint = @import("endpoint.zig").Endpoint;
const Producer = @import("endpoint.zig").Producer;

pub const CapturedExchange = struct {
    body: []u8,
    headers: std.StringHashMap([]u8),
};

pub const MockStore = struct {
    exchanges: std.StringHashMap(std.ArrayList(CapturedExchange)),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) MockStore {
        return .{
            .exchanges = std.StringHashMap(std.ArrayList(CapturedExchange)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MockStore) void {
        var it = self.exchanges.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.items) |*cap| {
                self.allocator.free(cap.body);
                var hit = cap.headers.iterator();
                while (hit.next()) |he| self.allocator.free(he.value_ptr.*);
                cap.headers.deinit();
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.exchanges.deinit();
    }

    pub fn getCaptures(self: *MockStore, name: []const u8) ?[]const CapturedExchange {
        if (self.exchanges.get(name)) |list| {
            return list.items;
        }
        return null;
    }

    pub fn captureCount(self: *MockStore, name: []const u8) usize {
        if (self.exchanges.get(name)) |list| {
            return list.items.len;
        }
        return 0;
    }

    pub fn reset(self: *MockStore) void {
        var it = self.exchanges.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.items) |*cap| {
                self.allocator.free(cap.body);
                var hit = cap.headers.iterator();
                while (hit.next()) |he| self.allocator.free(he.value_ptr.*);
                cap.headers.deinit();
            }
            entry.value_ptr.clearAndFree(self.allocator);
        }
    }

    fn capture(self: *MockStore, name: []const u8, ex: *Exchange) !void {
        const gop = try self.exchanges.getOrPut(name);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }

        // Snapshot body
        const body = try self.allocator.dupe(u8, ex.body);

        // Snapshot headers
        var headers = std.StringHashMap([]u8).init(self.allocator);
        var hit = ex.headers.iterator();
        while (hit.next()) |entry| {
            const val = try self.allocator.dupe(u8, entry.value_ptr.*);
            try headers.put(entry.key_ptr.*, val);
        }

        try gop.value_ptr.append(self.allocator, .{
            .body = body,
            .headers = headers,
        });
    }
};

const MockCtx = struct {
    name: []const u8,
    store: *MockStore,
};

pub fn makeMockEndpoint(allocator: std.mem.Allocator, name: []const u8, store: *MockStore) !Endpoint {
    const ctx = try allocator.create(MockCtx);
    ctx.* = .{ .name = name, .store = store };
    return .{
        .ctx = ctx,
        .createProducerFn = mockCreateProducer,
        .deinitFn = mockDeinit,
    };
}

fn mockDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const c: *MockCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(c);
}

fn mockCreateProducer(ctx: ?*anyopaque, _: std.mem.Allocator) !Producer {
    return .{ .ctx = ctx, .sendFn = mockSend };
}

fn mockSend(ctx: ?*anyopaque, ex: *Exchange) !void {
    const c: *MockCtx = @ptrCast(@alignCast(ctx.?));
    try c.store.capture(c.name, ex);
}
