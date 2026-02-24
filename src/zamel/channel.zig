const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;

pub const ChannelMessage = struct {
    body: []u8,
    headers: std.StringHashMap([]u8),

    pub fn deinit(self: *ChannelMessage, allocator: std.mem.Allocator) void {
        if (self.body.len != 0) allocator.free(self.body);
        var it = self.headers.iterator();
        while (it.next()) |e| allocator.free(e.value_ptr.*);
        self.headers.deinit();
    }
};

/// Copy exchange data into a ChannelMessage (owned copies).
pub fn messageFromExchange(allocator: std.mem.Allocator, ex: *Exchange) !ChannelMessage {
    const body = try allocator.dupe(u8, ex.body);
    errdefer allocator.free(body);

    var headers = std.StringHashMap([]u8).init(allocator);
    errdefer {
        var it = headers.iterator();
        while (it.next()) |e| allocator.free(e.value_ptr.*);
        headers.deinit();
    }

    var it = ex.headers.iterator();
    while (it.next()) |e| {
        const v = try allocator.dupe(u8, e.value_ptr.*);
        try headers.put(e.key_ptr.*, v);
    }

    return .{ .body = body, .headers = headers };
}

/// Move ownership from ChannelMessage into a new Exchange.
pub fn exchangeFromMessage(allocator: std.mem.Allocator, msg: *ChannelMessage) !Exchange {
    var ex = Exchange.init(allocator);

    // Transfer body ownership
    if (msg.body.len > 0) {
        try ex.setBody(msg.body);
        allocator.free(msg.body);
        msg.body = &[_]u8{};
    }

    // Copy headers
    var it = msg.headers.iterator();
    while (it.next()) |e| {
        try ex.putHeader(e.key_ptr.*, e.value_ptr.*);
        allocator.free(e.value_ptr.*);
    }
    msg.headers.deinit();
    msg.headers = std.StringHashMap([]u8).init(allocator);

    return ex;
}

/// Thread-safe bounded channel (ring buffer) for ChannelMessage.
pub const Channel = struct {
    buffer: []?ChannelMessage,
    capacity: usize,
    head: usize,
    tail: usize,
    count: usize,
    closed: bool,
    mutex: std.Thread.Mutex,
    not_empty: std.Thread.Condition,
    not_full: std.Thread.Condition,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Channel {
        const buf = try allocator.alloc(?ChannelMessage, capacity);
        @memset(buf, null);
        return .{
            .buffer = buf,
            .capacity = capacity,
            .head = 0,
            .tail = 0,
            .count = 0,
            .closed = false,
            .mutex = .{},
            .not_empty = .{},
            .not_full = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Channel) void {
        // Clean up any remaining messages
        for (self.buffer) |*slot| {
            if (slot.*) |*msg| msg.deinit(self.allocator);
        }
        self.allocator.free(self.buffer);
    }

    /// Put a message into the channel. Blocks if full. Returns error if closed.
    pub fn put(self: *Channel, msg: ChannelMessage) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.count == self.capacity and !self.closed) {
            self.not_full.wait(&self.mutex);
        }

        if (self.closed) return error.ChannelClosed;

        self.buffer[self.tail] = msg;
        self.tail = (self.tail + 1) % self.capacity;
        self.count += 1;

        self.not_empty.signal();
    }

    /// Take a message from the channel. Blocks if empty. Returns null if closed and empty.
    pub fn take(self: *Channel) ?ChannelMessage {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.count == 0 and !self.closed) {
            self.not_empty.wait(&self.mutex);
        }

        if (self.count == 0) return null; // closed and empty

        const msg = self.buffer[self.head].?;
        self.buffer[self.head] = null;
        self.head = (self.head + 1) % self.capacity;
        self.count -= 1;

        self.not_full.signal();
        return msg;
    }

    /// Close the channel. Wakes all waiters.
    pub fn close(self: *Channel) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.closed = true;
        self.not_empty.broadcast();
        self.not_full.broadcast();
    }
};

// -------- tests --------

test "channel put and take" {
    var ch = try Channel.init(std.testing.allocator, 4);
    defer ch.deinit();

    // Create messages
    const headers1 = std.StringHashMap([]u8).init(std.testing.allocator);
    const body1 = try std.testing.allocator.dupe(u8, "message-1");
    try ch.put(.{ .body = body1, .headers = headers1 });

    const headers2 = std.StringHashMap([]u8).init(std.testing.allocator);
    const body2 = try std.testing.allocator.dupe(u8, "message-2");
    try ch.put(.{ .body = body2, .headers = headers2 });

    // Take in FIFO order
    var msg1 = ch.take().?;
    defer msg1.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("message-1", msg1.body);

    var msg2 = ch.take().?;
    defer msg2.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("message-2", msg2.body);
}

test "channel blocks and close wakes" {
    var ch = try Channel.init(std.testing.allocator, 2);
    defer ch.deinit();

    // Spawn a thread that waits on take, should return null after close
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(channel: *Channel) void {
            // This should block until close
            const msg = channel.take();
            // After close, should be null
            std.debug.assert(msg == null);
        }
    }.run, .{&ch});

    // Give thread time to start waiting
    @import("time_util.zig").sleepMs(10);

    ch.close();
    thread.join();

    // Put should fail after close
    const headers = std.StringHashMap([]u8).init(std.testing.allocator);
    const body = try std.testing.allocator.dupe(u8, "test");
    const result = ch.put(.{ .body = body, .headers = headers });
    if (result) |_| {
        unreachable;
    } else |err| {
        try std.testing.expectEqual(error.ChannelClosed, err);
    }
    // Clean up the message we tried to put
    std.testing.allocator.free(body);
    var headers_mut = headers;
    headers_mut.deinit();
}

test "messageFromExchange copies data" {
    var ex = Exchange.init(std.testing.allocator);
    defer ex.deinit();
    try ex.setBody("hello");
    try ex.putHeader("key", "val");

    var msg = try messageFromExchange(std.testing.allocator, &ex);
    defer msg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("hello", msg.body);
    try std.testing.expectEqualStrings("val", msg.headers.get("key").?);
}

test "exchangeFromMessage transfers ownership" {
    const alloc = std.testing.allocator;

    var headers = std.StringHashMap([]u8).init(alloc);
    const hval = try alloc.dupe(u8, "value");
    try headers.put("hdr", hval);

    var msg = ChannelMessage{
        .body = try alloc.dupe(u8, "body-data"),
        .headers = headers,
    };
    defer msg.deinit(alloc);

    var ex = try exchangeFromMessage(alloc, &msg);
    defer ex.deinit();

    try std.testing.expectEqualStrings("body-data", ex.body);
    try std.testing.expectEqualStrings("value", ex.getHeader("hdr").?);
}
