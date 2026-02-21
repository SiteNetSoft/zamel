const std = @import("std");
const Consumer = @import("consumer.zig").Consumer;
const Exchange = @import("exchange.zig").Exchange;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;

pub const TimerCtx = struct {
    interval_ms: u64,
    repeat: u64,
};

pub fn timer(allocator: std.mem.Allocator, interval_ms: u64, repeat: u64) !Consumer {
    const p = try allocator.create(TimerCtx);
    p.* = .{ .interval_ms = interval_ms, .repeat = repeat };

    return .{
        .ctx = p,
        .startFn = timerStart,
        .deinitFn = timerDeinit,
    };
}

fn timerDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const p: *TimerCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
}

fn timerStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
    const cfg: *const TimerCtx = @ptrCast(@alignCast(ctx.?));

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    var i: u64 = 0;
    while (cfg.repeat == 0 or i < cfg.repeat) : (i += 1) {
        var ex = Exchange.init(allocator);
        defer ex.deinit();

        var tick_buf: [32]u8 = undefined;
        const tick_str = try std.fmt.bufPrint(&tick_buf, "{d}", .{i});
        try ex.putHeader("tick", tick_str);

        var body_buf: [64]u8 = undefined;
        const body_str = try std.fmt.bufPrint(&body_buf, "tick {d}", .{i});
        try ex.setBody(body_str);

        if (i % 2 == 0)
            try ex.putHeader("type", "A")
        else
            try ex.putHeader("type", "B");

        try exec.run(plan, @intCast(route_id), &ex);

        @import("time_util.zig").sleepMs(cfg.interval_ms);
    }
}

// -------- file reader consumer --------
// Reads a file and creates one Exchange per line.
// URI: "file:path/to/file"

const FileReaderCtx = struct {
    path: []const u8,
};

pub fn fileReader(allocator: std.mem.Allocator, path: []const u8) !Consumer {
    const p = try allocator.create(FileReaderCtx);
    p.* = .{ .path = path };
    return .{
        .ctx = p,
        .startFn = fileReaderStart,
        .deinitFn = fileReaderDeinit,
    };
}

fn fileReaderDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const p: *FileReaderCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
}

fn fileReaderStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
    const cfg: *const FileReaderCtx = @ptrCast(@alignCast(ctx.?));
    const posix = std.posix;

    // Read entire file via posix
    const fd = try posix.openat(posix.AT.FDCWD, cfg.path, .{ .ACCMODE = .RDONLY }, 0);
    defer posix.close(fd);

    var content_list: std.ArrayList(u8) = .empty;
    defer content_list.deinit(allocator);
    var read_buf: [4096]u8 = undefined;
    while (true) {
        const rc = posix.system.read(fd, &read_buf, read_buf.len);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                if (rc == 0) break;
                try content_list.appendSlice(allocator, read_buf[0..rc]);
            },
            .INTR => continue,
            else => break,
        }
    }
    const content = content_list.items;

    var exec = SyncExecutor.init(services);
    defer exec.deinit();
    var line_num: u64 = 0;

    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |line| {
        if (line.len == 0) continue;

        var ex = Exchange.init(allocator);
        defer ex.deinit();

        try ex.setBody(line);

        var num_buf: [16]u8 = undefined;
        const num_str = try std.fmt.bufPrint(&num_buf, "{d}", .{line_num});
        try ex.putHeader("CamelFileLineNumber", num_str);
        try ex.putHeader("CamelFileName", cfg.path);

        try exec.run(plan, @intCast(route_id), &ex);
        line_num += 1;
    }
}
