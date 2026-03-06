const std = @import("std");
const Consumer = @import("consumer.zig").Consumer;
const Exchange = @import("exchange.zig").Exchange;
const RoutePlan = @import("plan.zig").RoutePlan;
const SyncExecutor = @import("executor_sync.zig").SyncExecutor;
const Services = @import("services.zig").Services;
const time_util = @import("time_util.zig");

/// A cron field: either a wildcard, a single value, a step (*/N), or a list of values.
pub const CronField = union(enum) {
    wildcard,
    value: u8,
    step: u8, // */N
    list: []const u8,
};

/// Parsed cron expression: minute hour day-of-month month day-of-week
pub const CronExpr = struct {
    minute: CronField = .wildcard,
    hour: CronField = .wildcard,
    day_of_month: CronField = .wildcard,
    month: CronField = .wildcard,
    day_of_week: CronField = .wildcard,

    /// Check if the given broken-down time matches this expression.
    pub fn matches(self: CronExpr, minute: u8, hour: u8, day: u8, month: u8, dow: u8) bool {
        return fieldMatches(self.minute, minute) and
            fieldMatches(self.hour, hour) and
            fieldMatches(self.day_of_month, day) and
            fieldMatches(self.month, month) and
            fieldMatches(self.day_of_week, dow);
    }

    fn fieldMatches(field: CronField, val: u8) bool {
        switch (field) {
            .wildcard => return true,
            .value => |v| return v == val,
            .step => |s| return if (s == 0) true else (val % s == 0),
            .list => |l| {
                for (l) |v| {
                    if (v == val) return true;
                }
                return false;
            },
        }
    }
};

/// Parse a cron expression string.
/// Supports: * (wildcard), N (value), */N (step), N,N,N (list)
/// Format: "minute hour day_of_month month day_of_week"
pub fn parseCron(expr: []const u8) !CronExpr {
    var result = CronExpr{};
    var field_idx: u8 = 0;
    var it = std.mem.splitScalar(u8, expr, ' ');
    while (it.next()) |field_str| {
        if (field_str.len == 0) continue;
        const f = try parseField(field_str);
        switch (field_idx) {
            0 => result.minute = f,
            1 => result.hour = f,
            2 => result.day_of_month = f,
            3 => result.month = f,
            4 => result.day_of_week = f,
            else => return error.TooManyCronFields,
        }
        field_idx += 1;
    }
    if (field_idx < 5) return error.TooFewCronFields;
    return result;
}

fn parseField(field: []const u8) !CronField {
    if (std.mem.eql(u8, field, "*")) return .wildcard;

    // */N step
    if (field.len > 2 and field[0] == '*' and field[1] == '/') {
        const n = std.fmt.parseUnsigned(u8, field[2..], 10) catch return error.InvalidCronField;
        return .{ .step = n };
    }

    // List: contains commas
    if (std.mem.indexOfScalar(u8, field, ',') != null) {
        // Count values
        var count: usize = 0;
        var cit = std.mem.splitScalar(u8, field, ',');
        while (cit.next()) |_| count += 1;

        // We store as a static-ish slice. For simplicity in this embedded context,
        // use a comptime-friendly approach: store up to 12 values inline.
        var vals: [12]u8 = undefined;
        var vi: usize = 0;
        var cit2 = std.mem.splitScalar(u8, field, ',');
        while (cit2.next()) |v| {
            if (vi >= 12) return error.CronListTooLong;
            vals[vi] = std.fmt.parseUnsigned(u8, v, 10) catch return error.InvalidCronField;
            vi += 1;
        }
        // Return slice into static storage — caller must copy if needed
        return .{ .list = vals[0..vi] };
    }

    // Single value
    const v = std.fmt.parseUnsigned(u8, field, 10) catch return error.InvalidCronField;
    return .{ .value = v };
}

const CronCtx = struct {
    expr: CronExpr,
    max_fires: u64, // 0 = unlimited
};

pub fn cronConsumer(allocator: std.mem.Allocator, expr: CronExpr, max_fires: u64) !Consumer {
    const p = try allocator.create(CronCtx);
    p.* = .{ .expr = expr, .max_fires = max_fires };
    return .{
        .ctx = p,
        .startFn = cronStart,
        .deinitFn = cronDeinit,
    };
}

fn cronDeinit(ctx: ?*anyopaque, allocator: std.mem.Allocator) void {
    const p: *CronCtx = @ptrCast(@alignCast(ctx.?));
    allocator.destroy(p);
}

fn cronStart(ctx: ?*anyopaque, allocator: std.mem.Allocator, services: Services, plan: *const RoutePlan, route_id: u32) !void {
    const cfg: *const CronCtx = @ptrCast(@alignCast(ctx.?));

    var exec = SyncExecutor.init(services);
    defer exec.deinit();

    var fires: u64 = 0;
    var last_minute: ?u8 = null;

    while (cfg.max_fires == 0 or fires < cfg.max_fires) {
        const now_ms = services.clock.nowMillis();
        const now = decompose(now_ms);

        // Only fire once per minute and if expression matches
        const cur_minute = now.minute;
        if (last_minute == null or last_minute.? != cur_minute) {
            if (cfg.expr.matches(now.minute, now.hour, now.day, now.month, now.dow)) {
                last_minute = cur_minute;
                fires += 1;

                var ex = Exchange.init(allocator);
                defer ex.deinit();

                var fire_buf: [32]u8 = undefined;
                const fire_str = try std.fmt.bufPrint(&fire_buf, "{d}", .{fires});
                try ex.putHeader("CamelCronFire", fire_str);

                var ts_buf: [32]u8 = undefined;
                const ts_str = try std.fmt.bufPrint(&ts_buf, "{d}", .{now_ms});
                try ex.putHeader("CamelCronTimestamp", ts_str);
                try ex.setBody("cron");

                try exec.run(plan, @intCast(route_id), &ex);
            }
        }

        // Sleep until next minute boundary (or 1 second for checking)
        time_util.sleepMs(1000);
    }
}

const TimeComponents = struct {
    minute: u8,
    hour: u8,
    day: u8,
    month: u8,
    dow: u8,
};

/// Decompose milliseconds since epoch into time components.
fn decompose(ms: u64) TimeComponents {
    const secs = ms / 1000;
    const days_since_epoch = secs / 86400;
    const time_of_day = secs % 86400;

    const hour: u8 = @intCast(time_of_day / 3600);
    const minute: u8 = @intCast((time_of_day % 3600) / 60);

    // Day of week: Jan 1 1970 was Thursday (4)
    const dow: u8 = @intCast((days_since_epoch + 4) % 7); // 0=Sun

    // Convert days to year/month/day
    var remaining_days = days_since_epoch;
    var year: u32 = 1970;
    while (true) {
        const days_in_year: u64 = if (isLeapYear(year)) 366 else 365;
        if (remaining_days < days_in_year) break;
        remaining_days -= days_in_year;
        year += 1;
    }

    const leap = isLeapYear(year);
    const month_days = [12]u16{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: u8 = 1;
    for (month_days) |md| {
        if (remaining_days < md) break;
        remaining_days -= md;
        month += 1;
    }
    const day: u8 = @intCast(remaining_days + 1);

    return .{ .minute = minute, .hour = hour, .day = day, .month = month, .dow = dow };
}

fn isLeapYear(year: u32) bool {
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
}

// -------- tests --------

test "parseCron: every minute" {
    const expr = try parseCron("* * * * *");
    try std.testing.expect(expr.matches(0, 0, 1, 1, 0));
    try std.testing.expect(expr.matches(30, 12, 15, 6, 3));
}

test "parseCron: specific time" {
    const expr = try parseCron("30 12 * * *");
    try std.testing.expect(expr.matches(30, 12, 1, 1, 0));
    try std.testing.expect(!expr.matches(0, 12, 1, 1, 0));
    try std.testing.expect(!expr.matches(30, 13, 1, 1, 0));
}

test "parseCron: step expression" {
    const expr = try parseCron("*/5 * * * *");
    try std.testing.expect(expr.matches(0, 0, 1, 1, 0));
    try std.testing.expect(expr.matches(5, 0, 1, 1, 0));
    try std.testing.expect(expr.matches(10, 0, 1, 1, 0));
    try std.testing.expect(!expr.matches(3, 0, 1, 1, 0));
}

test "parseCron: too few fields" {
    try std.testing.expectError(error.TooFewCronFields, parseCron("* *"));
}

test "decompose: epoch" {
    const t = decompose(0);
    try std.testing.expectEqual(@as(u8, 0), t.minute);
    try std.testing.expectEqual(@as(u8, 0), t.hour);
    try std.testing.expectEqual(@as(u8, 1), t.day);
    try std.testing.expectEqual(@as(u8, 1), t.month);
    try std.testing.expectEqual(@as(u8, 4), t.dow); // Thursday
}

test "decompose: known date" {
    // 2024-01-15 14:30:00 UTC = 1705329000 seconds
    const ms: u64 = 1705329000 * 1000;
    const t = decompose(ms);
    try std.testing.expectEqual(@as(u8, 30), t.minute);
    try std.testing.expectEqual(@as(u8, 14), t.hour);
    try std.testing.expectEqual(@as(u8, 15), t.day);
    try std.testing.expectEqual(@as(u8, 1), t.month);
}
