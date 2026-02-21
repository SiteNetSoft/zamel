const std = @import("std");
const posix = std.posix;

pub fn sleepMs(ms: u64) void {
    const ns = ms * std.time.ns_per_ms;
    var req = posix.timespec{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
    while (true) {
        switch (posix.errno(posix.system.nanosleep(&req, &req))) {
            .SUCCESS => return,
            .INTR => continue,
            else => unreachable,
        }
    }
}
