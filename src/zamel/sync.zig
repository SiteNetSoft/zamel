const std = @import("std");
const linux = std.os.linux;
const assert = std.debug.assert;

/// Futex-based mutex (replacement for removed std.Thread.Mutex).
pub const Mutex = struct {
    state: std.atomic.Value(u32) = .init(0),

    const UNLOCKED: u32 = 0;
    const LOCKED: u32 = 1;
    const CONTENDED: u32 = 2;

    pub fn lock(self: *Mutex) void {
        if (self.state.cmpxchgWeak(UNLOCKED, LOCKED, .acquire, .monotonic) != null) {
            self.lockSlow();
        }
    }

    fn lockSlow(self: *Mutex) void {
        while (true) {
            // Try to transition to contended state
            const prev = self.state.swap(CONTENDED, .acquire);
            if (prev == UNLOCKED) return;

            // Wait on futex
            _ = linux.futex_4arg(
                @ptrCast(&self.state.raw),
                .{ .cmd = .WAIT, .private = true },
                CONTENDED,
                null,
            );
        }
    }

    pub fn unlock(self: *Mutex) void {
        const prev = self.state.swap(UNLOCKED, .release);
        assert(prev != UNLOCKED);
        if (prev == CONTENDED) {
            // Wake one waiter
            _ = linux.futex_3arg(
                @ptrCast(&self.state.raw),
                .{ .cmd = .WAKE, .private = true },
                1,
            );
        }
    }
};

/// Futex-based condition variable (replacement for removed std.Thread.Condition).
pub const Condition = struct {
    seq: std.atomic.Value(u32) = .init(0),

    pub fn wait(self: *Condition, mutex: *Mutex) void {
        const seq = self.seq.load(.acquire);
        mutex.unlock();
        // Wait until seq changes
        _ = linux.futex_4arg(
            @ptrCast(&self.seq.raw),
            .{ .cmd = .WAIT, .private = true },
            seq,
            null,
        );
        mutex.lock();
    }

    pub fn signal(self: *Condition) void {
        _ = self.seq.fetchAdd(1, .release);
        _ = linux.futex_3arg(
            @ptrCast(&self.seq.raw),
            .{ .cmd = .WAKE, .private = true },
            1,
        );
    }

    pub fn broadcast(self: *Condition) void {
        _ = self.seq.fetchAdd(1, .release);
        _ = linux.futex_3arg(
            @ptrCast(&self.seq.raw),
            .{ .cmd = .WAKE, .private = true },
            std.math.maxInt(i32),
        );
    }
};

/// One-shot event flag (replacement for removed std.Thread.ResetEvent).
pub const Event = struct {
    state: std.atomic.Value(u32) = .init(0),

    const UNSET: u32 = 0;
    const SET: u32 = 1;

    pub fn set(self: *Event) void {
        self.state.store(SET, .release);
        // Wake all waiters
        _ = linux.futex_3arg(
            @ptrCast(&self.state.raw),
            .{ .cmd = .WAKE, .private = true },
            std.math.maxInt(i32),
        );
    }

    pub fn wait(self: *Event) void {
        while (self.state.load(.acquire) != SET) {
            _ = linux.futex_4arg(
                @ptrCast(&self.state.raw),
                .{ .cmd = .WAIT, .private = true },
                UNSET,
                null,
            );
        }
    }

    /// Wait with a timeout in nanoseconds. Returns error.Timeout if the
    /// event is not set within the given duration.
    pub fn timedWait(self: *Event, timeout_ns: u64) error{Timeout}!void {
        if (self.state.load(.acquire) == SET) return;

        const ts = linux.timespec{
            .sec = @intCast(timeout_ns / std.time.ns_per_s),
            .nsec = @intCast(timeout_ns % std.time.ns_per_s),
        };

        _ = linux.futex_4arg(
            @ptrCast(&self.state.raw),
            .{ .cmd = .WAIT, .private = true },
            UNSET,
            &ts,
        );

        if (self.state.load(.acquire) == SET) return;
        return error.Timeout;
    }

    pub fn isSet(self: *const Event) bool {
        return self.state.load(.acquire) == SET;
    }
};
