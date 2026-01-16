const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const EndpointRef = @import("endpoint.zig").EndpointRef;

pub const RouteId = u32;

pub const ChoiceBranch = struct {
    when: Predicate,
    route: RouteId,
};

pub const Step = union(enum) {
    Process: Processor,
    Filter: Predicate,
    To: EndpointRef,

    // Nested routing:
    Choice: struct {
        branches: []const ChoiceBranch, // order matters
        otherwise: ?RouteId = null,
    },

    // Fan-out:
    Multicast: struct {
        routes: []const RouteId,
        parallel: bool = false, // executor can ignore until async exists
    },

    // Message splitting:
    Split: struct {
        splitter: Processor,   // MVP: splitter writes split list into headers or a store; later make a real Splitter type
        route: RouteId,
        parallel: bool = false,
    },

    // Fan-in / stateful (placeholder now, enables future):
    Aggregate: struct {
        correlation: Predicate, // placeholder; later replace with CorrelationKey extractor
        route: RouteId,
        completion_timeout_ms: u64 = 0,
    },

    // Policies (wrapper around a child route):
    Policy: struct {
        kind: PolicyKind,
        route: RouteId,
    },
};

pub const PolicyKind = union(enum) {
    Retry: struct { max: u32, backoff_ms: u32 = 0 },
    Timeout: struct { ms: u32 },
    DeadLetter: struct { endpoint: EndpointRef },
    CircuitBreaker: struct { failure_threshold: u32, reset_ms: u32 },
};
