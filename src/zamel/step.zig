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

    // Message splitting: splits body by delimiter, runs sub-route per part
    Split: struct {
        delimiter: u8 = '\n',
        route: RouteId,
    },

    // Aggregation: runs sub-route, then joins all resulting bodies
    Aggregate: struct {
        separator: []const u8 = "\n",
        route: RouteId,
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
