const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const EndpointRef = @import("endpoint.zig").EndpointRef;
const Splitter = @import("splitter.zig").Splitter;
const RecipientResolver = @import("recipient_resolver.zig").RecipientResolver;

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

    // Message splitting: splits body by kind, runs sub-route per part
    Split: struct {
        kind: SplitKind = .{ .scalar = '\n' },
        route: RouteId,
    },

    // Aggregation: runs sub-route, then joins all resulting bodies
    Aggregate: struct {
        separator: []const u8 = "\n",
        route: RouteId,
    },

    // Fire-and-forget copy to a side endpoint:
    WireTap: EndpointRef,

    // Dynamic routing to endpoints resolved at runtime:
    RecipientList: struct {
        resolver: RecipientResolver,
    },

    // Rate limiting: introduces a delay between messages:
    Throttle: struct {
        interval_ms: u32,
    },

    // Idempotent consumer: dedup by header key via StateStore
    IdempotentConsumer: struct {
        key_header: []const u8,
        route: RouteId,
    },

    // Content enricher: call endpoint, optionally merge result
    Enrich: struct {
        endpoint: EndpointRef,
        merge: ?Processor = null,
    },

    // Policies (wrapper around a child route):
    Policy: struct {
        kind: PolicyKind,
        route: RouteId,
    },
};

pub const SplitKind = union(enum) {
    scalar: u8,
    sequence: []const u8,
    custom: Splitter,
};

pub const PolicyKind = union(enum) {
    Retry: struct { max: u32, backoff_ms: u32 = 0 },
    Timeout: struct { ms: u32 },
    DeadLetter: struct { endpoint: EndpointRef },
    CircuitBreaker: struct { failure_threshold: u32, reset_ms: u32 },
};
