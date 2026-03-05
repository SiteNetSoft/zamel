const std = @import("std");
const Predicate = @import("predicate.zig").Predicate;
const Processor = @import("processor.zig").Processor;
const EndpointRef = @import("endpoint.zig").EndpointRef;
const Splitter = @import("splitter.zig").Splitter;
const RecipientResolver = @import("recipient_resolver.zig").RecipientResolver;
const AggregationStrategy = @import("aggregation.zig").AggregationStrategy;

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
        strategy: ?AggregationStrategy = null,
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
        skip_duplicate: bool = true, // false = run route but set header
        eager: bool = true, // false = store key AFTER execution
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

    // Error handler: doTry/doCatch/doFinally
    DoTry: struct {
        try_route: RouteId,
        catch_route: ?RouteId = null,
        finally_route: ?RouteId = null,
    },

    // JSON marshal/unmarshal
    Marshal: struct { format: DataFormat },
    Unmarshal: struct { format: DataFormat },

    // Claim check: store/retrieve body via StateStore
    ClaimCheck: struct {
        action: ClaimCheckAction,
        key: []const u8,
    },

    // Delay: sleep for a specified number of milliseconds
    Delay: struct { ms: u32 },

    // Log: log a message template with interpolation
    Log: struct {
        message: []const u8,
        level: LogLevel = .info,
    },

    // Routing slip: route through URIs listed in a header
    RoutingSlip: struct {
        header: []const u8,
    },

    // Load balancer: distribute exchanges across multiple routes
    LoadBalancer: struct {
        strategy: LoadBalancerStrategy,
        routes: []const RouteId,
    },

    // Transform: like Process but semantically indicates body transformation
    Transform: Processor,

    // Dynamic router: route to endpoint URI stored in a header
    DynamicRouter: struct {
        header: []const u8,
    },
};

pub const SplitKind = union(enum) {
    scalar: u8,
    sequence: []const u8,
    custom: Splitter,
};

pub const DataFormat = enum { json };

pub const ClaimCheckAction = enum { store, retrieve };

pub const LogLevel = enum { debug, info, warn, err };

pub const LoadBalancerStrategy = enum { round_robin, random };

pub const PolicyKind = union(enum) {
    Retry: struct { max: u32, backoff_ms: u32 = 0 },
    Timeout: struct { ms: u32 },
    DeadLetter: struct {
        endpoint: EndpointRef,
        retries: u32 = 0,
        retry_backoff_ms: u32 = 0,
    },
    CircuitBreaker: struct {
        failure_threshold: u32,
        reset_ms: u32,
        success_threshold: u32 = 1,
    },
    Redelivery: struct {
        max_redeliveries: u32,
        initial_delay_ms: u32 = 0,
        multiplier: u32 = 1,
        max_delay_ms: u32 = 0,
        use_exponential: bool = false,
    },
};
