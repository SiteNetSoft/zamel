# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Zamel is an integration framework written in Zig, inspired by Apache Camel. It provides a routing DSL for building message-processing pipelines that connect various systems via URI-addressable endpoints.

## Build Commands

```bash
zig build          # compile the project
zig build run      # build and run the demo (src/main.zig)
```

Requires Zig 0.16.0+. No external dependencies.

```bash
zig build test     # run unit + integration tests (31 tests)
```

## Architecture

The core abstraction is a **message exchange** flowing through a **route** (a sequence of steps), constructed via a **fluent builder DSL**, and executed by an **executor**.

### Key types (all in `src/zamel/`)

- **Exchange** (`exchange.zig`) — The message unit: a body (`[]u8`) plus string headers (`StringHashMap`). All memory is owned and freed via its allocator.

- **Step** (`step.zig`) — A tagged union representing one pipeline operation: `Process`, `Filter`, `To`, `Choice`, `Multicast`, `Split`, `Aggregate`, `Policy`. Choice/Multicast/Split contain nested `RouteId` references for sub-routes.

- **RoutePlan** (`plan.zig`) — A flat list of `Route`s (each a `[]Step`), indexed by `RouteId` (u32). Sub-routes from Choice/Multicast are stored as separate entries in the same plan.

- **RtBuilder** (`builder_rt.zig`) — Fluent runtime builder. Chains like `fromUri("timer:500") → choice() → when(...) → toUri("log:info") → endWhen() → end() → start()`. Uses an ArenaAllocator so the entire plan can be freed at once. Nested builders: `ChoiceBuilder`, `WhenBuilder`, `OtherwiseBuilder`, `PolicyBuilder`, `SplitBuilder`, `AggregateBuilder`.

- **SyncExecutor** (`executor_sync.zig`) — Walks a RoutePlan recursively and executes each Step. Handles Process, Filter, To, Choice, Multicast, Split, Aggregate, and Policy (Retry). Timeout/DeadLetter/CircuitBreaker policies return `error.NotImplemented`.

- **Registry** (`registry.zig`) — Resolves URI strings (scheme:rest) into Endpoints. Implements `log:{debug,info,warn,error}`, `timer:{ms}?repeat={n}`, and `file:{path}?append=true` schemes. Timer and file sources drive routes as Consumers.

- **Services** (`services.zig`) — Dependency injection container holding the allocator, Clock, and optional Scheduler/StateStore.

- **Predicate / Processor** (`predicate.zig`, `processor.zig`) — Callback interfaces using function pointer + `?*anyopaque` context. Factory functions for common predicates live in `predicates.zig` (e.g., `headerEq`, `bodyContains`).

- **Endpoint** (`endpoint.zig`) — Interface for creating Producers that can send an Exchange. `EndpointRef` is the tagged union that Steps reference.

### Data flow

```
Registry.startSource() creates Exchanges
  → SyncExecutor.run() walks the RoutePlan
    → each Step processes/filters/routes the Exchange
      → .To steps create a Producer from the Endpoint and send the Exchange
```

### Patterns

- All callback interfaces (Predicate, Processor, Endpoint, Producer) use the same `fn_ptr + ?*anyopaque` context pattern — no vtables or Zig interfaces.
- Memory for builder-created plans is arena-allocated; the arena is owned by `RtBuilder` and freed in `deinit()`.
- Predicate factory functions in `predicates.zig` return errors on allocation failure (proper error propagation).
- Shared utilities (e.g., `sleepMs` in `time_util.zig`) avoid code duplication across modules.
