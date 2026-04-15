---
status: draft
date: 2025-04-15
decision-makers:
  - "@gossi"
consulted: []
informed: []
---

# Stream Semantics: Map Commanded Streams to ESDB Subjects

Related ADRs:

- [0001 - Event Semantics](./0001-event-semantics.md)

## Context and Problem Statement

Commanded uses stream_uuid to identify event streams, while EventSourcingDB (ESDB) uses subjects. The adapter must translate between Commanded's stream concept and ESDB's subject-based organization while maintaining namespace separation for multi-tenant applications.

## Decision Drivers

- Commanded stream_uuid format: `"#{identity_prefix}#{aggregate_uuid}"`
- ESDB subjects require `/` prefix for hierarchical organization
- Stream prefix must be configurable per commanded app
- `:all` subscription must read all events under the app namespace
- Natural hierarchy enables ESDB queries at any level

## Considered Options

- Use stream_uuid directly as subject (no prefix)
- Prepend stream_prefix (sanitize the slashes)

## Decision Outcome

Chosen option: `"/#{stream_prefix}/#{stream_uuid}"`, because leading slash is
required for ESDB hierarchical subjects and consistent prefix handling.

The `StreamMapper` concatenates stream components together. The mapper
ensures the `/` are correctly places to format the proper subject for ESDB.

### Mapping Formula

```
Commanded stream_uuid: "#{identity_prefix}#{aggregate_uuid}"
ESDB subject: "/#{stream_prefix}/#{stream_uuid}"
```

### Component Sources

| Component | Source | Example |
| --------- | ------ | ------- |
| `aggregate_uuid` | Command struct field, defined by `identity:` in router | `"ACC123"` |
| `identity_prefix` | Router's `identify` macro or dispatch `prefix:` option | `"bank-account/"`, `"bank-account"` |
| `stream_prefix` | Adapter config `stream_prefix:` | `"myapp/"`, `"myapp"` |
| `stream_uuid` | `#{identity_prefix}/#{aggregate_uuid}` | `"bank-account/ACC123"` |
| `ESDB subject` | `"/#{stream_prefix}/#{stream_uuid}"` | `"/myapp/bank-account/ACC123"` |

### Example

With `stream_prefix: "myapp/"` and `identity_prefix: "bank-account/"`:

- aggregate_uuid: `ACC123`
- identity_prefix: `bank-account/`
- stream_uuid: `bank-account/ACC123`
- ESDB subject: `/myapp/bank-account/ACC123`

### The `:all` Stream

Commanded's `:all` (subscribe to every event) maps to the app-scoped root:

```
:all → /#{stream_prefix}
```

With `stream_prefix: "myapp/"`, subscribing to `:all` reads from `"/myapp/"` with `recursive: true`, returning every event under that prefix.

### Why Slashes in Prefixes

Using `/` as separators in `stream_prefix` and `identity_prefix` creates a natural hierarchy in ESDB:

```
/myapp/
  /bank-account/
    /ACC123
    /DEF456
  /order/
    /ORD001
```

This enables ESDB queries at any level (all bank accounts, all orders, all events for the app).

### stream_prefix Purpose

The `stream_prefix` is a **namespace separator** that isolates events from
different Commanded applications. An application can consist of multiple
commanded applications. For example each commanded application can be it's own subdomain.

#### Configuration

The adapter is configured with a `stream_prefix`:

```elixir
# config/*.exs
config :your_app, YourSubdomainCommandedApplication,
  stream_prefix: "your-subdomain/"
```

#### How Consumers Use It

Consumers of the event store don't need to know about `stream_prefix` directly. The adapter handles the translation:

| Consumer Action | What Happens |
| --------------- | ------------- |
| `append_to_stream` | Adapter prepends `stream_prefix` when writing to ESDB |
| `subscribe` | Adapter subscribes to prefixed subjects |
| `:all` subscription | Adapter reads from `#{stream_prefix}/` with `recursive: true` |
| Read events | Adapter strips `stream_prefix` from ESDB subjects |

#### Consumer Code Example

```elixir
# Commanded router defines identity prefix
defmodule BankRouter do
  use Commanded.Commands.Router

  identify :account, prefix: "bank-account/"

  dispatch OpenAccount, to: BankAccount, aggregate: :account
end

# Writing events
Commanded.Commands.dispatch(BankRouter, %OpenAccount{account_number: "ACC123"})
# → Writes to ESDB subject: /your-subdomain/bank-account/ACC123

# Subscribing to all events for an aggregate
Commanded.EventStore.subscribe("bank-account/ACC123")
# → Reads from ESDB subject: /your-subdomain/bank-account/ACC123

# Subscribing to all events in the app
Commanded.EventStore.subscribe(:all)
# → Reads from ESDB subject: /your-subdomain/ with recursive: true
```

## Consequences

### Good

- Namespace separation per app prevents data leakage
- Hierarchical subjects enable efficient querying at any level
- `:all` subscription works with recursive reading
- Natural path-like structure matches ESDB conventions

### Bad

- Requires configuration of stream_prefix for each app
- Identity prefix must be consistently applied in router

### Neutral

- Aggregate UUID comes from command struct (router configuration)
- Stream prefix is adapter configuration
