---
status: draft
date: 2026-04-20
decision-makers:
  - "@gossi"
consulted: []
informed: []
---

# Subscriptions for Commanded

Related ADRs:

- [0001 - Event Semantics](./0001-event-semantics.md)
- [0002 - Stream Semantics](./0002-stream-semantics.md)
- [0003 - Stream Version Semantics](./0003-stream-version-semantics.md)

## Context and Problem Statement

The ESDB adapter must implement Commanded's subscription functionality to enable event handlers to receive persisted events. This ADR documents the assessment of Commanded's subscription model and identifies what needs to be implemented for the ESDB adapter.

## Decision Drivers

Subscriptions in Commanded are the mechanism by which event handlers receive
persisted events from the event store. Commanded provides two distinct
subscription types: **transient** and **persistent**, each serving different use
cases in a CQRS/ES architecture.

- Commanded provides two subscription types: transient (`subscribe/2`) and persistent (`subscribe_to/6`)
- Commanded requires checkpointing for persistent subscriptions to enable resumption
- Commanded supports concurrent subscribers with configurable distribution
- Commanded provides strong consistency tracking via the Subscriptions module

### 1. Subscription Types

| Type | Function | Characteristics |
|------|----------|-----------------|
| **Transient** | `subscribe/2` | Fire-and-forget subscription without persistence; events delivered in real-time |
| **Persistent** | `subscribe_to/5-6` | Stateful subscription that remembers position; supports resumption after restarts |

### 2. Transient Subscriptions (`subscribe/2`)

- **Purpose**: Real-time notifications for events appended to a stream
- **Registration**: Uses EventStore adapter's internal mechanism
- **Distribution**: Publisher-subscriber pattern; sends `{:events, [recorded_events]}` messages
- **Acknowledgment**: Not required (fire-and-forget)
- **Persistence**: None - subscription state is not saved

### 3. Persistent Subscriptions (`subscribe_to/5-6`)

- **Purpose**: Reliable event processing with at-least-once delivery guarantees
- **State Management**: Tracks last acknowledged event position for resumption
- **Start Positions**:
  - `:origin` - Start from beginning, delete any existing checkpoint
  - `:current` - Resume from last checkpoint
  - `integer` - Start from specific event number
- **Acknowledgment**: Required via `ack_event/3` - updates checkpoint

### 4. Consistency Model

| Mode | Behavior | Use Case |
| --- | --- | --- |
| `:eventual` | Command returns immediately after event persistence | High throughput, eventual consistency |
| `:strong` | Waits for all handlers to process event via Subscriptions module |Critical consistency requirements |

The strong consistency is implemented via the Commanded.Subscriptions module which:

- Tracks handler registrations via Subscriptions.Registry
- Uses ETS table to track which handlers have processed which events
- Provides wait_for/4 function to synchronize

### 5. Concurrency Support

- **Configuration**: Via `subscribe_to` options with `:concurrency` key
- **Default**: 1 (single subscriber)
- **Distribution**: Round-robin event distribution among subscribers
- **Optional**: `partition_by` function for custom routing
- **Monitoring**: Subscriber lifecycle

### 6. Message Protocol

Subscribers receive these messages:

- `{:subscribed, subscription}` - Confirmation of subscription
- `{:events, recorded_events}` - Batch of recorded events

Subscribers send:

- `Commanded.EventStore.ack_event/3` function call - Acknowledge successful processing

### 7. Registry and Tracking

The `Subscriptions.Registry` module is used to:

- Register event handlers (`register/4`)
- List all registered handlers (`all/1`)

This is separate from the event store subscription - it's Commanded's internal tracking for strong consistency enforcement.

### 8. Subscription Management

- `unsubscribe/2` - Stop subscription
- `delete_subscription/3` - Remove subscription and checkpoint

### 9. Expected Error Messages

Commanded expects specific error atoms from the adapter. The adapter MUST return these error tuples for the corresponding operations:

| Function | Error Atom | Description |
|----------|------------|-------------|
| `subscribe_to/6` | `{:error, :subscription_already_exists}` | Subscription with this name already exists |
| `subscribe_to/6` | `{:error, :too_many_subscribers}` | Concurrency limit reached |
| `delete_subscription/3` | `{:error, :subscription_not_found}` | Subscription does not exist |

Generic Errors are in the format `{:error, term}`

#### Concurrency Error Handling

When a subscription allows multiple subscribers (`:concurrency > 1`):

1. First subscriber creates the subscription successfully
2. Second subscriber joins the existing subscription (if under limit)
3. Third+ subscriber (at limit) receives `{:error, :too_many_subscribers}`
4. Duplicate subscription name with `:concurrency == 1` receives `{:error, :subscription_already_exists}`

## Considered Options

- Full implementation of both transient and persistent subscriptions
- Partial implementation (transient only)
- Defer implementation to later phase

## Decision Outcome

Chosen option: "Full implementation of both transient and persistent
subscriptions", because Commanded's event handlers depend on persistent
subscriptions for reliable event processing, and transient subscriptions are
needed for real-time notifications.

- the `:partition` tag will not be supported

## Consequences

### Good

- Enables reliable event processing for command/event handlers
- Supports resumption after application restarts
- Provides at-least-once delivery via checkpointing
- Enables concurrent event processing

### Bad

- Requires CheckpointStore implementation for persistent subscriptions
- In-memory stream_version tracking needed per subscription
- Complex GenServer lifecycle management

### Neutral

- Transient and persistent subscriptions require separate storage (per ADR 0003)

## Open Implementation Items

- [INVESTIGATE: Confirm CheckpointStore storage mechanism (ETS, Redis, etc.)]
- [INVESTIGATE: Review EventSourcingDB observe_events API for real-time streaming]
- [INVESTIGATE: Determine partition_by implementation approach]
- [IMPLEMENT: Transient subscription via Registry + PubSub]
- [IMPLEMENT: Persistent subscription GenServer]
- [IMPLEMENT: CheckpointStore for checkpoint persistence]
- [IMPLEMENT: SubscriptionSupervisor for lifecycle management]
- [IMPLEMENT: ack_event to checkpoint mapping]
- [IMPLEMENT: delete_subscription to checkpoint deletion]
