---
status: draft
date: 2025-04-15
decision-makers:
  - "@gossi"
consulted: []
informed: []
---

# Define stream_version semantics for ESDB adapter

## Context and Problem Statement

EventSourcingDB (ESDB) differs from EventStoreDB in how it numbers events. ESDB uses a global counter per subject (`event.id`), while Commanded expects `stream_version` to be a per-stream sequential number (1, 2, 3...). Additionally, Commanded tests verify different behaviors for single stream vs `:all` subscriptions, and transient vs persistent subscriptions. This ADR documents the semantic meaning of stream_version in our ESDB adapter.

## Decision Drivers

- Commanded's subscription_test_case.ex verifies that stream_version increments per stream independently
- For single stream subscriptions: stream_version resets at 1 for each new stream
- For :all subscriptions: each stream tracks its own stream_version independently
- Persistent subscriptions checkpoint stream_version for resumption
- Transient subscriptions have no checkpoint but still receive stream_version
- delete_subscription resets the checkpoint allowing fresh start from :origin

## Considered Options

- Use ESDB's global event.id as stream_version
- Compute stream_version internally in adapter modules

## Decision Outcome

Chosen option: "Compute stream_version internally in adapter modules", because ESDB does not provide per-stream versioning natively. The adapter must track stream_version per stream in memory and persist checkpoints for resumption.

### stream_version Storage is Separate

**CRITICAL**: Transient and persistent subscriptions cannot share the same stream_version storage because persistent subscriptions can reset their position via `delete_subscription()`, which would corrupt transient subscriptions if they shared storage.

| Storage | Transient | Persistent |
|---------|-----------|-----------|
| **stream_version** | In-memory per Subscription process | In-memory per Subscription process |
| **Checkpoint** | None (ephemeral) | Stored in CheckpointStore |
| **Reset Capability** | No (dies with process) | Yes (via delete_subscription) |

### stream_version By Subscription Type

| Subscription | stream_version | event_number | Checkpoint |
|--------------|---------------|--------------|------------|
| **Transient** (`subscribe/2`) | Per-stream position (in-memory) | Global position | None |
| **Persistent** (`subscribe_to/6`) | Per-stream position (in-memory) | Global position | Stored on ack via event_number |

### stream_version By Stream Scope

| Stream Scope | What Gets Tracked | Starting Value |
|-------------|-----------------|---------------|
| **Single Stream** | stream_version | 1 per stream |
| **:all Streams** | stream_version per stream + event_number global | 1 per stream / 1 global |

### Function Effects on stream_version

| Function | stream_version Effect | event_number Effect |
|----------|---------------------|-------------------|
| subscribe/2 | Tracks in-memory, dies with process | Tracks in-memory, dies with process |
| subscribe_to/6 | Tracks in-memory + checkpoint | Used for resume on :all |
| ack | (persistent) In-memory tracked, not stored | Stored as checkpoint |
| delete_subscription | N/A | RESETS to 1 |

## Consequences

### Good

- Matches Commanded expectations for stream_version
- Supports both single stream and :all subscriptions
- Enables subscription resumption via checkpoints

### Bad

- Adapter must track state in memory (stream_versions map)
- Requires checkpoint storage for persistent subscriptions

### Neutral

- ESBD's global event.id is used as event_number without additional storage

## More Information

### Runtime Behavior Example

```
Single Stream (:all subscription):
─────────────────────────────────────────────────
stream1: event#1 → stream_version: 1, event_number: 1
         event#2 → stream_version: 2, event_number: 2
         
stream2: event#1 → stream_version: 1, event_number: 3  ← stream_version resets!
         event#2 → stream_version: 2, event_number: 4

After delete_subscription(:all, "sub"):
──────────────────────────────────────��──────────
Back to receiving from event_number: 1 (global reset)
```

### Implementation Notes

- **EventObserver** publishes events from ESDB to transient subscribers
  - Tracks stream_versions locally in its State struct for publishing
  - No checkpoint storage (transient subscribers are ephemeral)
- **Subscription** (persistent) manages its own stream_versions
  - Tracks stream_versions locally in its State struct (in-memory)
  - On ack: stores checkpoint via CheckpointStore
  - Each persistent subscription has independent stream_version tracking
- **CheckpointStore** stores persistent subscription checkpoints
  - Key: `{subscription_name}` (ONE checkpoint per subscription, not per stream!)
  - Value: last seen event_number (global)
  - delete_subscription removes the checkpoint
- **Transient** subscriptions have no checkpoint
  - They subscribe via Registry/pubsub, not Subscription GenServer
  - When the subscriber process dies, stream_version tracking is lost

### Checkpoint and ack() Details

The checkpoint is **a single value per subscription**, not per stream. This applies to both single stream and :all subscriptions.

| Aspect | Single Stream | :all Streams |
|--------|---------------|--------------|
| Checkpoint Key | `{subscription_name}` | `{subscription_name}` |
| Checkpoint Value | `event_number` (global) | `event_number` (global) |
| stream_version tracked | YES (in-memory) | YES (in-memory) |
| Checkpoint stored on ack | YES (event_number) | YES (event_number) |

#### How ack() Works

```
Subscriber receives event
  → Sends ack_event(adapter_meta, subscription_pid, event)
  → Adapter uses event.event_number (not stream_version!)
  → CheckpointStore.put(subscription_name, event_number)
```

The adapter receives the entire `RecordedEvent` struct on ack:
```elixir
%RecordedEvent{
  event_number: 1,    # global - USED FOR CHECKPOINT
  stream_version: 1, # per-stream - NOT stored in checkpoint
  stream_id: "stream1"
}
```

### Why Separate Storage is Required

```
Scenario: Two subscribers to same stream
────────────────────────────────────────────────────────
Subscriber A (transient): starts at stream_version: 1
Subscriber B (persistent): starts at stream_version: 1, checkpoints

Events flow:
  Event 1 → A: sees sv=1, B: sees sv=1 (both in-memory)
  Event 2 → A: sees sv=2, B: sees sv=2
  Event 3 → A: sees sv=3, B: sees sv=3, acks -> checkpoint saved

B calls delete_subscription():
  → checkpoint deleted
  → next B subscription starts from :origin (sv=1)
  → A is unaffected (in-process memory)

If they shared storage:
  → delete_subscription() would reset A's position too!
  → A would miss events 4+
```
