---
status: draft
date: 2026-04-06
---

# Use :current for checkpoint on subscription start

## Context and Problem Statement

When a persistent subscription starts after an Elixir application restart, the in-memory checkpoint (stored in ETS) is lost. We need to determine how to handle subscription startup when no checkpoint exists - should it replay all events from the beginning or only receive new events?

The challenge is that EventSourcingDB uses a global event counter, and without persistent checkpoint storage, we cannot determine which events were successfully processed before the restart.

## Decision Drivers

- Checkpoints are lost on Elixir VM restart (stored in-memory only)
- Deployments trigger restarts, causing checkpoint resets
- Commanded expects subscriptions to resume from the last acked position
- EventQL count approach cannot determine which events were processed before restart
- Full persistence in ESDB deferred to v2 for simplicity

## Considered Options

- **:origin** - Start from the beginning of the event stream, replaying all events
- **:current** - Only receive new events written after subscription creation
- Persistent checkpoint in ESDB (as events) - Full persistence for v2
- EventQL count as checkpoint - Query current event count to determine position

## Decision Outcome

Chosen option: ":current", because it provides a simple solution for v1 that
handles checkpoint loss gracefully. When no checkpoint exists after restart,
only new events are delivered. Events written during the restart window are lost
but this is an acceptable trade-off for the v1 draft.

We assume that commanded is the only event producer and when it's down, no other
events are produced, that we might miss.

### Consequences

- Good, because simple implementation - no complex recovery logic needed
- Good, because avoids duplicate event processing after restart
- Bad, because events written during restart window are not delivered (event loss)
- Neutral, because checkpoint is still tracked in-memory during runtime

## Pros and Cons of the Options

### :origin

- Bad, because would re-process all events after every restart causing duplicates
- Bad, because can lead to data corruption if events are idempotent but have side effects

### :current

- Good, because simple implementation
- Good, because avoids duplicate processing
- Bad, because loses events during restart window

### Persistent checkpoint in ESDB

- Good, because survives all failure scenarios
- Bad, because more complex implementation deferred to v2

### EventQL count

- Bad, because cannot determine which events were processed before restart
- Bad, because same result as :current but with extra query overhead
