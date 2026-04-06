# `commanded_adapter_eventsourcingdb` Architecture

## Subscriptions

```mermaid
graph TD
    CommandedApp["Commanded Application<br/>(Event Handlers, Process Managers)"]
    
    subgraph Adapter["EventSourcingDB Adapter"]
        Adapter[">>> Commanded.EventStore.Adapters.EventSourcingDB"]
        SubMan[">>> SubscriptionManager"]
        CheckpointStore[">>> CheckpointStore"]
        Observer[">>> ObserverProcess"]
        Transient[">>> TransientSubscriber"]
    end
    
    ESDB[">>> EventSourcingDB<br/>(observe_events)"]

    CommandedApp -- {:events, [event]} --> Adapter
    CommandedApp -- {:subscribed, pid} --- Adapter
    CommandedApp -- ack/3 --- Adapter
    
    Adapter -- subscribe/2 --> Transient
    Adapter -- subscribe_to/6 --> SubMan
    
    SubMan -- creates --> Observer
    SubMan -- stores/retrieves --> CheckpointStore
    
    Observer -- observe_events --> ESDB
    Observer -- {:events, [event]} --> CommandedApp
    Observer -- {:ack, event} --> SubMan
```

### Subscription Types

| Type | Use Case | Checkpoint | Module |
| ---- | -------- | ---------- | ------ |
| Transient | One-time reads | None | TransientSubscriber |
| Persistent | Event handlers, process managers | Yes (ETS) | ObserverProcess |

### Modules

#### SubscriptionManager

##### Purpose

Orchestrates the lifecycle of persistent subscriptions and manages checkpoint loading.

##### Responsibilities

- Creates and registers persistent subscriptions
- Loads checkpoints on subscription startup
- Stops subscriptions (observer process) while preserving checkpoint
- Deletes subscriptions and their checkpoints
- Updates checkpoints when events are acknowledged
- Maintains in-memory registry of active subscriptions

#### CheckpointStore

##### Purpose

Provides in-memory storage for subscription checkpoint positions.

##### Responsibilities

- Stores last acked event ID and number per subscription
- Retrieves checkpoint positions
- Deletes checkpoint data

#### ObserverProcess

##### Purpose

Bridges EventSourcingDB's observation stream to Commanded's push-based event delivery.

##### Responsibilities

- Watches events from EventSourcingDB via `observe_events`
- Forwards received events to the subscriber via messages
- Handles acknowledgments from subscriber
- Updates checkpoint in CheckpointStore on ack

#### TransientSubscriber

##### Purpose

Handles one-time transient subscriptions that don't require checkpoint tracking.

##### Responsibilities

- Processes events via `subscribe/2` for single-stream reads
- No checkpoint persistence needed (one-time reads)

### Key Decisions

**stop_observing keeps checkpoint** — Graceful unsubscribe stops observer but retains checkpoint.
