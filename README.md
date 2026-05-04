# commanded_eventsourcingdb_adapter

An Elixir event store adapter that integrates [Commanded](https://github.com/commanded/commanded) with [EventSourcingDB](https://eventsourcingdb.io) – a purpose-built database for event sourcing.

Documentation:

- [`commanded_eventsourcingdb_adapter` on hexdocs](https://hexdocs.pm/commanded_eventsourcingdb_adapter)
- [EventSourcingDB Documentation](https://docs.eventsourcingdb.io/)
- [EventSourcingDB Elixir SDK](https://hexdocs.pm/eventsourcingdb)

## Supported Features

- ✅ `append_to_stream` - Write events to a stream with expected version handling
- ✅ `stream_forward` - Read events from a stream
- ✅ `subscribe` - Transient subscriptions for real-time notifications
- ✅ `subscribe_to` - Persistent subscriptions with checkpointing
- ✅ `ack_event` - Event acknowledgment for checkpoint updates
- ✅ `unsubscribe` - Cancel subscriptions
- ✅ `delete_subscription` - Remove subscriptions and checkpoints
- ✅ Correlation and causation ID tracking via metadata
- ✅ CloudEvents format for event storage
- ❌  Snapshots - ESDB has no snapshot storage/feature. Read more about the [snapshot paradox](https://docs.eventsourcingdb.io/blog/2026/03/02/the-snapshot-paradox/)

## Installation

The package can be installed by adding `commanded_eventsourcingdb_adapter` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:commanded_eventsourcingdb_adapter, "~> 0.0.1"}
  ]
end
```

## Configuration

Configure the adapter in your application config:

```elixir
# config/config.exs
config :my_app, MyApp,
  event_store: [
    adapter: Commanded.EventStore.Adapters.EventSourcingDB,
    client: [
      api_token: "your-api-token",
      base_url: "http://localhost:3000"
    ],
    stream_prefix: "myapp",
    source: "https://my.app"
  ]
```

### Configuration Options

- `:client` - Required. ESDB client configuration containing `:url` and `:token`.
- `:stream_prefix` - Optional. Prefix for stream subjects. Defaults to `""`.
- `:source` - Required. Event source URI for CloudEvents.

## CloudEvents

Commanded has its own internal event representation as
[RecordedEvent](https://hexdocs.pm/commanded/Commanded.EventStore.RecordedEvent.html)
whereas EventSourcingDB follows the [CloudEvents](https://cloudevents.io/)
specification. The adapter maps between the two as described in [ADR
0001](./docs/adrs/0001-event-semantics.md).

### Event Source

Define in your config, see above.

### Event ID

The event ID within commanded's `RecordedEvent` is a concatenation of the source + event id from ESDB: `#{event.source}/#{event.id}`

### Event Types

Commanded has a default type provider, that (de)serializes your module name as
event type. In the example below, the event has `Elixir.AccountOpened` as event
type. This is perhaps not the best value for your event type, consider your
custom [type provider](https://hexdocs.pm/commanded/Commanded.EventStore.TypeProvider.html)
for naming your events types.

### Event Subject

Commanded is stream-oriented, which translates to subjects as per CloudEvents
spec, [ADR 0002](./docs/adrs/0002-stream-semantics.md) specifies the used semantics.

The subject: `/#{stream_prefix}/#{identity_prefix}/#{aggregate_uuid}`

- `stream_prefix`: defined in your `config/config.exs` within your `event_store`
  config for your commanded app
- `identity_prefix`: Using the prefix option in [`identify`](https://hexdocs.pm/commanded/Commanded.Commands.Router.html#identify/2)
- `aggregate_uuid`: [define aggregate identity in your
  router](https://hexdocs.pm/commanded/Commanded.Commands.Router.html#module-define-aggregate-identity)

### Metadata Storage

Commanded stores `correlation_id`, `causation_id`, and `metadata` as part of the
event's `data` field using a special `__commanded_metadata__` key:

```elixir
# Data field stored in ESDB
%{
  "__commanded_metadata__" => %{
    "correlation_id" => "uuid-string",
    "causation_id" => "uuid-string",
    "metadata" => %{"key" => "value"}
  },
  # ... event data fields
}
```

### Sample CloudEvent

This is what an event looks like when stored in EventSourcingDB:

```json
{
  "specversion": "1.0",
  "id": "5",
  "source": "https://my.app",
  "subject": "/myapp/bank-account/ACC123",
  "type": "Elixir.AccountOpened",
  "datacontenttype": "application/json",
  "data": {
    "__commanded_metadata__": {
      "correlation_id": "aaa-bbb-ccc",
      "causation_id": "ddd-eee-fff",
      "metadata": {}
    },
    "account_number": "ACC123",
    "initial_balance": 1000
  },
  "time": "2025-04-15T10:00:00Z",
  "predecessorhash": "0000000000000000000000000000000000000000000000000000000000000000",
  "hash": "abc123..."
}
```

## Testing

Run tests using:

```bash
mix test
```

Tests use [Testcontainers](https://github.com/testcontainers/testcontainers-elixir) to spin up an EventSourcingDB instance.
