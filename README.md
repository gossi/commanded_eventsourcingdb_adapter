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
    source: "/myapp/"
    name: MyApp.EventStore
  ]
```

### Configuration Options

- `:client` - Required. ESDB client configuration containing `:url` and `:token`.
- `:stream_prefix` - Optional. Prefix for stream subjects. Defaults to `""`.
- `:source` - Required. Event source URI for CloudEvents.

## Metadata Storage

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
  "source": "/myapp/",
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
