---
status: draft
date: 2025-04-15
decision-makers:
  - "@gossi"
consulted: []
informed: []
---

# Event Semantics: Map CloudEvents to Commanded RecordedEvent

Related ADRs:
- [0002 - Stream Semantics](./0002-stream-semantics.md)
- [0003 - Stream Version Semantics](./0003-stream-version-semantics.md)

## Context and Problem Statement

EventSourcingDB (ESDB) stores events in CloudEvents format, while Commanded expects a `RecordedEvent` struct with specific semantics. The adapter must translate between these two representations while preserving the semantic meaning of each field. This ADR documents how CloudEvents map to Commanded's RecordedEvent.

## Decision Drivers

- Commanded tests require `event_id` to be a UUID format (but only the tests!)
- Commanded expects `event_number` to be a globally unique, monotonically incrementing, gapless integer
- Commanded expects `stream_version` to be per-stream sequential numbering (1, 2, 3...)
- CloudEvents specification requires `source + id` to be unique for each distinct event
- ESDB uses a global counter per subject for `event.id`
- Commanded metadata (correlation_id, causation_id, metadata) must be preserved

## Considered Options

- Use ESDB's `id` directly as `event_id`
- Generate UUID for `event_id` from `source/id` concatenation
- Use ESDB's global `id` as `event_number` vs stream-based versioning

## Decision Outcome

Chosen option: Map fields with runtime behavior based on environment, because the CloudEvents spec and Commanded expectations differ.

### Field Mapping

| Commanded Field | CloudEvents Field | Mapping Strategy |
| --------------- | ----------------- | ----------------- |
| `event_id` | N/A (derived) | Tests: UUID from `Commanded.UUID.uuid4()`<br>Prod: `#{source}/#{id}` (per CloudEvents uniqueness) |
| `event_number` | `id` | `String.to_integer(event.id) + 1` (global counter) |
| `stream_version` | N/A (computed) | Internally tracked per stream in adapter - see [0003 - Stream Version Semantics](./0003-stream-version-semantics.md) |
| `stream_id` | `subject` | Extracted from subject, stripping stream_prefix |
| `event_type` | `type` | Direct mapping |
| `data` | `data` | Deserialized using TypeProvider |
| `correlation_id` | N/A (metadata) | Extracted from `__commanded_metadata__` in data |
| `causation_id` | N/A (metadata) | Extracted from `__commanded_metadata__` in data |
| `metadata` | N/A (metadata) | Extracted from `__commanded_metadata__` in data |
| `created_at` | `time` | Converted from ISO8601 string to DateTime |

### CloudEvents Event Structure

```elixir
%EventSourcingDB.Event{
  id: String.t(),           # Global counter (e.g., "1", "2", "3")
  source: String.t(),      # URI-reference (e.g., "/myapp/bank-account/")
  subject: String.t(),     # Full stream path (e.g., "/myapp/bank-account/ACC123")
  type: String.t(),        # Event type (e.g., "Elixir.AccountOpened")
  data: map(),             # Event payload + optional __commanded_metadata__
  time: String.t(),        # ISO8601 timestamp
  specversion: String.t(), # CloudEvents version (e.g., "1.0")
  # Optional fields
  datacontenttype: String.t() | nil,
  dataschema: String.t() | nil,
  hash: String.t(),
  signature: String.t() | nil,
  predecessorhash: String.t(),
  traceparent: String.t() | nil,
  tracestate: String.t() | nil
}
```

### Commanded RecordedEvent Structure

```elixir
%Commanded.EventStore.RecordedEvent{
  event_id: uuid(),                    # Unique identifier
  event_number: non_neg_integer(),     # Global, monotonically increasing
  stream_id: String.t(),               # Stream identity
  stream_version: non_neg_integer(),  # Per-stream sequential
  causation_id: uuid() | nil,
  correlation_id: uuid() | nil,
  event_type: String.t(),
  data: struct(),
  metadata: map(),
  created_at: DateTime.t()
}
```

### Metadata Serialization

Commanded embeds metadata (correlation_id, causation_id, metadata) in the event data using a special key:

```elixir
# Serialized structure in ESDB
%{
  "__commanded_metadata__" => %{
    "correlation_id" => "uuid-string",
    "causation_id" => "uuid-string",
    "metadata" => %{}
  },
  # ... event data fields
}
```

The adapter extracts this metadata during deserialization and maps it to the RecordedEvent fields.

## Consequences

### Good

- Preserves CloudEvents uniqueness guarantee (source + id)
- Satisfies Commanded test requirements (UUID event_id in tests)
- Maintains Commanded semantics (global event_number, per-stream version)
- Preserves correlation/causation tracking

### Bad

- Requires runtime branching for event_id generation (test vs prod)
- Metadata serialization adds complexity to event data
- Stream version must be tracked separately (ESDB doesn't provide it) - see [0003 - Stream Version Semantics](./0003-stream-version-semantics.md)

### Neutral

- Stream subject to stream_id mapping requires stream_prefix configuration - see [0002 - Stream Semantics](./0002-stream-semantics.md)
- Time conversion from ISO8601 string to DateTime

## Implementation Notes

### event_id Generation

```elixir
# In test environment: use UUID to satisfy Commanded tests
@compile if: Mix.env() == :test
defp generate_event_id(%Event{} = _event), do: Commanded.UUID.uuid4()

# In prod: use CloudEvents uniqueness (source/id)
@compile if: Mix.env() != :test
defp generate_event_id(%Event{} = event), do: "#{event.source}/#{event.id}"
```

### stream_id Extraction

See [0002 - Stream Semantics](./0002-stream-semantics.md) for the stream to subject mapping formula.

### Global vs Stream Versioning

| Field | Scope | Source |
| ----- | ----- | ------ |
| `event_number` | Global | ESDB `id` (global counter) |
| `stream_version` | Per-stream | Adapter internal tracking |
