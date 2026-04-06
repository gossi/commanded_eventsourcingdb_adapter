# commanded_eventsourcingdb_adapter

This project is an adapter for commanded using the EventSourcingDB (esdb)
written in elixir.

## Mix Project

The project uses mix with ExUnit. Refer to both documentation for available commands

- [Mix](https://hexdocs.pm/mix/Mix.html)
- [ExUnit](https://hexdocs.pm/ex_unit/ExUnit.html)

## Commanded

- [commanded](https://github.com/commanded/commanded)
- [Adapter](https://github.com/commanded/commanded/blob/master/guides/Choosing%20an%20Event%20Store.md)

## EventSourcingDB (ESDB)

- [Documentation](https://docs.eventsourcingdb.io/)
- [Elixir SDK Documentation](https://hexdocs.pm/eventsourcingdb)
- [CloudEvents Spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)

## Facts about Commanded + ESDB

### How EventSourcingDB uses CloudEvents

Here is how ESDB interprets and applies the CloudEvents spec:

Explanataion for `EventSourcingDB.Event` module:

- `id`: A global event counter, as per spec is of type string, but contains an
  integer value
- Uniqueness: An event is unique by source + id:
  > Producers MUST ensure that source + id is unique for each distinct event.
- ESDB does NOT have a numbering per subject (or within ONE stream per commanded
  terms)

### Stream to Subject Mapping

- A stream prefix is configured _per_ commanded app
- An app can contain multiple commanded apps.
- A stream prefix is a namespace (eg. subdomain/bounded context)

#### The Formula

- Commanded stream_uuid: `"#{identity_prefix}#{aggregate_uuid}"`
- ESDB subject: `"/#{stream_prefix}#{stream_uuid}"`

Example with `stream_prefix: "myapp/"` and `identity_prefix: "bank-account/"`:

- stream_uuid: `bank-account/ACC123`
- ESDB subject: `/myapp/bank-account/ACC123`

#### What Each Component Is

| Component | Source | Example |
| --------- | ------ | ------- |
| **`aggregate_uuid`** | Command struct field, defined by `identity:` in router | `"ACC123"` |
| **`identity_prefix`** | Router's `identify` macro or dispatch `prefix:` option | `"bank-account/"` |
| **`stream_prefix`** | Adapter config `stream_prefix:` | `"myapp/"` |
| **`stream_uuid`** | `identity_prefix <> aggregate_uuid` | `"bank-account/ACC123"` |
| **`ESDB subject`** | `"/#{stream_prefix}#{stream_uuid}"` | `"/myapp/bank-account/ACC123"` |

#### The `:all` Stream

Commanded's `:all` (subscribe to every event) maps to the app-scoped root:
`:all` → `/#{stream_prefix}`

With `stream_prefix: "myapp/"`, subscribing to `:all` reads from `"/myapp/"` with `recursive: true`, returning every event under that prefix.

#### Why Slashes in Prefixes?

Using `/` as separators in `stream_prefix` and `identity_prefix` creates a
natural hierarchy in ESDB:

```txt
/myapp/
  /bank-account/
    /ACC123
    /DEF456
  /order/
    /ORD001
```

This enables ESDB queries at any level (all bank accounts, all orders, all
events for the app).

## Editing Code

When editing code, code comments MUST be respected, especially those that
mention to DO NOT CHANGE something. That is these comments guard certain
validated facts about the project (read above).

Also, at times commanded has a very opinionated convention/expectation. For
example the recorded event id must be unique - but the tests explicitely check
for a UUID format. That is not necessary when the critical fact about a unique
id is given otherwise. As a matter of that, the code has some macros that
return different values when run under tests, to comply with these opinionated
conventions from commanded's test cases.

Respect architecture decision records (ADRs) in `docs/adrs/`.

## Testing

EventSourcingDB utilizes Testcontainer for testing. Also commanded has an idea
for testing adapters.

Use `mix test` for running tests (runs on podman, docker is not necessarily required).

- [Using Testcontainers for testing
  EventSourcingDB](https://hexdocs.pm/eventsourcingdb/readme.html#using-testcontainers)
- [Testcontainers with Elixir](https://github.com/testcontainers/testcontainers-elixir)
- [Commanded Adapter
  Tests](https://github.com/commanded/commanded/tree/master/test/event_store)

When running `mix test` it connects to docker registry. Running many tests can
lead to an error `429`, which means rating limit is reached. At this point,
tests can no longer be run. Stop executing tests. Do whatever else is possible
then. Use `mix compile` for checking if the code compiles.

To be mindful about rate limits, when following TDD, then mix allows to run
`mix test <file>`, which is helpful to run only a limited set of tests.

## Alternative Implementations

There are other adapters available that already implemented the commanded
adapter. They are great to lookup sample implementations.

### Extreme Adapter

The extreme adapter is for the `kurrent` database

- [Extreme Adapter Source
  Code](https://github.com/commanded/commanded-extreme-adapter/tree/master)
- [Extreme Adapter Tests](https://github.com/exponentially/extreme/tree/master/test)
- [Extreme SDK](https://github.com/exponentially/extreme)

### Event Store Adapter

The event store adapter uses postgres db for storing events.

- [Event Store Adapter
  Source](https://github.com/commanded/commanded-eventstore-adapter)
- [Event Store Adapter
  Tests](https://github.com/commanded/commanded-eventstore-adapter/tree/master/test)
