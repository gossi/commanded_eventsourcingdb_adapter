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

## Architecture

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
- [Extreme Adapter Tests](https://github.com/commanded/commanded-extreme-adapter/tree/master/test/event_store)
- [Extreme SDK](https://github.com/exponentially/extreme)
- [Kurrent Docs](https://docs.kurrent.io/server/latest/)

### Event Store Adapter

The event store adapter uses postgres db for storing events.

- [Event Store Adapter
  Source](https://github.com/commanded/commanded-eventstore-adapter)
- [Event Store Adapter
  Tests](https://github.com/commanded/commanded-eventstore-adapter/tree/master/test)
