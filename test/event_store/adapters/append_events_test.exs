defmodule Commanded.EventStore.Adapters.EventSourcingDB.AppendEventsTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB

  use Commanded.EventStore.EventSourcingDBTestCase
  use Commanded.EventStore.AppendEventsTestCase, event_store: EventSourcingDB
end
