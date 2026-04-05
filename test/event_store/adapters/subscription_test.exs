defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB

  use Commanded.EventStore.EventSourcingDBTestCase
  use Commanded.EventStore.SubscriptionTestCase, event_store: EventSourcingDB
end
