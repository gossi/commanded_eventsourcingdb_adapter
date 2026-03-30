defmodule Commanded.EventStore.Adapters.EventSourcingDBTest do
  use Commanded.EventSourcingDBTestCase
  # doctest Commanded.EventStore.Adapters.EventSourcingDB

  # test "greets the world" do
  #   assert Commanded.EventStore.Adapters.EventSourcingDB.hello() == :world
  # end

  test "ping", %{esdb_meta: esdb_meta} do
    IO.inspect(esdb_meta)

    assert :ok == Commanded.EventStore.Adapters.EventSourcingDB.ping(esdb_meta)
  end
end
