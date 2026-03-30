defmodule Commanded.EventStore.Adapters.EventSourcingDB.Config do
  def client(config) do
    EventSourcingDB.Client.new(config)
  end
end
