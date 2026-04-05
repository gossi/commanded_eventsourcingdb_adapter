defmodule Commanded.EventStore.Adapters.EventSourcingDB.Config do
  def client(config) do
    EventSourcingDB.Client.new(config)
  end

  def read_options(:all), do: %EventSourcingDB.ReadEventsOptions{recursive: true}
  def read_options(_uuid), do: nil
end
