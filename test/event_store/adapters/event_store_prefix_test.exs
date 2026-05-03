defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventStorePrefixTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB
  alias Commanded.EventStore.EventSourcingDBTestCase

  use Commanded.EventStore.EventStorePrefixTestCase, event_store: EventSourcingDB

  def start_event_store(config) do
    config =
      Keyword.update!(config, :prefix, fn prefix ->
        ["commandedtest", prefix, Commanded.UUID.uuid4()] |> Enum.join("/")
      end)

    config =
      case Keyword.pop(config, :prefix) do
        {nil, config} -> config
        {prefix, config} -> Keyword.put(config, :stream_prefix, prefix)
      end

    EventSourcingDBTestCase.start_event_store(config)
  end
end
