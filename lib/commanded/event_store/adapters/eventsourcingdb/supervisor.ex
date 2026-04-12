defmodule Commanded.EventStore.Adapters.EventSourcingDB.Supervisor do
  @moduledoc false

  use Supervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.Config
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher
  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor

  def start_link(config) do
    event_store = Keyword.fetch!(config, :event_store)
    name = Module.concat([event_store, Supervisor])

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    IO.inspect(config, label: "supervisor")
    client_config = Keyword.fetch!(config, :client)
    client = Config.client(client_config)
    stream_prefix = Keyword.get(config, :stream_prefix, "")

    event_store = Keyword.fetch!(config, :event_store)
    pubsub_name = Module.concat([event_store, PubSub])
    event_publisher_name = Module.concat([event_store, EventPublisher])

    children = [
      {Registry, keys: :duplicate, name: pubsub_name, partitions: 1},
      %{
        id: EventPublisher,
        start:
          {EventPublisher, :start_link,
           [
             {client, event_store, pubsub_name, stream_prefix},
             [name: event_publisher_name]
           ]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
