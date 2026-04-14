defmodule Commanded.EventStore.Adapters.EventSourcingDB.Supervisor do
  @moduledoc false

  use Supervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.Config
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventObserver
  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor

  def start_link(config) do
    event_store = Keyword.fetch!(config, :event_store)
    name = Module.concat([event_store, Supervisor])

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    client_config = Keyword.fetch!(config, :client)
    client = Config.client(client_config)
    stream_prefix = Keyword.get(config, :stream_prefix, "")

    event_store = Keyword.fetch!(config, :event_store)
    pubsub_name = Module.concat([event_store, PubSub])
    observer_registry_name = Module.concat([event_store, ObserverRegistry])
    subscription_registry_name = Module.concat([event_store, SubscriptionRegistry])
    event_publisher_name = Module.concat([event_store, EventPublisher])
    event_observer_name = Module.concat([event_store, EventObserver])
    subscription_supervisor_name = Module.concat([event_store, SubscriptionSupervisor])

    children = [
      {Registry, keys: :duplicate, name: pubsub_name, partitions: 1},
      {Registry, keys: :duplicate, name: observer_registry_name, partitions: 1},
      {Registry, keys: :unique, name: subscription_registry_name, partitions: 1},
      %{
        id: EventPublisher,
        start:
          {EventPublisher, :start_link,
           [
             {client, event_store, pubsub_name, observer_registry_name, stream_prefix},
             [name: event_publisher_name]
           ]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      },
      %{
        id: EventObserver,
        start:
          {EventObserver, :start_link,
           [
             {client, event_store, observer_registry_name, stream_prefix},
             [name: event_observer_name]
           ]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      },
      %{
        id: SubscriptionSupervisor,
        start:
          {SubscriptionSupervisor, :start_link,
           [
             [
               client: client,
               stream_prefix: stream_prefix
             ],
             [name: subscription_supervisor_name]
           ]},
        restart: :permanent,
        shutdown: 5000,
        type: :supervisor
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
