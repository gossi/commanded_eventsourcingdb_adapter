defmodule Commanded.EventStore.Adapters.EventSourcingDB.Supervisor do
  @moduledoc false

  use Supervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.Config

  def start_link(config) do
    event_store = Keyword.fetch!(config, :event_store)
    name = Module.concat([event_store, Supervisor])

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    event_store = Keyword.fetch!(config, :event_store)
    client_config = Keyword.fetch!(config, :client)
    client = Config.client(client_config)
    pubsub_name = Module.concat([event_store, PubSub])
    subscriptions_name = Module.concat([event_store, SubscriptionsSupervisor])
    subscription_registry_name = Module.concat([event_store, SubscriptionRegistry])
    stream_prefix = Keyword.get(config, :stream_prefix, "")

    Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore.init()

    children = [
      {Registry, keys: :duplicate, name: pubsub_name, partitions: 1},
      {Registry,
       keys: :duplicate, name: Module.concat([event_store, Subscriptions]), partitions: 1},
      {Registry,
       keys: :unique, name: Module.concat([event_store, SubscriptionProcesses]), partitions: 1},
      {DynamicSupervisor, strategy: :one_for_one, name: subscriptions_name},
      {Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionRegistry,
       name: subscription_registry_name},
      {Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher,
       client: client,
       pubsub: pubsub_name,
       transient_pubsub: Module.concat([event_store, Subscriptions]),
       subject: "/#{stream_prefix}"}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
