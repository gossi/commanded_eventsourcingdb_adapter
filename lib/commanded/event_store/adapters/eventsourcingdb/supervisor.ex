defmodule Commanded.EventStore.Adapters.EventSourcingDB.Supervisor do
  @moduledoc false

  use Supervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor

  def start_link(config) do
    event_store = Keyword.fetch!(config, :event_store)
    name = Module.concat([event_store, Supervisor])

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    event_store = Keyword.fetch!(config, :event_store)

    observer_processes_name = Module.concat([event_store, :ObserverProcesses])

    children = [
      {Registry, keys: :unique, name: observer_processes_name},
      {SubscriptionSupervisor, event_store}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
