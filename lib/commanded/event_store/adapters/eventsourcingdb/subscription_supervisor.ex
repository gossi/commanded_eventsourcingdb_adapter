defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor do
  @moduledoc """
  Supervisor for managing subscription processes.
  """

  use DynamicSupervisor

  def start_link(event_store) do
    name = name(event_store)
    DynamicSupervisor.start_link(__MODULE__, [], name: name)
  end

  @impl true
  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_subscription(
          term(),
          String.t(),
          String.t() | nil,
          pid(),
          :origin | :current | integer(),
          Keyword.t()
        ) :: {:ok, pid()} | {:error, term()}
  def start_subscription(
        _event_store,
        stream_uuid,
        _subscription_name,
        subscriber,
        start_from,
        opts
      ) do
    client = Keyword.get(opts, :client)
    stream_prefix = Keyword.get(opts, :stream_prefix, "")

    start_args = [
      client: client,
      stream_uuid: stream_uuid,
      stream_prefix: stream_prefix,
      subscriber: subscriber,
      start_from: start_from
    ]

    Commanded.EventStore.Adapters.EventSourcingDB.Subscription.start_link(start_args)
  end

  @spec stop_subscription(term(), pid()) :: :ok | {:error, term()}
  def stop_subscription(_event_store, subscription_pid) do
    GenServer.stop(subscription_pid, :normal, 5_000)
    :ok
  end

  defp name(event_store), do: Module.concat([event_store, SubscriptionSupervisor])
end
