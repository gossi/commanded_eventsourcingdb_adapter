defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.Subscription
  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore

  @spec start_link(Keyword.t(), GenServer.options()) :: GenServer.on_start()
  def start_link(config, opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, config, opts)
  end

  @impl true
  def init(config) do
    client = Keyword.fetch!(config, :client)
    stream_prefix = Keyword.get(config, :stream_prefix, "")

    DynamicSupervisor.init(strategy: :one_for_one, extra_arguments: [client, stream_prefix])
  end

  def start_subscription(
        event_store,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts
      ) do
    name = name(event_store)

    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    spec =
      subscription_spec(
        event_store,
        stream,
        subscription_name,
        subscriber,
        start_from,
        concurrency_limit: concurrency_limit
      )

    case DynamicSupervisor.start_child(name, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, existing_pid}} ->
        if concurrency_limit == 1 do
          {:error, :subscription_already_exists}
        else
          GenServer.call(existing_pid, {:add_subscriber, subscriber})
        end

      reply ->
        reply
    end
  end

  @spec stop_subscription(term(), pid()) :: :ok | {:error, term()}
  def stop_subscription(event_store, subscription_pid) do
    name = name(event_store)
    DynamicSupervisor.terminate_child(name, subscription_pid)
    :ok
  end

  @spec delete_subscription(term(), String.t() | :all, String.t()) :: :ok
  def delete_subscription(_event_store, _stream, subscription_name) do
    CheckpointStore.delete(subscription_name)
    :ok
  end

  defp subscription_spec(
         event_store,
         stream,
         subscription_name,
         subscriber,
         start_from,
         opts
       ) do
    start_args = [
      event_store,
      stream,
      subscription_name,
      subscriber,
      start_from,
      opts
    ]

    %{
      id: {Subscription, stream, subscription_name},
      start: {Subscription, :start_link, start_args},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  defp name(event_store), do: Module.concat([event_store, __MODULE__])
end
