defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.Adapters.EventSourcingDB.Subscription

  @spec start_link(Keyword.t(), GenServer.options()) :: GenServer.on_start()
  def start_link(config, opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, config, opts)
  end

  @impl true
  def init(config) do
    client = Keyword.fetch!(config, :client)
    event_store = Keyword.fetch!(config, :event_store)
    stream_prefix = Keyword.fetch!(config, :stream_prefix)

    DynamicSupervisor.init(
      strategy: :one_for_one,
      extra_arguments: [client, event_store, stream_prefix]
    )
  end

  @doc """
  Start (or join) a subscription.

  If a subscription with the same `{stream, subscription_name}` already exists,
  the new subscriber is added to it via `Subscription.add_subscriber/2`. The
  resulting reply mirrors `subscribe_to/6` semantics:

  * `{:ok, pid}` – success
  * `{:error, :subscription_already_exists}` – name already taken at concurrency limit 1
  * `{:error, :too_many_subscribers}` – concurrency limit reached
  """
  def start_subscription(
        event_store,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts
      ) do
    spec = subscription_spec(stream, subscription_name, subscriber, start_from, opts)

    case DynamicSupervisor.start_child(supervisor_name(event_store), spec) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> Subscription.add_subscriber(pid, subscriber)
      {:error, _reason} = error -> error
    end
  end

  @spec stop_subscription(term(), pid()) :: :ok
  def stop_subscription(event_store, pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(supervisor_name(event_store), pid)
    :ok
  end

  @doc """
  Stop the subscription if running and delete its checkpoint.
  """
  @spec delete_subscription(term(), String.t(), term(), String.t()) ::
          :ok | {:error, :subscription_not_found}
  def delete_subscription(event_store, stream_prefix, stream, subscription_name) do
    pid = Subscription.whereis(event_store, stream, subscription_name)
    has_checkpoint = match?({:ok, _}, CheckpointStore.get(stream_prefix, subscription_name))

    cond do
      is_pid(pid) ->
        stop_subscription(event_store, pid)
        CheckpointStore.delete(stream_prefix, subscription_name)
        :ok

      has_checkpoint ->
        CheckpointStore.delete(stream_prefix, subscription_name)
        :ok

      true ->
        {:error, :subscription_not_found}
    end
  end

  defp subscription_spec(stream, subscription_name, subscriber, start_from, opts) do
    %{
      id: {Subscription, stream, subscription_name},
      start:
        {Subscription, :start_link,
         [stream, subscription_name, subscriber, start_from, opts]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  defp supervisor_name(event_store), do: Module.concat([event_store, __MODULE__])
end
