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
        opts,
        index \\ 0
      ) do
    name = name(event_store)

    spec =
      subscription_spec(
        event_store,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts,
        index
      )

    case DynamicSupervisor.start_child(name, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, _pid}} ->
        case Keyword.get(opts, :subscriber_max_count) do
          nil ->
            {:error, :subscription_already_exists}

          subscriber_max_count ->
            if index < subscriber_max_count - 1 do
              start_subscription(
                event_store,
                stream,
                subscription_name,
                subscriber,
                start_from,
                opts,
                index + 1
              )
            else
              {:error, :too_many_subscribers}
            end
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
         opts,
         index
       ) do
    # extra args get prepended from DynamicSupervisor, see #init
    # extra_arguments = [client, stream_prefix]
    start_args = [
      event_store,
      stream,
      subscription_name,
      subscriber,
      start_from,
      Keyword.put(opts, :index, index)
    ]

    %{
      id: {Subscription, stream, subscription_name, index},
      start: {Subscription, :start_link, start_args},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  defp name(event_store), do: Module.concat([event_store, __MODULE__])
end
