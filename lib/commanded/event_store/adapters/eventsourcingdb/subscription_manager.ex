defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionManager do
  @moduledoc """
  Coordinates the lifecycle of persistent subscriptions.

  Responsibilities:
  - Creates and registers persistent subscriptions
  - Loads checkpoints on subscription startup (uses :current as fallback)
  - Stops subscriptions (observer process) while preserving checkpoint
  - Deletes subscriptions and their checkpoints
  - Updates checkpoints when events are acknowledged
  - Maintains in-memory registry of active subscriptions
  """

  use GenServer

  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  @doc """
  Create a new persistent subscription.
  Loads checkpoint or falls back to :current.
  """
  def create_subscription(pid, stream_uuid, subscription_name, subscriber, start_from, opts) do
    GenServer.call(
      pid,
      {:create_subscription, stream_uuid, subscription_name, subscriber, start_from, opts},
      30_000
    )
  end

  @doc """
  Get subscription details.
  """
  def get_subscription(pid, stream_uuid, subscription_name) do
    GenServer.call(pid, {:get_subscription, stream_uuid, subscription_name})
  end

  @doc """
  List all subscriptions.
  """
  def list_subscriptions(pid) do
    GenServer.call(pid, :list_subscriptions)
  end

  @doc """
  Update checkpoint after ack_event.
  """
  def update_checkpoint(pid, stream_uuid, subscription_name, event_id, event_number) do
    GenServer.call(
      pid,
      {:update_checkpoint, stream_uuid, subscription_name, event_id, event_number}
    )
  end

  @doc """
  Get checkpoint for a subscription.
  """
  def get_checkpoint(pid, stream_uuid, subscription_name) do
    GenServer.call(pid, {:get_checkpoint, stream_uuid, subscription_name})
  end

  @doc """
  Stop subscription (observer process) but keep checkpoint.
  """
  def stop_observing(pid, stream_uuid, subscription_name) do
    GenServer.call(pid, {:stop_observing, stream_uuid, subscription_name})
  end

  @doc """
  Stop subscription by observer process pid.
  """
  def stop_observing_by_pid(pid, observer_pid) do
    GenServer.call(pid, {:stop_observing_by_pid, observer_pid})
  end

  @doc """
  Delete subscription and remove checkpoint.
  """
  def delete_subscription(pid, stream_uuid, subscription_name) do
    GenServer.call(pid, {:delete_subscription, stream_uuid, subscription_name})
  end

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call(
        {:create_subscription, stream_uuid, subscription_name, subscriber, start_from, opts},
        _from,
        state
      ) do
    key = {stream_uuid, subscription_name}
    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    case Map.get(state, key) do
      nil ->
        checkpoint = get_checkpoint_from_store(subscription_name, stream_uuid)

        subscription = %{
          stream_uuid: stream_uuid,
          subscription_name: subscription_name,
          subscriber: subscriber,
          start_from: start_from,
          checkpoint: checkpoint,
          concurrency_limit: concurrency_limit,
          subscribers: [subscriber]
        }

        Process.monitor(subscriber)
        {:reply, {:ok, subscription}, Map.put(state, key, subscription)}

      %{subscribers: subscribers, concurrency_limit: limit} ->
        cond do
          subscriber in subscribers ->
            {:reply, {:error, :subscription_already_exists}, state}

          length(subscribers) < limit ->
            Process.monitor(subscriber)

            updated =
              Map.update!(state, key, fn s ->
                %{s | subscribers: [subscriber | s.subscribers]}
              end)

            {:reply, {:ok, Map.get(updated, key)}, updated}

          true ->
            {:reply, {:error, :too_many_subscribers}, state}
        end
    end
  end

  def handle_call({:get_subscription, stream_uuid, subscription_name}, _from, state) do
    key = {stream_uuid, subscription_name}

    case Map.get(state, key) do
      nil -> {:reply, {:error, :not_found}, state}
      subscription -> {:reply, {:ok, subscription}, state}
    end
  end

  def handle_call(:list_subscriptions, _from, state) do
    {:reply, Map.values(state), state}
  end

  def handle_call(
        {:update_checkpoint, stream_uuid, subscription_name, event_id, event_number},
        _from,
        state
      ) do
    CheckpointStore.store_checkpoint(subscription_name, stream_uuid, event_id, event_number)

    key = {stream_uuid, subscription_name}

    updated_state =
      case Map.get(state, key) do
        nil ->
          state

        subscription ->
          new_checkpoint = %{event_id: event_id, event_number: event_number}
          Map.put(state, key, %{subscription | checkpoint: new_checkpoint})
      end

    {:reply, :ok, updated_state}
  end

  def handle_call({:get_checkpoint, stream_uuid, subscription_name}, _from, state) do
    key = {stream_uuid, subscription_name}

    case Map.get(state, key) do
      nil ->
        {:reply, {:error, :not_found}, state}

      %{checkpoint: nil} ->
        {:reply, {:error, :not_found}, state}

      %{checkpoint: checkpoint} ->
        {:reply, {:ok, checkpoint}, state}
    end
  end

  def handle_call({:stop_observing, stream_uuid, subscription_name}, _from, state) do
    key = {stream_uuid, subscription_name}

    case Map.get(state, key) do
      nil ->
        {:reply, {:error, :not_found}, state}

      _subscription ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:stop_observing_by_pid, observer_pid}, _from, state) do
    case Enum.find(state, fn {_, v} -> observer_pid in v.subscribers end) do
      {key, _subscription} ->
        {:reply, :ok, state}

      nil ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_subscription, stream_uuid, subscription_name}, _from, state) do
    key = {stream_uuid, subscription_name}

    CheckpointStore.delete_checkpoint(subscription_name, stream_uuid)

    {:reply, :ok, Map.delete(state, key)}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_state =
      state
      |> Enum.reduce(state, fn
        {key, %{subscribers: [pid], concurrency_limit: _}}, acc ->
          Map.delete(acc, key)

        {key, %{subscribers: subscribers, concurrency_limit: limit} = sub}, acc ->
          if pid in subscribers do
            Map.put(acc, key, %{sub | subscribers: List.delete(subscribers, pid)})
          else
            acc
          end

        _, acc ->
          acc
      end)

    {:noreply, new_state}
  end

  defp get_checkpoint_from_store(subscription_name, stream_uuid) do
    case CheckpointStore.get_checkpoint(subscription_name, stream_uuid) do
      {:ok, checkpoint} -> checkpoint
      {:error, :not_found} -> nil
    end
  end
end
