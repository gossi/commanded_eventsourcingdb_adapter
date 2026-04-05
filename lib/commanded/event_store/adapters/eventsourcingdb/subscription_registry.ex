defmodule Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionRegistry do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  def register(pid, name, stream_uuid) do
    GenServer.call(pid, {:register, name, stream_uuid})
  end

  def register(pid, name, stream_uuid, concurrency_limit) do
    GenServer.call(pid, {:register, name, stream_uuid, concurrency_limit})
  end

  def unregister(pid, name, stream_uuid) do
    GenServer.call(pid, {:unregister, name, stream_uuid})
  end

  def exists?(pid, name, stream_uuid) do
    GenServer.call(pid, {:exists?, name, stream_uuid})
  end

  def subscriber_count(pid, name, stream_uuid) do
    GenServer.call(pid, {:subscriber_count, name, stream_uuid})
  end

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call({:register, name, stream_uuid}, {caller, _}, state) do
    handle_register(name, stream_uuid, caller, 1, state)
  end

  def handle_call({:register, name, stream_uuid, concurrency_limit}, {caller, _}, state) do
    handle_register(name, stream_uuid, caller, concurrency_limit, state)
  end

  defp handle_register(name, stream_uuid, caller, concurrency_limit, state) do
    key = {name, stream_uuid}

    case Map.get(state, key) do
      nil ->
        Process.monitor(caller)
        {:reply, :ok, Map.put(state, key, %{subscribers: [caller], limit: concurrency_limit})}

      %{subscribers: subscribers, limit: limit} ->
        cond do
          caller in subscribers ->
            {:reply, :ok, state}

          length(subscribers) < limit ->
            Process.monitor(caller)

            {:reply, :ok,
             Map.put(state, key, %{subscribers: [caller | subscribers], limit: limit})}

          true ->
            {:reply, {:error, :too_many_subscribers}, state}
        end
    end
  end

  def handle_call({:unregister, name, stream_uuid}, {caller, _}, state) do
    key = {name, stream_uuid}

    case Map.get(state, key) do
      nil ->
        {:reply, :ok, state}

      %{subscribers: [caller], limit: _} ->
        {:reply, :ok, Map.delete(state, key)}

      %{subscribers: subscribers, limit: limit} ->
        new_subscribers = List.delete(subscribers, caller)
        {:reply, :ok, Map.put(state, key, %{subscribers: new_subscribers, limit: limit})}
    end
  end

  def handle_call({:exists?, name, stream_uuid}, _from, state) do
    key = {name, stream_uuid}
    {:reply, Map.has_key?(state, key), state}
  end

  def handle_call({:subscriber_count, name, stream_uuid}, _from, state) do
    key = {name, stream_uuid}

    case Map.get(state, key) do
      nil -> {:reply, 0, state}
      %{subscribers: subscribers} -> {:reply, length(subscribers), state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    state =
      Enum.reduce(state, state, fn
        {key, %{subscribers: [pid], limit: _}}, acc ->
          Map.delete(acc, key)

        {key, %{subscribers: subscribers, limit: limit}}, acc ->
          if pid in subscribers do
            Map.put(acc, key, %{subscribers: List.delete(subscribers, pid), limit: limit})
          else
            acc
          end

        {key, _}, acc ->
          acc
      end)

    {:noreply, state}
  end
end
