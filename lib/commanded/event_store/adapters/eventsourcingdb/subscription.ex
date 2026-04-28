defmodule Commanded.EventStore.Adapters.EventSourcingDB.Subscription do
  @moduledoc false
  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias Commanded.EventStore.RecordedEvent
  alias EventSourcingDB.BoundOptions
  alias EventSourcingDB.Event
  alias EventSourcingDB.ObserveEventsOptions
  alias EventSourcingDB.ReadEventsOptions

  @observer_restart_delay 250

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :stream_prefix,
      :stream,
      :subscription_name,
      :start_from,
      :concurrency_limit,
      :observer_pid,
      :observer_ref,
      subscribers: [],
      subscriber_index: 0,
      stream_versions: %{},
      last_event_id: nil,
      pending_acks: []
    ]
  end

  def start_link(
        client,
        event_store,
        stream_prefix,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts
      ) do
    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    state = %State{
      client: client,
      event_store: event_store,
      stream_prefix: stream_prefix,
      stream: stream,
      subscription_name: subscription_name,
      start_from: start_from,
      concurrency_limit: concurrency_limit,
      subscribers: [{subscriber, nil}]
    }

    GenServer.start_link(__MODULE__, state, name: name(event_store, stream, subscription_name))
  end

  def name(event_store, stream, subscription_name) do
    {:global, {event_store, stream, subscription_name}}
  end

  def whereis(event_store, stream, subscription_name) do
    case :global.whereis_name({event_store, stream, subscription_name}) do
      :undefined -> nil
      pid when is_pid(pid) -> pid
    end
  end

  def add_subscriber(pid, subscriber) do
    GenServer.call(pid, {:add_subscriber, subscriber})
  end

  def ack(pid, %RecordedEvent{} = event) do
    GenServer.call(pid, {:ack, event})
  end

  @impl true
  def init(%State{subscribers: [{subscriber, _}]} = state) do
    ref = Process.monitor(subscriber)
    state = %{state | subscribers: [{subscriber, ref}]}

    state = initialize_position(state)

    send(subscriber, {:subscribed, self()})

    {:ok, state, {:continue, :start_observer}}
  end

  @impl true
  def handle_continue(:start_observer, state) do
    {:noreply, start_observer(state)}
  end

  @impl true
  def handle_call({:add_subscriber, pid}, _from, %State{} = state) do
    cond do
      Enum.any?(state.subscribers, fn {p, _} -> p == pid end) ->
        {:reply, {:error, :subscription_already_exists}, state}

      length(state.subscribers) >= state.concurrency_limit and state.concurrency_limit == 1 ->
        {:reply, {:error, :subscription_already_exists}, state}

      length(state.subscribers) >= state.concurrency_limit ->
        {:reply, {:error, :too_many_subscribers}, state}

      true ->
        ref = Process.monitor(pid)
        new_subscribers = state.subscribers ++ [{pid, ref}]
        send(pid, {:subscribed, self()})
        {:reply, {:ok, self()}, %{state | subscribers: new_subscribers}}
    end
  end

  @impl true
  def handle_call({:ack, %RecordedEvent{event_number: number}}, _from, %State{} = state) do
    {acked, remaining} =
      Enum.split_with(state.pending_acks, fn {n, _id} -> n <= number end)

    case List.last(acked) do
      nil ->
        {:reply, :ok, state}

      {_n, esdb_id} ->
        :ok = CheckpointStore.put(state.stream_prefix, state.subscription_name, esdb_id)
        {:reply, :ok, %{state | pending_acks: remaining}}
    end
  end

  @impl true
  def handle_cast({:stream_event, %Event{} = event}, %State{} = state) do
    if matches_stream?(event, state) do
      {:noreply, dispatch_event(event, state)}
    else
      {:noreply, %{state | last_event_id: event.id}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{observer_ref: ref} = state) do
    state = %{state | observer_ref: nil, observer_pid: nil}
    Process.send_after(self(), :restart_observer, @observer_restart_delay)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    case Enum.split_with(state.subscribers, fn {_pid, r} -> r == ref end) do
      {[], _} ->
        {:noreply, state}

      {[_], remaining} ->
        if remaining == [] do
          {:stop, :normal, %{state | subscribers: []}}
        else
          new_index = rem(state.subscriber_index, max(length(remaining), 1))
          {:noreply, %{state | subscribers: remaining, subscriber_index: new_index}}
        end
    end
  end

  def handle_info(:restart_observer, %State{observer_pid: nil} = state) do
    {:noreply, start_observer(state)}
  end

  def handle_info(:restart_observer, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %State{observer_pid: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp dispatch_event(%Event{} = event, %State{} = state) do
    stream_id = StreamMapper.get_stream_id(event.subject, state.stream_prefix)
    stream_version = Map.get(state.stream_versions, stream_id, 0) + 1

    event_number =
      if all_stream?(state.stream) do
        EventMapper.to_global_event_number(event)
      else
        stream_version
      end

    recorded_event =
      EventMapper.to_recorded_event(event, stream_version, state.stream_prefix, event_number)

    state =
      case state.subscribers do
        [] ->
          state

        subscribers ->
          {pid, _ref} = Enum.at(subscribers, rem(state.subscriber_index, length(subscribers)))
          send(pid, {:events, [recorded_event]})

          %{
            state
            | subscriber_index: rem(state.subscriber_index + 1, length(subscribers))
          }
      end

    %{
      state
      | stream_versions: Map.put(state.stream_versions, stream_id, stream_version),
        last_event_id: event.id,
        pending_acks: state.pending_acks ++ [{event_number, event.id}]
    }
  end

  defp matches_stream?(%Event{} = event, %State{} = state) do
    cond do
      not String.starts_with?(event.subject, StreamMapper.to_subject(state.stream_prefix)) ->
        false

      all_stream?(state.stream) ->
        true

      true ->
        StreamMapper.get_stream_id(event.subject, state.stream_prefix) == state.stream
    end
  end

  defp all_stream?(:all), do: true
  defp all_stream?("$all"), do: true
  defp all_stream?(_), do: false

  defp initialize_position(%State{} = state) do
    state =
      case CheckpointStore.get(state.stream_prefix, state.subscription_name) do
        {:ok, ckp} when is_binary(ckp) and ckp != "" ->
          %{state | last_event_id: ckp}

        _ ->
          case state.start_from do
            :origin ->
              state

            :current ->
              case latest_event_id(state) do
                {:ok, id} -> %{state | last_event_id: id}
                :empty -> state
              end

            n when is_integer(n) and n > 0 ->
              %{state | last_event_id: Integer.to_string(n - 1)}

            _ ->
              state
          end
      end

    initialize_stream_version(state)
  end

  defp initialize_stream_version(%State{last_event_id: nil} = state), do: state

  defp initialize_stream_version(%State{stream: stream} = state) when stream in [:all, "$all"],
    do: state

  defp initialize_stream_version(%State{last_event_id: id} = state) when is_binary(id) do
    subject = StreamMapper.to_subject(state.stream_prefix, state.stream)

    opts =
      ReadEventsOptions.new(
        recursive: false,
        upper_bound: %BoundOptions{type: :inclusive, id: id}
      )

    case EventSourcingDB.read_events(state.client, subject, opts) do
      {:ok, stream} ->
        try do
          count = stream |> Enum.count(&match?(%Event{}, &1))
          %{state | stream_versions: Map.put(state.stream_versions, state.stream, count)}
        rescue
          _ -> state
        catch
          _, _ -> state
        end

      _ ->
        state
    end
  end

  defp latest_event_id(%State{} = state) do
    subject = StreamMapper.to_subject(state.stream_prefix, state.stream)
    opts = ReadEventsOptions.new(recursive: true, order: :antichronological)

    case EventSourcingDB.read_events(state.client, subject, opts) do
      {:ok, stream} ->
        try do
          case Enum.take(stream, 1) do
            [%Event{id: id}] -> {:ok, id}
            _ -> :empty
          end
        rescue
          _ -> :empty
        catch
          _, _ -> :empty
        end

      {:error, _reason} ->
        :empty
    end
  end

  defp start_observer(%State{} = state) do
    parent = self()
    opts = observe_options(state)
    subject = StreamMapper.to_subject(state.stream_prefix, state.stream)
    client = state.client

    {pid, ref} = spawn_monitor(fn -> run_observer(parent, client, subject, opts) end)

    %{state | observer_pid: pid, observer_ref: ref}
  end

  defp run_observer(parent, client, subject, opts) do
    case EventSourcingDB.observe_events(client, subject, opts) do
      {:ok, stream} ->
        consume_stream(parent, stream)

      {:error, _reason} ->
        :ok
    end
  end

  defp consume_stream(parent, stream) do
    try do
      Enum.each(stream, fn
        %Event{} = event ->
          GenServer.cast(parent, {:stream_event, event})

        _other ->
          :ok
      end)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp observe_options(%State{last_event_id: id}) when is_binary(id) and id != "" do
    %ObserveEventsOptions{
      recursive: true,
      lower_bound: %BoundOptions{type: :exclusive, id: id}
    }
  end

  defp observe_options(_state) do
    %ObserveEventsOptions{recursive: true}
  end
end
