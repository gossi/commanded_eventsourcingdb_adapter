defmodule Commanded.EventStore.Adapters.EventSourcingDB.ObserverProcess do
  @moduledoc """
  Bridges EventSourcingDB's observation stream to Commanded's push-based event delivery.

  Responsibilities:
  - Watches events from EventSourcingDB via observe_events
  - Forwards received events to the subscriber via messages
  - Handles acknowledgments from subscriber
  - Updates checkpoint in CheckpointStore on ack
  """

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper

  def start_link(opts) do
    client = Keyword.fetch!(opts, :client)
    subscriber = Keyword.fetch!(opts, :subscriber)
    subject = Keyword.fetch!(opts, :subject)
    stream_uuid = Keyword.fetch!(opts, :stream_uuid)
    stream_prefix = Keyword.get(opts, :stream_prefix, "")
    selector = Keyword.get(opts, :selector)
    checkpoint = Keyword.get(opts, :checkpoint)
    subscription_name = Keyword.get(opts, :subscription_name)
    event_store = Keyword.get(opts, :event_store)

    state = %{
      client: client,
      subscriber: subscriber,
      subject: subject,
      stream_uuid: stream_uuid,
      stream_prefix: stream_prefix,
      selector: selector,
      checkpoint: checkpoint,
      subscription_name: subscription_name,
      subject_version_map: %{}
    }

    GenServer.start_link(__MODULE__, state,
      name: via_tuple(subscription_name, stream_uuid, stream_prefix, event_store)
    )
  end

  defp via_tuple(subscription_name, stream_uuid, stream_prefix, event_store) do
    sanitized_prefix = String.replace(stream_prefix, "/", "_")
    unique_name = "#{subscription_name}:#{stream_uuid}:#{sanitized_prefix}"

    {:via, Registry, {Module.concat([event_store, :ObserverProcesses]), unique_name}}
  end

  @impl GenServer
  def init(state) do
    Process.monitor(state.subscriber)
    send(self(), :start_observing)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:start_observing, state) do
    send(state.subscriber, {:subscribed, self()})
    start_streaming(state)
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{subscriber: pid} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:ack, event}, state) do
    CheckpointStore.store_checkpoint(
      state.subscription_name,
      state.stream_uuid,
      event.event_id,
      event.event_number
    )

    new_checkpoint = %{event_id: event.event_id, event_number: event.event_number}
    {:noreply, %{state | checkpoint: new_checkpoint}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_streaming(state) do
    options = build_observe_options(state)

    case EventSourcingDB.observe_events(state.client, state.subject, options) do
      {:ok, events_stream} ->
        Task.start(fn -> stream_events(events_stream, state) end)
        {:noreply, state}

      {:error, reason} ->
        Logger.error("ESDB observe_events failed: #{inspect(reason)}")
        exit({:observe_events_failed, reason})
    end
  end

  defp stream_events(events_stream, state) do
    try do
      Enum.reduce_while(events_stream, state, fn
        %EventSourcingDB.Event{id: "heartbeat"}, acc ->
          {:cont, acc}

        %EventSourcingDB.Event{} = event, acc ->
          {:cont, dispatch_event(event, acc)}

        other, acc ->
          Logger.warning("Unexpected event: #{inspect(other)}")
          {:cont, acc}
      end)
    rescue
      e ->
        Logger.error("Stream error: #{inspect(e)}")
        state
    end
  end

  defp build_observe_options(state) do
    base_options = observe_options(state.subject, state.stream_prefix)

    case state.checkpoint do
      %{event_id: event_id} ->
        lower_bound = %EventSourcingDB.BoundOptions{type: :exclusive, id: event_id}
        %EventSourcingDB.ObserveEventsOptions{base_options | lower_bound: lower_bound}

      nil ->
        base_options
    end
  end

  defp observe_options(subject, stream_prefix) do
    recursive = is_all?(subject, stream_prefix)
    %EventSourcingDB.ObserveEventsOptions{recursive: recursive}
  end

  defp is_all?(subject, stream_prefix),
    do: subject == "/#{stream_prefix}" or (stream_prefix == "" and subject == "/")

  defp dispatch_event(event, state) do
    stream_id = StreamMapper.get_stream_id(event.subject, state.stream_prefix)
    {version, updated_map} = next_version(state.subject_version_map, stream_id)

    state = %{state | subject_version_map: updated_map}

    recorded_event = EventMapper.to_recorded_event(event, version, state.stream_prefix)
    recorded_event = %{recorded_event | stream_version: version}

    is_all = is_all?(state.subject, state.stream_prefix)
    stream_matches = recorded_event.stream_id == state.stream_uuid

    should_send = is_all or stream_matches

    passes_selector =
      case state.selector do
        nil -> true
        fun -> fun.(recorded_event)
      end

    if should_send && passes_selector do
      Logger.debug(
        "ObserverProcess sending event #{recorded_event.event_number} to #{inspect(state.subscriber)}"
      )

      send(state.subscriber, {:events, [recorded_event]})
    end

    state
  end

  defp next_version(version_map, stream_id) do
    version = Map.get(version_map, stream_id, 0) + 1
    {version, Map.put(version_map, stream_id, version)}
  end
end
