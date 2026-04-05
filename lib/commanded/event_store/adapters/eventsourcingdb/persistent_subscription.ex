defmodule Commanded.EventStore.Adapters.EventSourcingDB.PersistentSubscription do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false

    defstruct [
      :client,
      :subscription_name,
      :subject,
      :stream_uuid,
      :start_from,
      :subscriber,
      :subscription_registry,
      :subscriptions_registry,
      :stream_prefix,
      :selector,
      :subject_version_map,
      :last_event_id,
      :checkpoint
    ]
  end

  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.Adapters.EventSourcingDB.PersistentSubscription.State
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionRegistry

  def start_link(opts) do
    state = %State{
      client: Keyword.fetch!(opts, :client),
      subscription_name: Keyword.fetch!(opts, :subscription_name),
      subject: Keyword.fetch!(opts, :subject),
      stream_uuid: Keyword.fetch!(opts, :stream_uuid),
      start_from: Keyword.fetch!(opts, :start_from),
      subscriber: Keyword.fetch!(opts, :subscriber),
      subscription_registry: Keyword.fetch!(opts, :subscription_registry),
      subscriptions_registry: Keyword.fetch!(opts, :subscriptions_registry),
      stream_prefix: Keyword.get(opts, :stream_prefix, ""),
      selector: Keyword.get(opts, :selector),
      subject_version_map: %{},
      last_event_id: nil,
      checkpoint: nil
    }

    GenServer.start_link(__MODULE__, state, name: via_tuple(state))
  end

  defp via_tuple(%State{subscriptions_registry: registry, subscription_name: name}) do
    {:via, Registry, {registry, name}}
  end

  @impl GenServer
  def init(%State{} = state) do
    Process.monitor(state.subscriber)
    {:ok, state, {:continue, :start_observing}}
  end

  @impl GenServer
  def handle_continue(:start_observing, state) do
    state = load_checkpoint(state)
    state = apply_start_from(state)
    send(state.subscriber, {:subscribed, self()})
    {:noreply, state, {:continue, :start_stream}}
  end

  def handle_continue(:start_stream, state) do
    Task.start(fn -> stream_events(state) end)
    {:noreply, state}
  end

  defp load_checkpoint(state) do
    case CheckpointStore.get_checkpoint(state.subscription_name, state.stream_uuid) do
      {:ok, checkpoint} ->
        %{state | checkpoint: checkpoint, last_event_id: checkpoint.event_id}

      {:error, :not_found} ->
        state
    end
  end

  defp apply_start_from(state)

  defp apply_start_from(%State{start_from: :current} = state) do
    count = count_existing_events(state)

    %{
      state
      | subject_version_map:
          increment_version_map(state.subject_version_map, state.subject, count)
    }
  end

  defp apply_start_from(state), do: state

  defp count_existing_events(%State{
         client: client,
         subject: subject,
         stream_prefix: stream_prefix
       }) do
    options = observe_options(subject, stream_prefix)

    case EventSourcingDB.read_events(client, subject, options) do
      {:ok, stream} ->
        stream
        |> Stream.filter(&match?({:ok, _}, &1))
        |> Enum.count()

      _ ->
        0
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{subscriber: pid} = state) do
    SubscriptionRegistry.unregister(
      state.subscription_registry,
      state.subscription_name,
      state.stream_uuid
    )

    {:stop, :normal, state}
  end

  def handle_info({:ack, event}, state) do
    CheckpointStore.store_checkpoint(
      state.subscription_name,
      state.stream_uuid,
      event.event_id,
      event.event_number
    )

    {:noreply,
     %{state | checkpoint: %{event_id: event.event_id, event_number: event.event_number}}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp stream_events(state) do
    options = build_observe_options(state)

    case EventSourcingDB.observe_events(state.client, state.subject, options) do
      {:ok, events_stream} ->
        stream_events_loop(events_stream, state)

      {:error, reason} ->
        Logger.error("ESDB observe_events failed: #{inspect(reason)}")
        exit({:observe_events_failed, reason})
    end
  end

  defp stream_events_loop(events_stream, state) do
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

  defp build_observe_options(%State{
         subject: subject,
         stream_prefix: stream_prefix,
         checkpoint: checkpoint
       }) do
    base_options = observe_options(subject, stream_prefix)

    case checkpoint do
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
    stream_id = event.subject
    {version, updated_map} = next_version(state.subject_version_map, stream_id)
    state = %{state | subject_version_map: updated_map}

    recorded_event =
      Mapper.to_recorded_event(event, version, state.stream_prefix)
      |> Map.put(:stream_version, version)

    is_all = is_all?(state.subject, state.stream_prefix)
    stream_matches = recorded_event.stream_id == state.stream_uuid

    should_send = is_all or stream_matches

    Logger.debug(
      "dispatch_event: subject=#{event.subject}, is_all=#{is_all}, stream_matches=#{stream_matches}, state.subject=#{state.subject}"
    )

    passes_selector =
      case state.selector do
        nil -> true
        fun -> fun.(recorded_event)
      end

    if should_send && passes_selector do
      Logger.info(
        "PersistentSubscription sending event: #{inspect(recorded_event.event_number)} to #{inspect(state.subscriber)}"
      )

      send(state.subscriber, {:events, [recorded_event]})
    end

    state
  end

  defp next_version(version_map, stream_id) do
    version = Map.get(version_map, stream_id, 0) + 1
    {version, Map.put(version_map, stream_id, version)}
  end

  defp increment_version_map(version_map, subject, count) do
    Map.put(version_map, subject, count)
  end
end
