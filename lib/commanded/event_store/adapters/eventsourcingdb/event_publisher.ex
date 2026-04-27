defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher do
  @moduledoc """
  GenServer that observes events from EventSourcingDB and distributes them
  to subscribers via the Registry.

  Responsibilities:
  - Subscribes to ESDB observe_events endpoint
  - Tracks stream_version per stream (transient subscriptions)
  - Converts ESDB Event to RecordedEvent with correct stream_version
  - Publishes to $all registry (all streams)
  - Publishes to stream-specific registry
  """

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias Commanded.EventStore.RecordedEvent
  alias EventSourcingDB.Event
  alias EventSourcingDB.ObserveEventsOptions

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :pubsub,
      :observer_registry,
      :stream_prefix,
      :subject,
      :observer_ref,
      stream_versions: %{},
      retry_count: 0
    ]
  end

  @spec start_link(
          {EventSourcingDB.Client.t(), atom(), atom(), atom(), atom(), String.t()},
          GenServer.options()
        ) ::
          GenServer.on_start()
  def start_link(
        {client, event_store, pubsub_name, observer_registry_name, stream_prefix},
        opts \\ []
      ) do
    subject = StreamMapper.to_subject(stream_prefix)

    state = %State{
      client: client,
      event_store: event_store,
      pubsub: pubsub_name,
      observer_registry: observer_registry_name,
      stream_prefix: stream_prefix,
      subject: subject
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(%State{} = state) do
    Registry.register(state.observer_registry, state.stream_prefix, self())
    :ok = GenServer.cast(self(), :start_observer)
    {:ok, state}
  end

  @impl true
  def handle_cast(:start_observer, state) do
    {:ok, pid} = start_observer(state)
    ref = Process.monitor(pid)
    {:noreply, %{state | observer_ref: ref, retry_count: 0}}
  end

  @impl true
  def handle_cast({:stream_event, %Event{} = event}, state) do
    if matches_stream_prefix?(event, state.stream_prefix) do
      stream_id = StreamMapper.get_stream_id(event.subject, state.stream_prefix)

      current_version = Map.get(state.stream_versions, stream_id, 0)
      new_version = current_version + 1

      recorded_event = EventMapper.to_recorded_event(event, new_version, state.stream_prefix)

      :ok = publish_event(recorded_event, state)

      {:noreply,
       %{state | stream_versions: Map.put(state.stream_versions, stream_id, new_version)}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{observer_ref: ref} = state) do
    if state.retry_count >= 3 do
      Logger.error("EventPublisher: Observer stream failed after 3 retries. Giving up.")
      {:noreply, state}
    else
      Logger.warning("EventPublisher: Observer stream closed. Retrying in 1000ms...")
      :timer.sleep(1_000)
      :ok = GenServer.cast(self(), :start_observer)
      {:noreply, %{state | retry_count: state.retry_count + 1}}
    end
  end

  @impl true
  def handle_info(msg, state) do
    IO.inspect(msg, label: "EventPublisher msg")
    {:noreply, state}
  end

  defp publish_event(%RecordedEvent{} = event, state) do
    :ok = publish_to_all(event, state)
    :ok = publish_to_stream(event, state)
  end

  defp publish_to_all(%RecordedEvent{} = event, state) do
    Registry.dispatch(state.pubsub, "$all", fn entries ->
      for {pid, _} <- entries, do: send(pid, {:events, [event]})
    end)
  end

  defp publish_to_stream(%RecordedEvent{} = event, state) do
    %RecordedEvent{stream_id: stream_id} = event

    Registry.dispatch(state.pubsub, stream_id, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:events, [event]})
    end)
  end

  defp start_observer(state) do
    opts = %ObserveEventsOptions{recursive: true}
    parent_pid = self()

    Task.start(fn ->
      case EventSourcingDB.observe_events(state.client, state.subject, opts) do
        {:ok, stream} ->
          try do
            stream
            |> Stream.each(fn
              %EventSourcingDB.Event{} = event ->
                GenServer.cast(parent_pid, {:stream_event, event})

              other ->
                IO.inspect(other, label: "non-event yielded")
            end)
            |> Stream.run()
          rescue
            e ->
              IO.inspect(e, label: "observe events crashed")
              GenServer.cast(parent_pid, {:stream_error, e})
          end

        {:error, reason} ->
          IO.inspect(reason.reason, label: "observe error")
          send(parent_pid, {:stream_error, reason})
      end
    end)
  end

  defp matches_stream_prefix?(%Event{} = event, stream_prefix) do
    subject_prefix = StreamMapper.to_subject(stream_prefix)
    String.starts_with?(event.subject, subject_prefix)
  end
end
