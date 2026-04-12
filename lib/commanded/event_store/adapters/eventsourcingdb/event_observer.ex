defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventObserver do
  @moduledoc """
  GenServer that observes events from EventSourcingDB.

  This process subscribes to ESDB's observe_events endpoint and forwards
  received events to the EventPublisher after converting them to
  RecordedEvent format.

  Responsibilities:
  - Self-restart on crash (via :DOWN handling)
  - Filter events by stream_prefix
  - Convert ESDB Event to RecordedEvent
  - Lookup publisher via ObserverRegistry before forwarding
  """

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias EventSourcingDB.Event
  alias EventSourcingDB.ObserveEventsOptions

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :stream_prefix,
      :subject,
      :observer_registry,
      :observer_ref
    ]
  end

  @spec start_link(
          {EventSourcingDB.Client.t(), pid(), String.t(), String.t()},
          GenServer.options()
        ) ::
          GenServer.on_start()
  def start_link({client, event_store, observer_registry_name, stream_prefix}, opts \\ []) do
    subject = StreamMapper.to_subject(stream_prefix)

    state = %State{
      client: client,
      event_store: event_store,
      stream_prefix: stream_prefix,
      subject: subject,
      observer_registry: observer_registry_name
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(%State{} = state) do
    :ok = GenServer.cast(self(), :start_observer)

    {:ok, state}
  end

  @impl true
  def handle_cast(:start_observer, state) do
    {:ok, pid} = start_observer(state)

    ref = Process.monitor(pid)

    {:noreply, %{state | observer_ref: ref}}
  end

  @impl true
  def handle_cast({:stream_event, %Event{} = event}, state) do
    recorded_event = EventMapper.to_recorded_event(event, event.id, state.stream_prefix)

    Registry.dispatch(state.observer_registry, state.stream_prefix, fn entries ->
      for {pid, _} <- entries, do: GenServer.cast(pid, {:stream_event, recorded_event})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{observer_ref: ref} = state) do
    reconnect_delay = 1_000

    Logger.warning("Observer to EventStore is down. Will retry in #{reconnect_delay} ms.")

    :timer.sleep(reconnect_delay)

    :ok = GenServer.cast(self(), :start_observer)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    IO.inspect(msg, label: "EventObserver msg")
    {:noreply, state}
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
                if matches_stream_prefix?(event, state.stream_prefix) do
                  GenServer.cast(parent_pid, {:stream_event, event})
                end

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
