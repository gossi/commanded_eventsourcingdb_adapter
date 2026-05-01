defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher do
  @moduledoc false
  @doc """
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

  @observer_restart_delay 250

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :pubsub,
      :observer_registry,
      :stream_prefix,
      :subject,
      :observer_pid,
      :observer_ref,
      stream_versions: %{}
    ]
  end

  @spec start_link(
          {EventSourcingDB.Client.t(), atom(), atom(), atom(), String.t()},
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
    {:ok, state, {:continue, :start_observer}}
  end

  @impl true
  def handle_continue(:start_observer, state) do
    {:noreply, start_observer(state)}
  end

  @impl true
  def handle_cast({:stream_event, %Event{} = event}, state) do
    if matches_stream_prefix?(event, state.stream_prefix) do
      stream_id = StreamMapper.get_stream_id(event.subject, state.stream_prefix)
      new_version = Map.get(state.stream_versions, stream_id, 0) + 1
      recorded_event = EventMapper.to_recorded_event(event, new_version, state.stream_prefix)
      :ok = publish_event(recorded_event, state)

      {:noreply,
       %{state | stream_versions: Map.put(state.stream_versions, stream_id, new_version)}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{observer_ref: ref} = state) do
    Process.send_after(self(), :restart_observer, @observer_restart_delay)
    {:noreply, %{state | observer_pid: nil, observer_ref: nil}}
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

  defp start_observer(%State{} = state) do
    parent = self()
    client = state.client
    subject = state.subject
    opts = %ObserveEventsOptions{recursive: true}

    {pid, ref} = spawn_monitor(fn -> run_observer(parent, client, subject, opts) end)

    %{state | observer_pid: pid, observer_ref: ref}
  end

  defp run_observer(parent, client, subject, opts) do
    case EventSourcingDB.observe_events(client, subject, opts) do
      {:ok, stream} -> consume_stream(parent, stream)
      {:error, _reason} -> :ok
    end
  end

  defp consume_stream(parent, stream) do
    try do
      Enum.each(stream, fn
        %Event{} = event -> GenServer.cast(parent, {:stream_event, event})
        _other -> :ok
      end)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp matches_stream_prefix?(%Event{} = event, stream_prefix) do
    subject_prefix = StreamMapper.to_subject(stream_prefix)
    String.starts_with?(event.subject, subject_prefix)
  end
end
