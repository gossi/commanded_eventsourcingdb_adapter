defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher do
  @moduledoc """
  GenServer that observes events from EventSourcingDB.

  This process subscribes to ESDB's observe_events endpoint and forwards
  received events to the subscriber process.
  """

  use GenServer

  alias Commanded.EventStore.RecordedEvent
  alias EventSourcingDB.Event
  alias EventSourcingDB.ObserveEventsOptions
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :pubsub,
      :stream_prefix,
      :observer_ref
    ]
  end

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link({client, event_store, pubsub_name, stream_prefix}, opts \\ []) do
    state = %State{
      client: client,
      event_store: event_store,
      pubsub: pubsub_name,
      stream_prefix: stream_prefix
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
  def handle_cast({:stream_event, %EventSourcingDB.Event{} = event}, state) do
    publish_event(event, state)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_cast({:stream_error, reason}, state) do
    IO.puts("observe_events failed: #{inspect(reason)}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{observer_ref: ref} = state) do
    reconnect_delay = 1_000

    Logger.warn("Subscription to EventStore is down. Will retry in #{reconnect_delay} ms.")

    :timer.sleep(reconnect_delay)

    :ok = GenServer.cast(self(), :start_observer)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    IO.inspect(msg, label: "msg")
    {:noreply, state}
  end

  defp start_observer(state) do
    opts = %ObserveEventsOptions{recursive: true}
    parent_pid = self()

    Task.start(fn ->
      # Call observe_events HERE - in the Task process
      case EventSourcingDB.observe_events(
             state.client,
             StreamMapper.to_subject(state.stream_prefix),
             opts
           ) do
        {:ok, stream} ->
          try do
            stream
            |> Stream.each(fn
              %EventSourcingDB.Event{} = event ->
                IO.inspect(
                  String.starts_with?(
                    event.subject,
                    StreamMapper.to_subject(state.stream_prefix)
                  ),
                  label: "event starts with stream prefix"
                )

                if String.starts_with?(
                     event.subject,
                     StreamMapper.to_subject(state.stream_prefix)
                   ) do
                  GenServer.cast(parent_pid, {:stream_event, event})
                  # send(parent_pid, {:stream_event, event})
                end

              other ->
                IO.inspect(other, label: "non-event yielded")
            end)
            |> Stream.run()
          rescue
            e ->
              IO.inspect(e, label: "observe events crashed")
              GenServer.cast(parent_pid, {:stream_error, e})
              # send(parent_pid, {:stream_error, e})
          end

        {:error, reason} ->
          IO.inspect(reason.reason, label: "observe error")
          send(parent_pid, {:stream_error, reason})
      end
    end)
  end

  defp publish_event(%Event{} = event, state) do
    recorded_event = EventMapper.to_recorded_event(event, event.id, state.stream_prefix)

    :ok = publish_to_all(recorded_event, state)
    :ok = publish_to_stream(recorded_event, state)
  end

  defp publish_to_all(%RecordedEvent{} = event, state) do
    IO.inspect(event, label: "publish_to_all")

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
end
