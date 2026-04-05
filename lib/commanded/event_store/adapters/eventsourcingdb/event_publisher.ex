defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false

    defstruct [
      :client,
      :pubsub,
      :transient_pubsub,
      :subject,
      :last_event_id,
      :stream_versions,
      :global_event_number
    ]
  end

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher.State
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.RecordedEvent

  def start_link(opts) do
    state = %State{
      client: Keyword.fetch!(opts, :client),
      pubsub: Keyword.fetch!(opts, :pubsub),
      transient_pubsub: Keyword.get(opts, :transient_pubsub),
      subject: Keyword.get(opts, :subject, "/"),
      last_event_id: nil,
      stream_versions: %{},
      global_event_number: 0
    }

    GenServer.start_link(__MODULE__, state, name: Keyword.get(opts, :name))
  end

  @impl GenServer
  def init(%State{} = state) do
    Logger.info("EventPublisher started, observing: #{state.subject}")
    {:ok, state, {:continue, :start}}
  end

  @impl GenServer
  def handle_continue(:start, state) do
    Task.start(fn -> stream_events(state) end)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  defp stream_events(state) do
    options = %EventSourcingDB.ObserveEventsOptions{recursive: true}
    Logger.info("EventPublisher streaming from: #{state.subject}")
    stream_events_loop(state, options)
  end

  defp stream_events_loop(state, options, retries \\ 0) do
    Logger.info("Calling observe_events")

    case EventSourcingDB.observe_events(state.client, state.subject, options) do
      {:ok, events_stream} ->
        Logger.info("EventPublisher observing events stream")
        stream_state = %{state | stream_versions: %{}, global_event_number: 0}

        consume_events(events_stream, stream_state)

        Logger.info("EventPublisher stream ended, retrying...")
        :timer.sleep(1000)
        stream_events_loop(state, options, retries + 1)

      {:error, reason} ->
        Logger.error("ESDB observe_events failed (attempt #{retries + 1}): #{inspect(reason)}")

        if retries < 10 do
          :timer.sleep(1000 * (retries + 1))
          stream_events_loop(state, options, retries + 1)
        else
          Logger.error("ESDB observe_events failed after #{retries + 1} retries")
          exit({:observe_events_failed, reason})
        end
    end
  end

  defp consume_events(events_stream, stream_state) do
    try do
      Enum.reduce_while(events_stream, stream_state, fn
        %EventSourcingDB.Event{id: "heartbeat"}, acc ->
          {:cont, acc}

        %EventSourcingDB.Event{} = event, acc ->
          Logger.info("EventPublisher got event: #{inspect(event.id)}")
          {:cont, dispatch_event(event, acc)}

        other, acc ->
          Logger.warning("EventPublisher unexpected: #{inspect(other)}")
          {:cont, acc}
      end)
    rescue
      e ->
        Logger.error("Stream error: #{inspect(e)}")
        stream_state
    end
  end

  defp dispatch_event(event, state) do
    Logger.info("EventPublisher dispatching: #{event.subject}")

    stream_id = event.subject

    {version, new_versions} =
      Map.get_and_update(state.stream_versions, stream_id, fn v ->
        {if(v, do: v + 1, else: 1), if(v, do: v + 1, else: 1)}
      end)

    global_event_number = state.global_event_number + 1

    state = %{
      state
      | stream_versions: new_versions,
        last_event_id: event.id,
        global_event_number: global_event_number
    }

    recorded_event = %RecordedEvent{
      event_id: "#{event.source}/#{event.id}",
      event_number: global_event_number,
      stream_version: version,
      stream_id: String.trim_leading(stream_id, "/"),
      event_type: event.type,
      data: event.data,
      correlation_id: nil,
      causation_id: nil,
      metadata: %{},
      created_at: DateTime.utc_now()
    }

    Logger.info("Publishing recorded event: #{inspect(recorded_event.event_number)}")

    publish_to_transient(recorded_event, state.transient_pubsub, event.subject)

    state
  end

  defp publish_to_transient(%RecordedEvent{} = recorded_event, nil, _subject), do: :ok

  defp publish_to_transient(%RecordedEvent{} = recorded_event, transient_pubsub, subject) do
    Registry.dispatch(transient_pubsub, subject, fn entries ->
      for {pid, _} <- entries do
        send(pid, {:events, [recorded_event]})
      end
    end)

    Registry.dispatch(transient_pubsub, :all, fn entries ->
      for {pid, _} <- entries do
        send(pid, {:events, [recorded_event]})
      end
    end)
  end
end
