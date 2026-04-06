defmodule Commanded.EventStore.Adapters.EventSourcingDB.TransientSubscriber do
  @moduledoc """
  Handles transient subscriptions by directly observing events for a specific subject.
  """
  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper

  defmodule State do
    defstruct [
      :client,
      :subscriber,
      :subject,
      :stream_uuid,
      :stream_prefix
    ]
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    state = %State{
      client: Keyword.fetch!(opts, :client),
      subscriber: Keyword.fetch!(opts, :subscriber),
      subject: Keyword.fetch!(opts, :subject),
      stream_uuid: Keyword.fetch!(opts, :stream_uuid),
      stream_prefix: Keyword.get(opts, :stream_prefix, "")
    }

    {:ok, state, {:continue, :start_observing}}
  end

  @impl GenServer
  def handle_continue(:start_observing, state) do
    Task.start(fn -> observe_events(state) end)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  defp observe_events(state) do
    recursive = is_all?(state.subject, state.stream_prefix)
    options = %EventSourcingDB.ObserveEventsOptions{recursive: recursive}

    case EventSourcingDB.observe_events(state.client, state.subject, options) do
      {:ok, events_stream} ->
        stream_events(events_stream, state)

      {:error, reason} ->
        Logger.error("TransientSubscriber observe_events failed: #{inspect(reason)}")
        exit({:observe_events_failed, reason})
    end
  end

  defp is_all?(subject, stream_prefix),
    do: subject == "/#{stream_prefix}" or (stream_prefix == "" and subject == "/")

  defp stream_events(events_stream, state) do
    try do
      Enum.reduce_while(events_stream, 0, fn
        %EventSourcingDB.Event{id: "heartbeat"}, acc ->
          {:cont, acc}

        %EventSourcingDB.Event{} = event, acc ->
          version = acc + 1

          recorded_event =
            EventMapper.to_recorded_event(event, version, state.stream_prefix)

          is_all = is_all?(state.subject, state.stream_prefix)
          stream_matches = recorded_event.stream_id == state.stream_uuid
          should_send = is_all or stream_matches

          if should_send do
            send(state.subscriber, {:events, [recorded_event]})
          end

          {:cont, version}

        other, acc ->
          Logger.warning("TransientSubscriber unexpected: #{inspect(other)}")
          {:cont, acc}
      end)
    rescue
      e ->
        Logger.error("TransientSubscriber stream error: #{inspect(e)}")
        :ok
    end
  end
end
