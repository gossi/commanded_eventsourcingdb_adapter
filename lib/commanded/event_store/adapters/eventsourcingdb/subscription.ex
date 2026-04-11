defmodule Commanded.EventStore.Adapters.EventSourcingDB.Subscription do
  @moduledoc """
  GenServer that observes events from EventSourcingDB.

  This process subscribes to ESDB's observe_events endpoint and forwards
  received events to the subscriber process.
  """

  use GenServer

  alias EventSourcingDB.Event
  alias EventSourcingDB.ObserveEventsOptions
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper

  defstruct [
    :client,
    :stream_prefix,
    :stream_uuid,
    :subject,
    :subscriber,
    :subscriber_ref,
    :start_from,
    :observer_active
  ]

  @type t :: %__MODULE__{
          client: EventSourcingDB.Client.t(),
          stream_prefix: String.t(),
          stream_uuid: String.t(),
          subject: String.t(),
          subscriber: pid(),
          subscriber_ref: reference(),
          start_from: :origin | :current | non_neg_integer(),
          observer_active: boolean()
        }

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :client)
    stream_uuid = Keyword.fetch!(opts, :stream_uuid)
    stream_prefix = Keyword.get(opts, :stream_prefix, "")
    subscriber = Keyword.get(opts, :subscriber) || self()
    start_from = Keyword.get(opts, :start_from, :origin)
    subject = StreamMapper.to_subject(stream_prefix, stream_uuid)

    state = %__MODULE__{
      client: client,
      stream_prefix: stream_prefix,
      stream_uuid: stream_uuid,
      subject: subject,
      subscriber: subscriber,
      subscriber_ref: Process.monitor(subscriber),
      start_from: start_from,
      observer_active: false
    }

    send(self(), :start_observer)
    {:ok, state}
  end

  @impl true
  def handle_info(:start_observer, state) do
    start_observer(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:stream_event, %EventSourcingDB.Event{} = event}, state) do
    forward_event(event, state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:stream_error, reason}, state) do
    IO.puts("observe_events failed: #{inspect(reason)}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    if ref == state.subscriber_ref do
      {:stop, reason, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    IO.inspect(msg, label: "msg")
    {:noreply, state}
  end

  defp start_observer(state) do
    opts = %ObserveEventsOptions{recursive: true}
    parent_pid = self()

    result =
      Task.start(fn ->
        # Call observe_events HERE - in the Task process
        case EventSourcingDB.observe_events(state.client, state.subject, opts) do
          {:ok, stream} ->
            try do
              stream
              |> Stream.each(fn
                %EventSourcingDB.Event{} = event ->
                  send(parent_pid, {:stream_event, event})

                other ->
                  IO.inspect(other, label: "non-event yielded")
              end)
              |> Stream.run()
            rescue
              e ->
                IO.inspect(e, label: "observe events crashed")
                send(parent_pid, {:stream_error, e})
            end

          {:error, reason} ->
            send(parent_pid, {:stream_error, reason})
        end
      end)
  end

  defp forward_event(%Event{} = event, state) do
    recorded_event = EventMapper.to_recorded_event(event, event.id, state.stream_prefix)
    send(state.subscriber, {:events, [recorded_event]})
  end
end
