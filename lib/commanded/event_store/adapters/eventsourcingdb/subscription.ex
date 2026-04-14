defmodule Commanded.EventStore.Adapters.EventSourcingDB.Subscription do
  @moduledoc false
  use GenServer

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :stream_prefix,
      :subscription_name,
      :subscriber,
      :index,
      :stream,
      :start_from,
      :subscriber_ref,
      :observer_ref
    ]
  end

  def start_link(
        client,
        stream_prefix,
        subscription_name,
        subscriber,
        stream,
        start_from,
        index,
        opts \\ []
      ) do
    state = %State{
      client: client,
      stream_prefix: stream_prefix,
      subscription_name: subscription_name,
      subscriber: subscriber,
      stream: stream,
      start_from: start_from,
      index: index,
      subscriber_ref: Process.monitor(subscriber)
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(%State{} = state) do
    send(state.subscriber, {:subscribed, self()})

    :ok = GenServer.cast(self(), :start_observer)

    {:ok, state}
  end

  @impl true
  def handle_cast({:stream_event, %EventSourcingDB.Event{} = event}, state) do
    recorded_event = EventMapper.to_recorded_event(event, event.id, state.stream_prefix)

    send(state.subscriber, {:events, [recorded_event]})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    IO.inspect({ref, pid, reason}, label: "terminated")

    if ref == state.subscriber_ref do
      {:stop, reason, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:stream_error, reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_cast(:start_observer, state) do
    {:ok, pid} = start_observer(state)

    ref = Process.monitor(pid)

    {:noreply, %{state | observer_ref: ref}}
  end

  defp start_observer(state) do
    opts = %EventSourcingDB.ObserveEventsOptions{recursive: true}
    parent_pid = self()

    Task.start(fn ->
      subject = StreamMapper.to_subject(state.stream_prefix, state.stream)

      case EventSourcingDB.observe_events(state.client, subject, opts) do
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

  defp matches_stream_prefix?(%EventSourcingDB.Event{} = event, stream_prefix) do
    subject_prefix = StreamMapper.to_subject(stream_prefix)
    String.starts_with?(event.subject, subject_prefix)
  end
end
