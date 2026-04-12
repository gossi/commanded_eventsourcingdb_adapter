defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventPublisher do
  @moduledoc """
  GenServer that receives events from EventObserver and distributes them
  to subscribers via the Registry.

  Responsibilities:
  - Registers with ObserverRegistry for EventObserver to find
  - Receives RecordedEvent from EventObserver
  - Publishes to $all registry (all streams)
  - Publishes to stream-specific registry
  """

  use GenServer

  alias Commanded.EventStore.RecordedEvent

  defmodule State do
    @moduledoc false
    defstruct [
      :event_store,
      :pubsub,
      :observer_registry,
      :stream_prefix
    ]
  end

  @spec start_link(
          {EventSourcingDB.Client.t(), atom(), atom(), atom(), String.t()},
          GenServer.options()
        ) ::
          GenServer.on_start()
  def start_link(
        {_client, event_store, pubsub_name, observer_registry_name, stream_prefix},
        opts \\ []
      ) do
    state = %State{
      event_store: event_store,
      pubsub: pubsub_name,
      observer_registry: observer_registry_name,
      stream_prefix: stream_prefix
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(%State{} = state) do
    Registry.register(state.observer_registry, state.stream_prefix, self())
    {:ok, state}
  end

  @impl true
  def handle_cast({:stream_event, %RecordedEvent{} = event}, state) do
    publish_event(event, state)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
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
end
