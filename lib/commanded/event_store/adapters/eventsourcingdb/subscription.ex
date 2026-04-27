defmodule Commanded.EventStore.Adapters.EventSourcingDB.Subscription do
  @moduledoc false
  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.RecordedEvent

  defmodule State do
    @moduledoc false
    defstruct [
      :client,
      :event_store,
      :stream_prefix,
      :stream,
      :subscription_name,
      :start_from,
      :observer_ref,
      :concurrency_limit,
      :subscribers,
      :subscriber_index,
      stream_versions: %{},
      last_acked_event_number: 0,
      last_global_event_number: 0,
      retry_count: 0
    ]
  end

  def start_link(
        client,
        event_store,
        stream_prefix,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts
      ) do
    Logger.warning(
      "Subscription.start_link: stream_prefix=#{stream_prefix}, subscription_name=#{subscription_name}"
    )

    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    state = %State{
      client: client,
      event_store: event_store,
      stream_prefix: stream_prefix,
      stream: stream,
      subscription_name: subscription_name,
      start_from: start_from,
      concurrency_limit: concurrency_limit,
      subscribers: [{subscriber, Process.monitor(subscriber)}],
      subscriber_index: 0
    }

    Logger.warning("Subscription.start_link: state.stream_prefix=#{state.stream_prefix}")

    name =
      {:global, {event_store, stream, subscription_name, subscriber}}

    GenServer.start_link(__MODULE__, state, name: name)
  end

  @impl true
  def init(%State{} = state) do
    for {subscriber, _ref} <- state.subscribers do
      send(subscriber, {:subscribed, self()})
    end

    state = maybe_initialize_from_checkpoint(state)

    :ok = GenServer.cast(self(), :start_observer)

    {:ok, state}
  end

  @impl true
  def handle_call({:add_subscriber, new_subscriber}, _from, state) do
    existing_pids = for {pid, _} <- state.subscribers, do: pid

    if new_subscriber in existing_pids do
      {:reply, {:error, :subscription_already_exists}, state}
    else
      if length(state.subscribers) >= state.concurrency_limit do
        {:reply, {:error, :too_many_subscribers}, state}
      else
        ref = Process.monitor(new_subscriber)
        new_subscribers = [{new_subscriber, ref} | state.subscribers]
        send(new_subscriber, {:subscribed, self()})
        {:reply, {:ok, self()}, %{state | subscribers: new_subscribers}}
      end
    end
  end

  @impl true
  def handle_call(:get_subscribers, _from, state) do
    {:reply, state.subscribers, state}
  end

  @impl true
  def handle_call({:ack, %RecordedEvent{} = event}, _from, state) do
    Logger.warning(
      "handle_call(:ack): received ack for event_number=#{event.event_number}, stream=#{state.stream}"
    )

    checkpoint_number =
      if state.stream == :all or state.stream == "$all" do
        state.last_global_event_number
      else
        event.event_number
      end

    Logger.warning(
      "handle_call(:ack): storing checkpoint #{checkpoint_number} for subscription #{state.subscription_name}"
    )

    CheckpointStore.put(state.stream_prefix, state.subscription_name, checkpoint_number)
    {:reply, :ok, %{state | last_acked_event_number: checkpoint_number}}
  end

  def handle_call({:ack, _other} = msg, _from, state) do
    Logger.warning("handle_call(:ack): received unknown message: #{inspect(msg)}")
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:stream_event, %EventSourcingDB.Event{} = event}, state) do
    Logger.warning("handle_cast(:stream_event): received event id=#{event.id}")

    stream_id = StreamMapper.get_stream_id(event.subject, state.stream_prefix)

    current_version = Map.get(state.stream_versions, stream_id, 0)
    new_version = current_version + 1

    # For :all streams, track global event number sequentially (1, 2, 3...)
    new_global = state.last_global_event_number + 1

    Logger.warning(
      "handle_cast(:stream_event): stream_id=#{stream_id}, new_version=#{new_version}, new_global=#{new_global}"
    )

    event_number =
      if state.stream == :all or state.stream == "$all", do: new_global, else: new_version

    recorded_event =
      EventMapper.to_recorded_event(event, new_version, state.stream_prefix, event_number)

    Logger.warning("handle_cast(:stream_event): sending event with event_number=#{event_number}")

    if length(state.subscribers) > 0 do
      subscriber_idx = rem(state.subscriber_index, length(state.subscribers))
      {subscriber, _ref} = Enum.at(state.subscribers, subscriber_idx)
      send(subscriber, {:events, [recorded_event]})

      new_index = rem(subscriber_idx + 1, length(state.subscribers))

      {:noreply,
       %{
         state
         | stream_versions: Map.put(state.stream_versions, stream_id, new_version),
           subscriber_index: new_index,
           last_global_event_number: new_global
       }}
    else
      {:noreply,
       %{
         state
         | stream_versions: Map.put(state.stream_versions, stream_id, new_version),
           last_global_event_number: new_global
       }}
    end
  end

  @impl true
  def handle_cast({:stream_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{observer_ref: ref} = state) do
    Logger.warning("Observer stream closed. Retrying in 1000ms...")

    if state.retry_count >= 3 do
      Logger.error("Observer stream failed after 3 retries. Giving up.")
      {:noreply, state}
    else
      :timer.sleep(1_000)
      :ok = GenServer.cast(self(), :start_observer)
      {:noreply, %{state | retry_count: state.retry_count + 1}}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    new_subscribers = Enum.reject(state.subscribers, fn {_subscriber, r} -> r == ref end)

    if length(new_subscribers) == 0 do
      {:stop, reason, %{state | subscribers: []}}
    else
      {:noreply, %{state | subscribers: new_subscribers}}
    end
  end

  def handle_info(msg, state) do
    Logger.warning("handle_info: received unhandled message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_cast(:start_observer, state) do
    {:ok, pid} = start_observer(state)

    ref = Process.monitor(pid)

    {:noreply, %{state | observer_ref: ref, retry_count: 0}}
  end

  defp start_observer(state) do
    parent_pid = self()

    Task.start(fn ->
      opts = observe_options(state)
      subject = StreamMapper.to_subject(state.stream_prefix, state.stream)

      result = EventSourcingDB.observe_events(state.client, subject, opts)

      case result do
        {:ok, stream} ->
          try do
            Process.put(:observer_stream, true)

            stream
            |> Stream.each(fn
              %EventSourcingDB.Event{} = event ->
                if matches_stream_prefix?(event, state.stream_prefix) do
                  GenServer.cast(parent_pid, {:stream_event, event})
                end

              other ->
                :skip
            end)
            |> Stream.run()
          rescue
            e ->
              IO.inspect(e, label: "observe events crashed")
              GenServer.cast(parent_pid, {:stream_error, e})
          after
            Process.delete(:observer_stream)
          end

        {:error, reason} ->
          IO.inspect(reason, label: "observe error")
          send(parent_pid, {:stream_error, reason})
      end
    end)
  end

  defp matches_stream_prefix?(%EventSourcingDB.Event{} = event, stream_prefix) do
    subject_prefix = StreamMapper.to_subject(stream_prefix)
    String.starts_with?(event.subject, subject_prefix)
  end

  defp maybe_initialize_from_checkpoint(%State{} = state) do
    case CheckpointStore.get(state.stream_prefix, state.subscription_name) do
      {:ok, checkpoint} ->
        Logger.debug(
          "maybe_initialize_from_checkpoint: resuming from checkpoint #{checkpoint} for subscription #{state.subscription_name}"
        )

        # Always resume from checkpoint if one exists (persistent subscription behavior)
        %{state | last_acked_event_number: checkpoint, last_global_event_number: checkpoint}

      :error ->
        Logger.debug(
          "maybe_initialize_from_checkpoint: no checkpoint found for subscription #{state.subscription_name}"
        )

        # No checkpoint exists - use start_from parameter
        case state.start_from do
          :origin ->
            Logger.debug(
              "maybe_initialize_from_checkpoint(:origin): starting from origin"
            )

            %{state | stream_versions: %{}, last_acked_event_number: 0, last_global_event_number: 0}

          :current ->
            Logger.debug(
              "maybe_initialize_from_checkpoint(:current): no checkpoint, starting from origin"
            )

            %{state | stream_versions: %{}, last_acked_event_number: 0, last_global_event_number: 0}

          _ when is_integer(state.start_from) ->
            Logger.debug(
              "maybe_initialize_from_checkpoint: start_from=#{state.start_from}"
            )

            %{
              state
              | stream_versions: %{state.stream => state.start_from},
                last_acked_event_number: state.start_from,
                last_global_event_number: state.start_from
            }
        end
    end
  end

  defp observe_options(%State{start_from: :origin}) do
    %EventSourcingDB.ObserveEventsOptions{recursive: true}
  end

  defp observe_options(
         %State{
           start_from: :current,
           client: client
         } = state
       ) do
    event_number = state.last_acked_event_number

    case event_number_to_event_id(client, event_number) do
      {:ok, event_id} ->
        %EventSourcingDB.ObserveEventsOptions{
          recursive: true,
          lower_bound: %EventSourcingDB.BoundOptions{
            type: :exclusive,
            id: event_id
          }
        }

      :error ->
        %EventSourcingDB.ObserveEventsOptions{recursive: true}
    end
  end

  defp observe_options(
         %State{
           start_from: start_from,
           client: client
         } = state
       )
       when is_integer(start_from) do
    case event_number_to_event_id(client, start_from) do
      {:ok, event_id} ->
        %EventSourcingDB.ObserveEventsOptions{
          recursive: true,
          lower_bound: %EventSourcingDB.BoundOptions{
            type: :exclusive,
            id: event_id
          }
        }

      :error ->
        %EventSourcingDB.ObserveEventsOptions{recursive: true}
    end
  end

  defp event_number_to_event_id(_client, 0), do: :error

  defp event_number_to_event_id(client, event_number) when event_number > 0 do
    Logger.debug("event_number_to_event_id: looking up event at position #{event_number}")
    # event_number is 1-indexed, OFFSET is 0-indexed
    query = """
    FROM e IN events
    ORDER BY e.id ASC
    LIMIT 1
    OFFSET #{event_number - 1}
    SELECT e.id
    """

    case EventSourcingDB.run_eventql_query(client, query) do
      {:ok, stream} ->
        results = Enum.to_list(stream)
        Logger.debug("event_number_to_event_id: materialized results=#{inspect(results)}")

        case results do
          [%{"id" => event_id}] ->
            Logger.debug("event_number_to_event_id: found event_id=#{event_id}")
            {:ok, event_id}

          [] ->
            Logger.warning("event_number_to_event_id: empty results at position #{event_number}")
            :error

          other ->
            Logger.warning("event_number_to_event_id: unexpected result #{inspect(other)}")
            :error
        end

      {:error, reason} ->
        Logger.warning("event_number_to_event_id: query failed: #{inspect(reason)}")
        :error
    end
  end
end
