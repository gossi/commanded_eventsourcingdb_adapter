defmodule Commanded.EventStore.Adapters.EventSourcingDB do
  @moduledoc """
  Documentation for `Commanded.EventStore.Adapters.EventSourcingDB`.
  """

  alias Commanded.EventStore.EventData
  alias Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore
  alias Commanded.EventStore.Adapters.EventSourcingDB.Config
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.ObserverProcess
  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionManager

  @behaviour Commanded.EventStore.Adapter

  @impl Commanded.EventStore.Adapter
  @spec child_spec(Commanded.Application.t(), Keyword.t()) ::
          {:ok, [:supervisor.child_spec() | {module, term} | module], map()}
  def child_spec(application, config) do
    event_store =
      case Keyword.get(config, :name) do
        nil -> Module.concat([application, EventSourcingDB])
        name -> Module.concat([name, EventSourcingDB])
      end

    esdb_config = Keyword.get(config, :client, [])

    child_spec = [
      Supervisor.child_spec(
        {Commanded.EventStore.Adapters.EventSourcingDB.Supervisor,
         Keyword.merge(config, event_store: event_store)},
        id: event_store
      )
    ]

    adapter_meta = %{
      event_store: event_store,
      client: Config.client(esdb_config),
      stream_prefix: Keyword.get(config, :stream_prefix, ""),
      source: Keyword.get(config, :source)
    }

    {:ok, child_spec, adapter_meta}
  end

  def ping(adapter_meta) do
    client = client(adapter_meta)
    EventSourcingDB.ping(client)
  end

  def append_to_stream(adapter_meta, subject, expected_version, events),
    do: append_to_stream(adapter_meta, subject, expected_version, events, [])

  @impl Commanded.EventStore.Adapter
  @spec append_to_stream(
          map(),
          String.t(),
          Commanded.EventStore.Adapter.expected_version(),
          list(EventData.t()),
          Keyword.t()
        ) ::
          :ok
          | {:error, :wrong_expected_version}
          | {:error, term()}
  def append_to_stream(
        adapter_meta,
        subject,
        expected_version,
        events,
        _opts
      ) do
    client = client(adapter_meta)
    stream_prefix = stream_prefix(adapter_meta)
    source = source(adapter_meta)
    subject = StreamMapper.to_subject(subject, stream_prefix)

    event_candidates =
      Enum.map(events, fn event_data ->
        metadata = event_data.metadata || %{}

        data =
          EventMapper.serialize_event_data(
            event_data.data,
            event_data.correlation_id,
            event_data.causation_id,
            metadata
          )

        %EventSourcingDB.EventCandidate{
          source: source,
          subject: subject,
          type: Map.get(metadata, "type", event_data.event_type),
          data: data
        }
      end)

    preconditions =
      expected_version_to_preconditions(subject, expected_version)

    case EventSourcingDB.write_events(client, event_candidates, preconditions) do
      {:ok, _events} -> :ok
      {:error, reason} -> handle_write_error(reason, expected_version)
    end
  end

  def stream_forward(adapter_meta, subject),
    do: stream_forward(adapter_meta, subject, 0, 1_000)

  def stream_forward(adapter_meta, subject, start_version),
    do: stream_forward(adapter_meta, subject, start_version, 1_000)

  @impl Commanded.EventStore.Adapter
  @spec stream_forward(
          map(),
          String.t(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          Enumerable.t()
          | {:error, :stream_not_found}
          | {:error, term()}
  def stream_forward(
        adapter_meta,
        subject,
        start_version,
        _read_batch_size
      ) do
    client = client(adapter_meta)
    stream_prefix = stream_prefix(adapter_meta)
    subject = StreamMapper.to_subject(subject, stream_prefix)

    case EventSourcingDB.read_events(client, subject) do
      {:ok, events_stream} ->
        events =
          events_stream
          |> Stream.filter(&match?(%EventSourcingDB.Event{}, &1))
          |> Enum.to_list()

        if Enum.empty?(events) do
          {:error, :stream_not_found}
        else
          events
          |> Stream.with_index(1)
          |> Stream.filter(fn {_event, index} -> index >= max(start_version, 1) end)
          |> Stream.map(fn {event, event_number} ->
            EventMapper.to_recorded_event(event, event_number, stream_prefix)
          end)
        end

      {:error, _reason} ->
        {:error, :stream_not_found}
    end
  end

  @impl Commanded.EventStore.Adapter
  def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, :all)

  @impl Commanded.EventStore.Adapter
  @spec subscribe(map(), String.t() | :all) ::
          :ok | {:error, term()}
  def subscribe(adapter_meta, stream_uuid) do
    client = client(adapter_meta)
    stream_prefix = stream_prefix(adapter_meta)
    subject = StreamMapper.to_subject(stream_uuid, stream_prefix)

    # Start a TransientSubscriber to observe events for this subscription
    {:ok, _pid} =
      Commanded.EventStore.Adapters.EventSourcingDB.TransientSubscriber.start_link(
        client: client,
        subscriber: self(),
        subject: subject,
        stream_uuid: stream_uuid,
        stream_prefix: stream_prefix
      )

    :ok
  end

  @impl Commanded.EventStore.Adapter
  @spec subscribe_to(
          map(),
          String.t() | :all,
          String.t(),
          pid(),
          Commanded.EventStore.Adapter.start_from(),
          Keyword.t()
        ) ::
          {:ok, pid()} | {:error, :subscription_already_exists} | {:error, term()}
  def subscribe_to(adapter_meta, stream_uuid, subscription_name, subscriber, start_from, opts) do
    event_store = server_name(adapter_meta)
    subscription_manager = Module.concat(event_store, :SubscriptionManager)
    stream_prefix = stream_prefix(adapter_meta)
    subject = StreamMapper.to_subject(stream_uuid, stream_prefix)

    case SubscriptionManager.create_subscription(
           subscription_manager,
           stream_uuid,
           subscription_name,
           subscriber,
           start_from,
           opts
         ) do
      {:ok, subscription} ->
        if Enum.count(subscription.subscribers) == 1 do
          {:ok, observer_pid} =
            ObserverProcess.start_link(
              client: client(adapter_meta),
              subscriber: subscriber,
              subject: subject,
              stream_uuid: stream_uuid,
              stream_prefix: stream_prefix,
              subscription_name: subscription_name,
              selector: Keyword.get(opts, :selector),
              checkpoint: subscription.checkpoint,
              event_store: event_store
            )

          {:ok, observer_pid}
        else
          observer_pid =
            get_observer_pid(event_store, subscription_name, stream_uuid, stream_prefix)

          {:ok, observer_pid}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_observer_pid(event_store, subscription_name, stream_uuid, stream_prefix) do
    registry = Module.concat([event_store, :ObserverProcesses])
    sanitized_prefix = String.replace(stream_prefix, "/", "_")
    unique_name = "#{subscription_name}:#{stream_uuid}:#{sanitized_prefix}"

    case Registry.lookup(registry, unique_name) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @impl Commanded.EventStore.Adapter
  @spec ack_event(map(), pid(), Commanded.EventStore.RecordedEvent.t()) :: :ok
  def ack_event(_adapter_meta, subscription_pid, event) do
    send(subscription_pid, {:ack, event})
    :ok
  end

  @impl Commanded.EventStore.Adapter
  @spec unsubscribe(map(), pid()) :: :ok
  def unsubscribe(adapter_meta, subscription_pid) when is_pid(subscription_pid) do
    event_store = server_name(adapter_meta)
    subscription_manager = Module.concat(event_store, :SubscriptionManager)

    SubscriptionManager.stop_observing_by_pid(subscription_manager, subscription_pid)
    :ok
  end

  def unsubscribe(_adapter_meta, _subscription), do: :ok

  @impl Commanded.EventStore.Adapter
  @spec delete_subscription(map(), String.t() | :all, String.t()) ::
          :ok | {:error, :subscription_not_found} | {:error, term()}
  def delete_subscription(adapter_meta, stream_uuid, subscription_name) do
    event_store = server_name(adapter_meta)
    subscription_manager = Module.concat(event_store, :SubscriptionManager)

    case SubscriptionManager.delete_subscription(
           subscription_manager,
           stream_uuid,
           subscription_name
         ) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :subscription_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Commanded.EventStore.Adapter
  @spec read_snapshot(map(), String.t()) ::
          {:ok, Commanded.EventStore.SnapshotData.t()} | {:error, :snapshot_not_found}
  def read_snapshot(_adapter_meta, _source_uuid) do
    {:error, :snapshots_not_supported}
  end

  @impl Commanded.EventStore.Adapter
  @spec record_snapshot(map(), Commanded.EventStore.SnapshotData.t()) ::
          :ok | {:error, term()}
  def record_snapshot(_adapter_meta, _snapshot_data) do
    {:error, :snapshots_not_supported}
  end

  @impl Commanded.EventStore.Adapter
  @spec delete_snapshot(map(), String.t()) ::
          :ok | {:error, term()}
  def delete_snapshot(_adapter_meta, _source_uuid) do
    {:error, :snapshots_not_supported}
  end

  defp client(adapter_meta), do: Map.fetch!(adapter_meta, :client)

  defp server_name(adapter_meta) do
    Map.fetch!(adapter_meta, :event_store)
  end

  defp stream_prefix(adapter_meta) do
    Map.get(adapter_meta, :stream_prefix, "")
  end

  defp source(adapter_meta) do
    Map.fetch!(adapter_meta, :source)
  end

  defp expected_version_to_preconditions(subject, :no_stream) do
    [%EventSourcingDB.IsSubjectPristine{subject: subject}]
  end

  defp expected_version_to_preconditions(subject, :stream_exists) do
    [%EventSourcingDB.IsSubjectPopulated{subject: subject}]
  end

  defp expected_version_to_preconditions(_subject, :any_version), do: []

  defp expected_version_to_preconditions(subject, 0) do
    [%EventSourcingDB.IsSubjectPristine{subject: subject}]
  end

  defp expected_version_to_preconditions(subject, expected_version)
       when is_integer(expected_version) and expected_version > 0 do
    [
      %EventSourcingDB.IsEventQLQueryTrue{
        query:
          "FROM e IN events WHERE e.subject == '#{subject}' PROJECT INTO COUNT() == #{expected_version}"
      }
    ]
  end

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, :no_stream) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :stream_exists}
      "state conflict: subject not found\n" -> {:error, :stream_not_found}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, :stream_exists) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :stream_not_found}
      "state conflict: subject not found\n" -> {:error, :stream_not_found}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, 0) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :wrong_expected_version}
      "state conflict: subject not found\n" -> {:error, :stream_not_found}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, expected_version)
       when is_integer(expected_version) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :wrong_expected_version}
      "state conflict: subject not found\n" -> {:error, :wrong_expected_version}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, _expected_version) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :wrong_expected_version}
      "state conflict: subject not found\n" -> {:error, :stream_not_found}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(reason, _expected_version), do: {:error, reason}
end
