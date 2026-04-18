defmodule Commanded.EventStore.Adapters.EventSourcingDB do
  @moduledoc """
  Documentation for `Commanded.EventStore.Adapters.EventSourcingDB`.
  """

  alias Commanded.EventStore.EventData
  alias Commanded.EventStore.Adapters.EventSourcingDB.Config
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.Adapters.EventSourcingDB.SubscriptionSupervisor

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
      source: Keyword.get(config, :source),
      observer_registry: Module.concat([event_store, ObserverRegistry]),
      subscription_registry: Module.concat([event_store, SubscriptionRegistry])
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
    subject = StreamMapper.to_subject(stream_prefix, subject)

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
    subject = StreamMapper.to_subject(stream_prefix, subject)

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
          |> Stream.map(fn {event, stream_version} ->
            EventMapper.to_recorded_event(event, stream_version, stream_prefix)
          end)
        end

      {:error, _reason} ->
        {:error, :stream_not_found}
    end
  end

  @impl Commanded.EventStore.Adapter
  # apparently Registry can't take an atom as string, so we use the same special
  # string as the extreme adapter
  def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, "$all")

  @impl Commanded.EventStore.Adapter
  @spec subscribe(map(), String.t()) :: :ok | {:error, term()}
  def subscribe(adapter_meta, stream_uuid) do
    event_store = server_name(adapter_meta)
    pubsub_name = Module.concat([event_store, PubSub])

    with {:ok, _} <- Registry.register(pubsub_name, stream_uuid, []) do
      :ok
    end
  end

  @impl Commanded.EventStore.Adapter
  @spec subscribe_to(
          map(),
          :all,
          String.t(),
          pid(),
          Commanded.EventStore.Adapter.start_from(),
          Keyword.t()
        ) :: {:ok, pid()} | {:error, :subscription_already_exists} | {:error, term()}
  def subscribe_to(adapter_meta, :all, subscription_name, subscriber, start_from, opts) do
    subscribe_to(adapter_meta, "$all", subscription_name, subscriber, start_from, opts)
  end

  @impl Commanded.EventStore.Adapter
  @spec subscribe_to(
          map(),
          String.t(),
          String.t(),
          pid(),
          Commanded.EventStore.Adapter.start_from(),
          Keyword.t()
        ) :: {:ok, pid()} | {:error, :subscription_already_exists} | {:error, term()}
  def subscribe_to(adapter_meta, stream, subscription_name, subscriber, start_from, opts) do
    # client = client(adapter_meta)
    # stream_prefix = stream_prefix(adapter_meta)
    #
    # observer_registry = observer_registry(adapter_meta)
    event_store = server_name(adapter_meta)
    subscription_registry = subscription_registry(adapter_meta)

    start_result =
      SubscriptionSupervisor.start_subscription(
        event_store,
        stream,
        subscription_name,
        subscriber,
        start_from,
        opts
      )

    IO.inspect(start_result, label: "start result from subscribe_to()")

    case start_result do
      {:ok, pid} ->
        Registry.register(subscription_registry, {stream, subscription_name}, pid)
        {:ok, pid}
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
    SubscriptionSupervisor.stop_subscription(event_store, subscription_pid)
  end

  @impl Commanded.EventStore.Adapter
  @spec delete_subscription(map(), String.t() | :all, String.t()) ::
          :ok | {:error, :subscription_not_found} | {:error, term()}
  def delete_subscription(adapter_meta, stream_uuid, subscription_name) do
    event_store = server_name(adapter_meta)
    subscription_registry = subscription_registry(adapter_meta)

    case Registry.lookup(subscription_registry, {stream_uuid, subscription_name}) do
      [{pid, _}] ->
        SubscriptionSupervisor.stop_subscription(event_store, pid)
        SubscriptionSupervisor.delete_subscription(event_store, stream_uuid, subscription_name)
        :ok

      [] ->
        {:error, :subscription_not_found}
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

  defp observer_registry(adapter_meta) do
    Map.fetch!(adapter_meta, :observer_registry)
  end

  defp subscription_registry(adapter_meta) do
    event_store = server_name(adapter_meta)

    Module.concat([event_store, SubscriptionRegistry])
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

  defp handle_write_error(%EventSourcingDB.Errors.ApiError{reason: reason}, expected_version)
       when is_integer(expected_version) do
    case reason do
      "state conflict: precondition failed\n" -> {:error, :wrong_expected_version}
      "state conflict: subject not found\n" -> {:error, :wrong_expected_version}
      _ -> {:error, reason}
    end
  end

  defp handle_write_error(reason, _expected_version), do: {:error, reason}
end
