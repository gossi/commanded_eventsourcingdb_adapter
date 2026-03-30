defmodule Commanded.EventStore.Adapters.EventSourcingDB do
  @moduledoc """
  Documentation for `Commanded.EventStore.Adapters.EventSourcingDB`.
  """

  alias Commanded.EventStore.EventData
  alias Commanded.EventStore.Adapters.EventSourcingDB.Config

  @behaviour Commanded.EventStore.Adapter

  @impl Commanded.EventStore.Adapter
  # @spec child_spec(application, config) ::
  #         {:ok, [:supervisor.child_spec() | {module, term} | module], adapter_meta}
  def child_spec(_application, config) do
    # event_store =
    #   case Keyword.get(config, :name) do
    #     nil -> Module.concat([application, Extreme])
    #     name -> Module.concat([name, Extreme])
    #   end

    # Rename `prefix` config to `stream_prefix`
    # config =
    #   case Keyword.pop(config, :prefix) do
    #     {nil, config} -> config
    #     {prefix, config} -> Keyword.put(config, :stream_prefix, prefix)
    #   end
    client = Keyword.get(config, :client)

    # child_spec = [
    #   Supervisor.child_spec(
    #     {Commanded.EventStore.Adapters.EventSourcingDB.Supervisor, config},
    #     id: client
    #   )
    # ]

    child_spec = config

    adapter_meta = %{
      client: Config.client(client)
    }

    {:ok, child_spec, adapter_meta}
  end

  def ping(adapter_meta) do
    client = client(adapter_meta)

    EventSourcingDB.ping(client)
  end

  @impl
  # @spec append_to_stream(
  #         adapter_meta,
  #         stream_uuid,
  #         expected_version,
  #         events :: list(EventData.t()),
  #         opts :: Keyword.t()
  #       ) ::
  #         :ok
  #         | {:error, :wrong_expected_version}
  #         | {:error, error}
  def append_to_stream(
        adapter_meta,
        stream_uuid,
        expected_version,
        events,
        opts
      ) do
  end

  # @impl
  # @spec stream_forward(
  #         adapter_meta,
  #         stream_uuid,
  #         start_version :: non_neg_integer,
  #         read_batch_size :: non_neg_integer
  #       ) ::
  #         Enumerable.t()
  #         | {:error, :stream_not_found}
  #         | {:error, error}
  # def stream_forward(
  #       adapter_meta,
  #       stream_uuid,
  #       start_version,
  #       read_batch_size
  #     ) do
  # end

  # region subscriptions

  # @impl
  # def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, "/")

  # @impl
  # @spec subscribe(adapter_meta, stream_uuid | :all) ::
  #         :ok | {:error, error}
  # def subscribe(adapter_meta, stream_uuid) do
  # end

  # @impl
  # @spec subscribe_to(
  #         adapter_meta,
  #         stream_uuid | :all,
  #         subscription_name,
  #         subscriber,
  #         start_from,
  #         opts :: Keyword.t()
  #       ) ::
  #         {:ok, subscription}
  #         | {:error, :subscription_already_exists}
  #         | {:error, error}
  # def subscribe_to(
  #       adapter_meta,
  #       stream_uuid,
  #       subscription_name,
  #       subscriber,
  #       start_from,
  #       opts
  #     ) do
  # end

  # @impl
  # @spec ack_event(adapter_meta, pid, RecordedEvent.t()) :: :ok
  # def ack_event(adapter_meta, pid, event) do
  # end

  # @impl
  # @spec unsubscribe(adapter_meta, subscription) :: :ok
  # def unsubscribe(adapter_meta, subscription) do
  # end

  # @impl
  # @spec delete_subscription(
  #         adapter_meta,
  #         stream_uuid | :all,
  #         subscription_name
  #       ) ::
  #         :ok | {:error, :subscription_not_found} | {:error, error}
  # def delete_subscription(
  #       adapter_meta,
  #       stream_uuid,
  #       subscription_name
  #     ) do
  # end

  # region snapshots

  # @impl
  # @spec read_snapshot(adapter_meta, source_uuid) ::
  #         {:ok, SnapshotData.t()} | {:error, :snapshot_not_found}
  # def read_snapshot(adapter_meta, source_uuid) do
  # end

  # @impl
  # @spec record_snapshot(adapter_meta, SnapshotData.t()) ::
  #         :ok | {:error, error}
  # def record_snapshot(adapter_meta, snapshot_data) do
  # end

  # @impl
  # @spec delete_snapshot(adapter_meta, source_uuid) ::
  #         :ok | {:error, error}
  # def delete_snapshot(adapter_meta, source_uuid) do
  # end

  # region support
  defp client(adapter_meta), do: Map.fetch!(adapter_meta, :client)
end
