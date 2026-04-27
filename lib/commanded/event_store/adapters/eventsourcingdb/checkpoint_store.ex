defmodule Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore do
  @moduledoc """
  ETS-backed checkpoint store for subscriptions.

  Stores the last processed event_number for each subscription to enable resumption.
  Key includes stream_prefix to avoid collisions between test runs.
  """

  @table_name :esdb_adapter_checkpoints

  @spec init() :: :ok
  def init do
    case :ets.info(@table_name) do
      :undefined ->
        :ets.new(@table_name, [:set, :named_table, :public])
        :ok

      _ ->
        :ok
    end
  end

  @spec put(String.t(), String.t(), non_neg_integer()) :: :ok
  def put(stream_prefix, subscription_name, last_seen_event_number) do
    ensure_table_exists()
    key = checkpoint_key(stream_prefix, subscription_name)

    require Logger
    Logger.warning("CheckpointStore.put: storing key=#{inspect(key)}, value=#{last_seen_event_number}")

    :ets.insert(@table_name, {key, last_seen_event_number})
    :ok
  end

  @spec get(String.t(), String.t()) :: {:ok, non_neg_integer()} | :error
  def get(stream_prefix, subscription_name) do
    ensure_table_exists()
    key = checkpoint_key(stream_prefix, subscription_name)

    require Logger
    Logger.debug("CheckpointStore.get: looking up key=#{key}")

    case :ets.lookup(@table_name, key) do
      [{^key, last_seen_event_number}] ->
        Logger.debug("CheckpointStore.get: found key=#{key}, value=#{last_seen_event_number}")
        {:ok, last_seen_event_number}

      [] ->
        Logger.debug("CheckpointStore.get: key=#{key} not found")
        :error
    end
  end

  @spec delete(String.t(), String.t()) :: :ok
  def delete(stream_prefix, subscription_name) do
    ensure_table_exists()
    key = checkpoint_key(stream_prefix, subscription_name)

    require Logger
    Logger.warning("CheckpointStore.delete: deleting key=#{inspect(key)}")
    :ets.delete(@table_name, key)
    :ok
  end

  defp checkpoint_key(stream_prefix, subscription_name) do
    "#{stream_prefix}:#{subscription_name}"
  end

  defp ensure_table_exists do
    case :ets.info(@table_name) do
      :undefined ->
        :ets.new(@table_name, [:set, :named_table, :public])

      _ ->
        :ok
    end
  end
end
