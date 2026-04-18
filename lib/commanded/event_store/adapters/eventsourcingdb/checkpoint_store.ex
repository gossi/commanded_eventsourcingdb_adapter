defmodule Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore do
  @moduledoc """
  ETS-backed checkpoint store for subscriptions.

  Stores the last processed event_number for each subscription to enable resumption.
  Key is subscription_name (global, not per-stream).
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

  @spec put(String.t(), non_neg_integer()) :: :ok
  def put(subscription_name, last_seen_event_number) do
    ensure_table_exists()
    :ets.insert(@table_name, {subscription_name, last_seen_event_number})
    :ok
  end

  @spec get(String.t()) :: {:ok, non_neg_integer()} | :error
  def get(subscription_name) do
    ensure_table_exists()

    case :ets.lookup(@table_name, subscription_name) do
      [{_key, last_seen_event_number}] ->
        {:ok, last_seen_event_number}

      [] ->
        :error
    end
  end

  @spec delete(String.t()) :: :ok
  def delete(subscription_name) do
    ensure_table_exists()
    :ets.delete(@table_name, subscription_name)
    :ok
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
