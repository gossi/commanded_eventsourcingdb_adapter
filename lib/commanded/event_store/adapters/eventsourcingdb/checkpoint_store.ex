defmodule Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore do
  @moduledoc false
  @doc """
  ETS-backed checkpoint store for persistent subscriptions.

  Stores the last acknowledged ESDB event id for each subscription so that the
  subscription can resume from the next event after a restart. The key is
  prefixed with the `stream_prefix` to avoid collisions between commanded
  applications sharing the same VM.
  """

  @table_name :esdb_adapter_checkpoints

  @spec init() :: :ok
  def init do
    ensure_table_exists()
    :ok
  end

  @spec put(String.t(), String.t(), String.t()) :: :ok
  def put(stream_prefix, subscription_name, event_id) do
    ensure_table_exists()
    :ets.insert(@table_name, {checkpoint_key(stream_prefix, subscription_name), event_id})
    :ok
  end

  @spec get(String.t(), String.t()) :: {:ok, String.t()} | :error
  def get(stream_prefix, subscription_name) do
    ensure_table_exists()

    case :ets.lookup(@table_name, checkpoint_key(stream_prefix, subscription_name)) do
      [{_key, event_id}] -> {:ok, event_id}
      [] -> :error
    end
  end

  @spec delete(String.t(), String.t()) :: :ok
  def delete(stream_prefix, subscription_name) do
    ensure_table_exists()
    :ets.delete(@table_name, checkpoint_key(stream_prefix, subscription_name))
    :ok
  end

  defp checkpoint_key(stream_prefix, subscription_name) do
    "#{stream_prefix}:#{subscription_name}"
  end

  defp ensure_table_exists do
    case :ets.info(@table_name) do
      :undefined -> :ets.new(@table_name, [:set, :named_table, :public])
      _ -> :ok
    end
  end
end
