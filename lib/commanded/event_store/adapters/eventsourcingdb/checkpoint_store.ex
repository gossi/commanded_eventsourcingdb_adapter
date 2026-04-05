defmodule Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore do
  @moduledoc false

  @table_name __MODULE__

  def init do
    case :ets.info(@table_name, :name) do
      :undefined -> :ets.new(@table_name, [:set, :named_table, :public])
      _ -> :ok
    end
  end

  def store_checkpoint(subscription_name, stream_uuid, event_id, event_number) do
    key = {subscription_name, stream_uuid}
    value = %{event_id: event_id, event_number: event_number}
    :ets.insert(@table_name, {key, value})
    :ok
  end

  def get_checkpoint(subscription_name, stream_uuid) do
    key = {subscription_name, stream_uuid}

    case :ets.lookup(@table_name, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  def delete_checkpoint(subscription_name, stream_uuid) do
    key = {subscription_name, stream_uuid}
    :ets.delete(@table_name, key)
    :ok
  end
end
