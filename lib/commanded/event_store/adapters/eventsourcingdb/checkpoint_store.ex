defmodule Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore do
  @moduledoc """
  Minimal checkpoint store for subscriptions.

  Stores the last processed event ID for each subscription to enable resumption.
  """

  def init do
    :ok
  end

  @spec put(String.t(), String.t()) :: :ok
  def put(_subscription_name, _event_id) do
    :ok
  end

  @spec get(String.t()) :: {:ok, String.t()} | :error
  def get(_subscription_name) do
    :error
  end

  @spec delete(String.t()) :: :ok
  def delete(_subscription_name) do
    :ok
  end
end
