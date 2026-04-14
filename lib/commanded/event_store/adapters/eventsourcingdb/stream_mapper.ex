defmodule Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper do
  @moduledoc false

  @doc """
  Converts a stream identifier to an ESDB subject.
  """
  def to_subject(stream_prefix) do
    to_subject(stream_prefix, "")
  end

  def to_subject(stream_prefix, :all), do: to_subject(stream_prefix)
  def to_subject(stream_prefix, "$all"), do: to_subject(stream_prefix)

  def to_subject(stream_prefix, stream_uuid) do
    sanitize_subject("/" <> sanitize_stream_prefix(stream_prefix) <> "/" <> stream_uuid)
  end

  @doc """
  Extracts the stream identifier from an ESDB subject.

  Handles various formats:
  - "/prefix/stream_id" with matching prefix -> "stream_id"
  - "/stream_id" with empty prefix -> "stream_id"
  - "prefix/stream_id" -> "stream_id" (if prefix doesn't start with /)
  """
  def get_stream_id(subject, stream_prefix \\ "") do
    subject
    |> String.replace(stream_prefix, "")
    |> String.replace("//", "/")
    |> String.trim_leading("/")
  end

  defp sanitize_stream_prefix(stream_prefix) do
    stream_prefix
    |> String.trim("/")
  end

  defp sanitize_subject(subject) do
    subject
    |> String.trim_trailing("/")
    |> String.pad_leading(1, "/")
    |> String.replace("//", "/")
  end
end
