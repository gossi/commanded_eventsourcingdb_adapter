defmodule Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper do
  @moduledoc false

  @doc """
  Converts a stream identifier to an ESDB subject.
  """
  def to_subject(stream_prefix) do
    "/" <> stream_prefix
  end

  def to_subject(stream_prefix, :all), do: to_subject(stream_prefix)

  def to_subject(_stream_prefix, "/" <> _ = stream_uuid) do
    stream_uuid
  end

  def to_subject(stream_prefix, stream_uuid) do
    "/" <> stream_prefix <> stream_uuid
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

    # # Remove leading "/" if present
    # without_leading_slash = String.trim_leading(subject, "/")

    # # If stream_prefix is empty, return the rest
    # if stream_prefix == "" do
    #   without_leading_slash
    # else
    #   # Trim the stream_prefix (which doesn't have a leading "/")
    #   String.trim_leading(without_leading_slash, stream_prefix)
    # end
  end
end
