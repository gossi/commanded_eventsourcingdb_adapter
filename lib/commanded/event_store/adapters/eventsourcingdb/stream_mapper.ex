defmodule Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper do
  def to_subject(:all, stream_prefix), do: "/#{stream_prefix}"
  def to_subject(uuid, stream_prefix), do: "/#{stream_prefix}#{uuid}"

  def get_stream_id(subject, stream_prefix \\ ""),
    do:
      subject
      |> String.trim_leading("/")
      |> String.trim_leading(stream_prefix)
end
