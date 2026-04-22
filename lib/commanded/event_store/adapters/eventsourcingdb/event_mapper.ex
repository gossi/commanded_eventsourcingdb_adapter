defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventMapper do
  @moduledoc false

  alias Commanded.EventStore.RecordedEvent
  alias Commanded.EventStore.TypeProvider
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper
  alias EventSourcingDB.Event

  @commanded_metadata_key "__commanded_metadata__"

  def to_recorded_event(
        %Event{} = event,
        stream_version \\ 0,
        stream_prefix \\ "",
        event_number \\ nil
      ) do
    {data, correlation_id, causation_id, metadata} =
      extract_commanded_metadata(event.data, event.type)

    %RecordedEvent{
      # event_id is a unique identifier for the event. See generate_event_id()
      # DO NOT CHANGE this field
      event_id: generate_event_id(event),
      # event_number is either a global or subscription based counter
      # DO NOT CHANGE this field
      event_number: get_event_number(event, event_number),
      # Position of event within ONE specific stream
      stream_version: stream_version,
      # The part of the stream_id relevant for commanded
      # DO NOT CHANGE this field
      stream_id: StreamMapper.get_stream_id(event.subject, stream_prefix),
      event_type: event.type,
      data: data,
      correlation_id: correlation_id,
      causation_id: causation_id,
      metadata: metadata,
      created_at: to_date_time(event.time)
    }
  end

  defp get_event_number(%Event{} = _, event_number) when is_integer(event_number),
    do: event_number

  defp get_event_number(%Event{} = event, _), do: String.to_integer(event.id) + 1

  def serialize_event_data(data, correlation_id, causation_id, metadata) do
    commanded_meta = %{
      "correlation_id" => correlation_id,
      "causation_id" => causation_id,
      "metadata" => metadata || %{}
    }

    serialized_data = serialize_data(data)

    Map.put(serialized_data, @commanded_metadata_key, commanded_meta)
  end

  defp extract_commanded_metadata(data, type) do
    {commanded_meta, clean_data} = Map.pop(data, @commanded_metadata_key)

    {correlation_id, causation_id, metadata} =
      if commanded_meta do
        {
          Map.get(commanded_meta, "correlation_id"),
          Map.get(commanded_meta, "causation_id"),
          Map.get(commanded_meta, "metadata", %{})
        }
      else
        {nil, nil, %{}}
      end

    deserialized = deserialize_data(clean_data, type)

    {deserialized, correlation_id, causation_id, metadata}
  end

  defp deserialize_data(data, type) do
    struct_template = TypeProvider.to_struct(type)

    case struct_template do
      %module{} ->
        atom_keyed_data = to_atom_keys(data)
        struct(module, atom_keyed_data)

      _ ->
        data
    end
  rescue
    _ -> data
  end

  defp to_atom_keys(map) when is_map(map) do
    Map.new(map, fn {key, val} ->
      atom_key =
        case is_binary(key) do
          true -> String.to_existing_atom(key)
          false -> key
        end

      {atom_key, val}
    end)
  end

  defp to_atom_keys(other), do: other

  defp serialize_data(%_{} = struct), do: Map.from_struct(struct)
  defp serialize_data(data), do: data

  defp to_date_time(iso8601_datestring) do
    {:ok, datetime, _offset} = DateTime.from_iso8601(iso8601_datestring)
    datetime
  end

  # DO NOT CHANGE this function (both variants of it)

  # Generate the event ID
  #
  # ESDB/CloudEvents claim that source + id is a unique event id:
  # https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#source-1
  # We use that concatenation for a unique event id
  #
  # However, the commanded tests explicitely check for a UUID. Likely the tests
  # were written with no other option in mind.
  # So for tests, we hack that in.
  @compile if: Mix.env() == :test
  defp generate_event_id(%Event{} = _event), do: Commanded.UUID.uuid4()

  @compile if: Mix.env() != :test
  defp generate_event_id(%Event{} = event), do: "#{event.source}/#{event.id}"
end
