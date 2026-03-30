defmodule Commanded.EventStore.Adapters.EventSourcingDB.Mapper do
  @moduledoc false

  alias Commanded.EventStore.RecordedEvent
  alias EventSourcingDB.Event

  def to_recorded_event(%Event{} = event, event_number) do
    # metadata =
    #   case metadata do
    #     none when none in [nil, ""] -> %{}
    #     metadata -> serializer.deserialize(metadata, [])
    #   end

    # {causation_id, metadata} = Map.pop(metadata, "$causationId")
    # {correlation_id, metadata} = Map.pop(metadata, "$correlationId")

    %RecordedEvent{
      event_id: event.id,
      event_number: event_number,
      stream_id: event.subject,
      # stream_version: stream_version + 1,
      # causation_id: causation_id,
      # correlation_id: correlation_id,
      event_type: event.type,
      data: event.data,
      # metadata: metadata,
      created_at: to_date_time(event.created_at)
    }
  end

  defp to_date_time(iso8601_datestring) do
    DateTime.from_iso8601(iso8601_datestring)
  end
end
