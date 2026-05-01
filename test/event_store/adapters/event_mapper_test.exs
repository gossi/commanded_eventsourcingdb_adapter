defmodule Commanded.EventStore.Adapters.EventSourcingDB.EventMapperTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB.EventMapper
  alias Commanded.EventStore.RecordedEvent
  alias EventSourcingDB.Event

  use ExUnit.Case, async: true

  defp esdb_event(overrides \\ %{}) do
    base = %{
      "id" => "1",
      "source" => "/myapp/bank-account/",
      "subject" => "/myapp/bank-account/ACC123",
      "type" => "AccountOpened",
      "data" => %{
        "account_number" => "ACC123",
        "initial_balance" => 1000
      },
      "time" => "2025-04-15T10:00:00Z",
      "specversion" => "1.0",
      "datacontenttype" => "application/json",
      "predecessorhash" => "0000000000000000000000000000000000000000000000000000000000000000000",
      "hash" => "aaaaaaaa00000000000000000000000000000000000000000000000000000000000"
    }

    merged = Map.merge(base, overrides)
    Event.new(merged)
  end

  describe "to_recorded_event/4" do
    test "maps all fields correctly" do
      event = esdb_event()

      result = EventMapper.to_recorded_event(event, 1, "/myapp/bank-account/", nil)

      assert %RecordedEvent{} = result
      assert result.stream_id == "ACC123"
      assert result.stream_version == 1
      assert result.event_type == "AccountOpened"
      assert result.data == %{"account_number" => "ACC123", "initial_balance" => 1000}
    end

    test "extracts correlation_id from metadata" do
      event = esdb_event(%{
        "data" => %{
          "__commanded_metadata__" => %{
            "correlation_id" => "corr-uuid-123",
            "causation_id" => "caus-uuid-456",
            "metadata" => %{"key" => "value"}
          },
          "account_number" => "ACC123"
        }
      })

      result = EventMapper.to_recorded_event(event, 1, "", nil)

      assert result.correlation_id == "corr-uuid-123"
      assert result.causation_id == "caus-uuid-456"
      assert result.metadata == %{"key" => "value"}
    end

    test "extracts causation_id from metadata" do
      event = esdb_event(%{
        "data" => %{
          "__commanded_metadata__" => %{
            "correlation_id" => nil,
            "causation_id" => "caus-uuid-456",
            "metadata" => %{}
          },
          "account_number" => "ACC123"
        }
      })

      result = EventMapper.to_recorded_event(event, 1, "", nil)

      assert result.causation_id == "caus-uuid-456"
    end

    test "handles missing metadata with nil values" do
      event = esdb_event()

      result = EventMapper.to_recorded_event(event, 1, "", nil)

      assert result.correlation_id == nil
      assert result.causation_id == nil
      assert result.metadata == %{}
    end

    test "uses provided stream_version" do
      event = esdb_event()

      result = EventMapper.to_recorded_event(event, 5, "", nil)

      assert result.stream_version == 5
    end

    test "defaults stream_version to 0 when nil" do
      event = esdb_event()

      result = EventMapper.to_recorded_event(event, 0, "", nil)

      assert result.stream_version == 0
    end

    test "extracts stream_id from subject" do
      event = esdb_event(%{"subject" => "/myapp/bank-account/ACC123"})

      result = EventMapper.to_recorded_event(event, 1, "/myapp/bank-account/", nil)

      assert result.stream_id == "ACC123"
    end

    test "converts time to DateTime" do
      event = esdb_event()

      result = EventMapper.to_recorded_event(event, 1, "", nil)

      assert result.created_at == ~U[2025-04-15T10:00:00Z]
    end
  end

  describe "to_global_event_number/1" do
    test "converts id 5 to 6" do
      event = esdb_event(%{"id" => "5"})

      assert EventMapper.to_global_event_number(event) == 6
    end

    test "edge case id 0 returns 1" do
      event = esdb_event(%{"id" => "0"})

      assert EventMapper.to_global_event_number(event) == 1
    end
  end

  describe "serialize_event_data/4" do
    test "wraps data with metadata key" do
      data = %{"account_number" => "ACC123"}
      correlation_id = "corr-uuid"
      causation_id = "caus-uuid"
      metadata = %{"foo" => "bar"}

      result = EventMapper.serialize_event_data(data, correlation_id, causation_id, metadata)

      assert result["__commanded_metadata__"]["correlation_id"] == "corr-uuid"
      assert result["__commanded_metadata__"]["causation_id"] == "caus-uuid"
      assert result["__commanded_metadata__"]["metadata"] == %{"foo" => "bar"}
      assert result["account_number"] == "ACC123"
    end

    test "handles nil metadata defaults to empty map" do
      data = %{"amount" => 100}
      correlation_id = "corr"
      causation_id = nil
      metadata = nil

      result = EventMapper.serialize_event_data(data, correlation_id, causation_id, metadata)

      assert result["__commanded_metadata__"]["metadata"] == %{}
    end

    test "serializes map data" do
      data = %{"account_number" => "ACC123", "initial_balance" => 1000}

      result = EventMapper.serialize_event_data(data, nil, nil, %{})

      assert result["__commanded_metadata__"] != nil
      assert result["account_number"] == "ACC123"
      assert result["initial_balance"] == 1000
    end
  end

  describe "edge cases" do
    test "deserialize unknown type returns original data" do
      event = esdb_event(%{
        "type" => "UnknownEventType",
        "data" => %{"field" => "value"}
      })

      result = EventMapper.to_recorded_event(event, 1, "", nil)

      assert result.data == %{"field" => "value"}
    end
  end
end