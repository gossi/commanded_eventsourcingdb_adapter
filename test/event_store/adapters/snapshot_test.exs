defmodule Commanded.EventStore.Adapters.EventSourcingDB.SnapshotTest do
  use Commanded.EventStore.EventSourcingDBTestCase

  alias Commanded.EventStore.Adapters.EventSourcingDB

  test "read snapshot returns not supported", %{esdb_meta: esdb_meta} do
    source_uuid = "test-source"

    result = EventSourcingDB.read_snapshot(esdb_meta, source_uuid)
    assert result == {:error, :snapshots_not_supported}
  end

  test "record snapshot returns not supported", %{esdb_meta: esdb_meta} do
    snapshot_data = %Commanded.EventStore.SnapshotData{
      source_type: "TestSource",
      source_uuid: "test-uuid",
      source_version: 1,
      data: %{"test" => "data"}
    }

    result = EventSourcingDB.record_snapshot(esdb_meta, snapshot_data)
    assert result == {:error, :snapshots_not_supported}
  end

  test "delete snapshot returns not supported", %{esdb_meta: esdb_meta} do
    source_uuid = "test-source"

    result = EventSourcingDB.delete_snapshot(esdb_meta, source_uuid)
    assert result == {:error, :snapshots_not_supported}
  end
end
