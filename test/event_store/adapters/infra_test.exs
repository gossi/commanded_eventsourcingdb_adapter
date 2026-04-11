defmodule Commanded.EventStore.Adapters.EventSourcingDB.InfraTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB
  alias Commanded.EventStore.EventData

  use Commanded.EventStore.EventSourcingDBTestCase

  defmodule BankAccountOpened do
    @derive Jason.Encoder
    defstruct [:account_number, :initial_balance]
  end

  test "hello", %{esdb_meta: esdb_meta} do
    EventSourcingDB.subscribe(esdb_meta, "/")

    EventSourcingDB.append_to_stream(esdb_meta, "bank-account", 0, [build_event(1)])

    assert_receive {:events, _received_events}
  end

  defp build_event(account_number) do
    %EventData{
      causation_id: Commanded.UUID.uuid4(),
      correlation_id: Commanded.UUID.uuid4(),
      event_type: "#{__MODULE__}.BankAccountOpened",
      data: %BankAccountOpened{account_number: account_number, initial_balance: 1_000},
      metadata: %{"user_id" => "test"}
    }
  end
end
