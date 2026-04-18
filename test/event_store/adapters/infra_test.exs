defmodule Commanded.EventStore.Adapters.EventSourcingDB.InfraTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB
  alias Commanded.EventStore.EventData

  use Commanded.EventStore.EventSourcingDBTestCase

  defmodule BankAccountOpened do
    @derive Jason.Encoder
    defstruct [:account_number, :initial_balance]
  end

  describe "transient subscription" do
    test "subscribe to stream", %{esdb_meta: esdb_meta} do
      EventSourcingDB.subscribe(esdb_meta, "bank-account")

      EventSourcingDB.append_to_stream(esdb_meta, "bank-account", 0, [build_event(1)])

      assert_receive {:events, _received_events}

      # IO.inspect(received_events, label: "received events")
    end

    test "subscribe to all", %{esdb_meta: esdb_meta} do
      EventSourcingDB.subscribe(esdb_meta, :all)

      EventSourcingDB.append_to_stream(esdb_meta, "bank-account", 0, [build_event(1)])

      assert_receive {:events, _received_events}

      # IO.inspect(received_events, label: "received events")
    end
  end

  describe "persistent subscription to a single stream" do
    test "should receive `:subscribed` message once subscribed", %{esdb_meta: esdb_meta} do
      {:ok, subscription} =
        EventSourcingDB.subscribe_to(esdb_meta, "stream1", "subscriber", self(), :origin, [])

      IO.inspect(subscription, label: "subscription")

      assert_receive {:subscribed, ^subscription}
    end

    test "should receive events appended to stream", %{esdb_meta: esdb_meta} do
      {:ok, subscription} =
        EventSourcingDB.subscribe_to(esdb_meta, "stream1", "subscriber", self(), :origin, [])

      assert_receive {:subscribed, ^subscription}

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 0, build_events(1))
      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 1, build_events(2))
      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 3, build_events(3))

      assert_receive {:events, _}
      assert_receive {:events, _}
      assert_receive {:events, _}
      assert_receive {:events, _}
      assert_receive {:events, _}
      assert_receive {:events, _}

      # assert_receive_events(EventSourcingDB, esdb_meta, subscription, count: 1, from: 1)
      # assert_receive_events(EventSourcingDB, esdb_meta, subscription, count: 2, from: 2)
      # assert_receive_events(EventSourcingDB, esdb_meta, subscription, count: 3, from: 4)

      refute_receive {:events, _received_events}
    end

    test "should not receive events appended to another stream", %{esdb_meta: esdb_meta} do
      {:ok, _subscription} =
        EventSourcingDB.subscribe_to(esdb_meta, "stream1", "subscriber", self(), :origin, [])

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 0, build_events(1))
      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream2", 0, build_events(2))
      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream3", 0, build_events(3))

      assert_receive {:events, _received_events}

      # assert_receive_events(event_store, event_store_meta, subscription, count: 1, from: 1)
      refute_receive {:events, _received_events}
    end
  end

  describe "unsubscribe from all streams" do
    test "should not receive further events appended to any stream", %{esdb_meta: esdb_meta} do
      {:ok, subscription} =
        EventSourcingDB.subscribe_to(esdb_meta, :all, "subscriber", self(), :origin, [])

      assert_receive {:subscribed, ^subscription}

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 0, build_events(1))

      assert_receive {:events, _received_events}

      :ok = EventSourcingDB.unsubscribe(esdb_meta, subscription)

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream2", 0, build_events(2))
      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream3", 0, build_events(3))

      refute_receive {:events, _received_events}
    end

    test "should resume subscription when subscribing again", %{esdb_meta: esdb_meta} do
      {:ok, subscription1} =
        EventSourcingDB.subscribe_to(esdb_meta, :all, "subscriber", self(), :origin, [])

      assert_receive {:subscribed, ^subscription1}

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 0, build_events(1))

      # assert_receive_events(event_store, esdb_meta, subscription1, count: 1, from: 1)
      assert_receive {:events, _received_events}

      :ok = EventSourcingDB.unsubscribe(esdb_meta, subscription1)

      {:ok, subscription2} =
        EventSourcingDB.subscribe_to(esdb_meta, :all, "subscriber", self(), :origin, [])

      :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream2", 0, build_events(2))

      assert_receive {:subscribed, ^subscription2}
      # assert_receive {:events, received_events}
      # assert_receive_events(event_store, esdb_meta, subscription2, count: 2, from: 2)
    end
  end

  describe "delete subscription" do
    # @tag :focus
    # test "should be deleted", %{esdb_meta: esdb_meta} do
    #   {:ok, subscription1} =
    #     EventSourcingDB.subscribe_to(esdb_meta, :all, "subscriber", self(), :origin, [])

    #   assert_receive {:subscribed, ^subscription1}

    #   :ok = EventSourcingDB.append_to_stream(esdb_meta, "stream1", 0, build_events(1))

    #   # assert_receive_events(event_store, esdb_meta, subscription1, count: 1, from: 1)
    #   assert_receive {:events, _received_events}

    #   :ok = EventSourcingDB.unsubscribe(esdb_meta, subscription1)

    #   assert :ok = EventSourcingDB.delete_subscription(esdb_meta, :all, "subscriber")
    # end

    # test "should create new subscription after deletion", %{
    #   event_store: event_store,
    #   event_store_meta: event_store_meta
    # } do
    #   {:ok, subscription1} =
    #     event_store.subscribe_to(event_store_meta, :all, "subscriber", self(), :origin, [])

    #   assert_receive {:subscribed, ^subscription1}

    #   :ok = event_store.append_to_stream(event_store_meta, "stream1", 0, build_events(1))

    #   # assert_receive_events(event_store, event_store_meta, subscription1, count: 1, from: 1)
    #   assert_receive {:events, received_events}

    #   :ok = unsubscribe(event_store, event_store_meta, subscription1)

    #   :ok = event_store.delete_subscription(event_store_meta, :all, "subscriber")

    #   :ok = event_store.append_to_stream(event_store_meta, "stream2", 0, build_events(2))

    #   refute_receive {:events, _received_events}

    #   {:ok, subscription2} =
    #     event_store.subscribe_to(event_store_meta, :all, "subscriber", self(), :origin, [])

    #   # Should receive all events as subscription has been recreated from `:origin`
    #   assert_receive {:subscribed, ^subscription2}
    #   # assert_receive {:events, received_events}
    #   # assert_receive_events(event_store, event_store_meta, subscription2, count: 1, from: 1)
    #   # assert_receive_events(event_store, event_store_meta, subscription2, count: 2, from: 2)
    # end
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

  defp build_events(count) do
    for account_number <- 1..count, do: build_event(account_number)
  end
end
