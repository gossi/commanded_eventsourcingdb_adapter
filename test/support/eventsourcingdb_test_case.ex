defmodule Commanded.EventStore.EventSourcingDBTestCase do
  use ExUnit.CaseTemplate

  alias EventSourcingDB.TestContainer

  setup do
    {:ok, container} = Testcontainers.start_container(TestContainer.new())

    wait_for_esdb_ready(container)

    unique_prefix = Commanded.UUID.uuid4()
    stream_prefix = "test/#{unique_prefix}/"

    config = [
      stream_prefix: stream_prefix,
      source: "https://commanded.app",
      client: [
        base_url: TestContainer.get_base_url(container),
        api_token: TestContainer.get_api_token(container)
      ]
    ]

    {:ok, child_spec, event_store_meta} =
      Commanded.EventStore.Adapters.EventSourcingDB.child_spec(EventSourcingDBApplication, config)

    for child <- child_spec do
      start_supervised!(child)
    end

    client = event_store_meta.client

    on_exit(fn ->
      Testcontainers.stop_container(container.container_id)
    end)

    %{
      event_store_meta: event_store_meta,
      container: container,
      client: client
    }
  end

  # setup context do
  #   IO.inspect(context, label: "setup, context")
  #   :ok = Commanded.EventStore.Adapters.EventSourcingDB.CheckpointStore.init()

  #   # Create a unique stream prefix for this test to isolate events

  #   {:ok, %{event_store_meta: event_store_meta, esdb_meta: event_store_meta}}
  # end

  defp wait_for_esdb_ready(container, retries \\ 90) do
    base_url = TestContainer.get_base_url(container)
    api_token = TestContainer.get_api_token(container)
    client = EventSourcingDB.Client.new(base_url: base_url, api_token: api_token)

    case EventSourcingDB.ping(client) do
      :ok ->
        :ok

      _ when retries > 0 ->
        :timer.sleep(3000)
        wait_for_esdb_ready(container, retries - 1)

      _ ->
        raise "EventSourcingDB container failed to become ready after retries"
    end
  end
end
