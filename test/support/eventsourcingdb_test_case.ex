defmodule Commanded.EventSourcingDBTestCase do
  use ExUnit.CaseTemplate

  alias EventSourcingDB.TestContainer
  alias Commanded.EventStore.Adapters.EventSourcingDB

  setup do
    {:ok, container} = Testcontainers.start_container(TestContainer.new())
    on_exit(fn -> Testcontainers.stop_container(container.container_id) end)

    {:ok, esdb_meta} = start_eventsourcingdb(container)

    %{esdb_meta: esdb_meta, container: container}
  end

  def start_eventsourcingdb(container) do
    config = [
      # serializer: Commanded.Serialization.JsonSerializer,
      client: [
        base_url: TestContainer.get_base_url(container),
        api_token: TestContainer.get_api_token(container)
      ]
    ]

    {:ok, child_spec, esdb_meta} =
      EventSourcingDB.child_spec(EventSourcingDBApplication, config)

    # for child <- child_spec do
    #   start_supervised!(child)
    # end

    {:ok, esdb_meta}
  end
end
