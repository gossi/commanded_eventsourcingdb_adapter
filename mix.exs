defmodule Commanded.EventStore.Adapters.EventSourcingDB.MixProject do
  use Mix.Project

  @version "0.0.1"
  @source_url "https://github.com/gossi/commanded_eventsourcingdb_adapter"

  def project do
    [
      app: :commanded_eventsourcingdb_adapter,
      version: @version,
      elixir: "~> 1.19",
      package: package(),
      docs: docs(),
      aliases: aliases(),
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "#{@version}",
      extras: ["README.md"]
    ]
  end

  defp package do
    [
      name: "commanded_eventsourcingdb_adapter",
      description: "Commanded adapter for EventSourcingDB",
      links: %{
        "GitHub" => @source_url,
        "commanded" => "https://eventsourcingdb.io",
        "EventSourcingDB" => "https://eventsourcingdb.io",
        "EventSourcingDB Docs" => "https://docs.eventsourcingdb.io",
        "EventSourcingDB SDK" => "https://hexdocs.pm/eventsourcingdb"
      },
      licenses: ["MIT"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test),
    do: [
      "deps/commanded/test/event_store",
      "deps/commanded/test/support",
      "lib",
      "test/support"
    ]

  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:commanded, "~> 1.2"},
      {:eventsourcingdb, "~> 1.0"},
      {:testcontainers, "~> 2.3.0"},
      {:typedstruct, "~> 0.5"},

      # Test & build tooling
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false, warn_if_outdated: true},
      {:mox, "~> 1.0", only: :test},

      # Dev tooling
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      i: ["deps.get"],
      ic: ["deps.get", "deps.compile"]
    ]
  end
end
