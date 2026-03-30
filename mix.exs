defmodule Commanded.EventStore.Adapters.EventSourcingDB.MixProject do
  use Mix.Project

  def project do
    [
      app: :commanded_eventsourcingdb_adapter,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
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
      {:eventsourcingdb, "~> 0.7.1"},
      {:testcontainers, "~> 2.0.0"},
      {:typedstruct, "~> 0.5"},

      # Test & build tooling
      {:ex_doc, "~> 0.21", only: :dev},
      {:mox, "~> 1.0", only: :test}
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
