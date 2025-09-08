defmodule Extraction.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Extraction.Storage.GenServer, %{storage: Extraction.Storage.Local, formatter: Extraction.Storage.Formatter.Json, path_prefix: "/tmp/extraction_test", flush_interval_ms: 1000, name: :test_extractions}},
      {Extraction.Sse.GenServer, []},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Wiki.Extractions.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
