defmodule Pipeline.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    window =
      Flow.Window.fixed(5, :minute, fn %{"timestamp" => timestamp} -> timestamp * 1000 end)
      |> Flow.Window.allowed_lateness(1, :minute)

    children = [
      {Pipeline.Stages.Broadcaster, %{name: :broadcaster}},
      {Extraction.Storage.GenServer, %{name: :storage, storage: Extraction.Storage.S3, formatter: Extraction.Storage.Formatter.Json}},
      Supervisor.child_spec({Pipeline.Stages.Flow, %{from_stages: [:broadcaster], into_stages: [:storage], window: window}}, id: :flow_stream),
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Pipeline.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
