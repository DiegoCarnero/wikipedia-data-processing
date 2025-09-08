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

    base_storage_config = %{storage: Extraction.Storage.S3, formatter: Extraction.Storage.Formatter.Json}
    base_transf_config = %{from_stages: [:broadcaster], into_stages: [:aggr_storage], window: window}

    storage_config_raw =
      base_storage_config
      |> Map.put(:name, :raw_storage)
      |> Map.put(:path_prefix, "/tmp/test_folder_raw")
      |> Map.put(:subscribe_opts, [:broadcaster])

    storage_config_agrr =
      base_storage_config
      |> Map.put(:name, :aggr_storage)
      |> Map.put(:path_prefix, "/tmp/test_folder_aggr")

    children = [
      {Pipeline.Stages.Broadcaster, %{name: :broadcaster}},
      {Extraction.Sse.GenServer, []},
      Supervisor.child_spec({Extraction.Storage.GenStage, storage_config_raw}, id: :raw_storage),
      Supervisor.child_spec({Extraction.Storage.GenStage, storage_config_agrr}, id: :aggr_storage),
      Supervisor.child_spec({Pipeline.Stages.Flow, base_transf_config}, id: :flow_stream),
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Pipeline.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
