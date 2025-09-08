defmodule Pipeline.Stages.Flow do
  use Flow

  def start_link(
      %{
        from_stages: from_stages,
        into_stages: into_stages,
        window: window
      }) do
    Flow.from_stages(from_stages)
    |> Flow.map(fn x -> Jason.decode!(x.data) end)
    |> Transformations.Flows.aggr(window)
    |> Flow.into_stages(into_stages)
  end
end
