defmodule Transformations.Flows do

  defp default_stages() do
    System.schedulers_online
  end

  def aggr(flow_with_sources, window, n_partition_stages \\ default_stages()) do
    flow_with_sources
    |> Flow.filter(fn map ->
          Map.has_key?(map, "wiki") and Map.has_key?(map, "timestamp")
      end)
    |> Flow.partition(window: window, stages: n_partition_stages, key: {:key, "wiki"})
    |> Flow.group_by(& &1["wiki"])
    |> Flow.on_trigger(fn acc, _partition_info, {_type, window, trigger} ->
        if trigger == :done do
          results =
            Transformations.aggr_grouped(acc)
            |> Enum.map(&Map.put(&1, :interval, window))
          {results, acc}
        else
          {[], acc}
        end
      end)
  end

end
