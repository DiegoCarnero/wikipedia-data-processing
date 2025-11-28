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
          {results, []}
        else
          {[], acc}
        end
      end)
  end

  def from_local_ndjson(raw_ndjson_filepath) do
    File.stream!(raw_ndjson_filepath)
    |> Flow.from_enumerable()
    |> Flow.map(fn line ->
      case Jason.decode(line) do
          {:ok, map} -> map
          {:error, _reason} -> %{}
        end
    end)
  end

  def from_local_ndjson_data_only(raw_ndjson_filepath) do
    Transformations.Flows.from_local_ndjson(raw_ndjson_filepath)
    |> Flow.filter(fn map -> Map.has_key?(map, "data") end)
    |> Flow.map(fn map ->
      data_str = Access.get(map, "data")
      case Jason.decode(data_str) do
          {:ok, data} -> data
          {:error, _reason} -> %{}
        end
    end)
  end
end
