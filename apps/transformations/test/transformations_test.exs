defmodule TransformationsTest do
  use ExUnit.Case
  doctest Transformations

  test "expected result from flow" do
    # arrange
    window =
      Flow.Window.fixed(5, :minute, fn %{"timestamp" => timestamp} -> timestamp * 1000 end)
      |> Flow.Window.allowed_lateness(1, :minute)

    src_data =
      File.read!("test/test_data/test_src_data.ndjson")
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)
    expected_result =
      File.read!("test/test_data/test_result.ndjson")
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!(&1, keys: :atoms))
      |> Enum.sort_by(&{&1.interval, &1.wiki})
    # act
    actual_result =
      src_data
      |> Flow.from_enumerable
      |> Transformations.Flows.aggr(window)
      |> Enum.to_list
      |> Enum.sort_by(&{&1.interval, &1.wiki})
    # assert
    assert expected_result == actual_result
  end

  test "same result for varying stages" do
    # arrange
    window =
      Flow.Window.fixed(5, :minute, fn %{"timestamp" => timestamp} -> timestamp * 1000 end)
      |> Flow.Window.allowed_lateness(1, :minute)

    src_data =
      File.read!("test/test_data/test_src_data.ndjson")
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)
    # act
    max_stages_result =
      src_data
      |> Flow.from_enumerable
      |> Transformations.Flows.aggr(window)
      |> Enum.to_list
      |> Enum.sort_by(&{&1.interval, &1.wiki})

    one_stage_result =
      src_data
      |> Flow.from_enumerable
      |> Transformations.Flows.aggr(window, 1)
      |> Enum.to_list
      |> Enum.sort_by(&{&1.interval, &1.wiki})
    # assert
    assert max_stages_result == one_stage_result
  end

  test "same result for sorted and unsorted streams" do
    # arrange
    window =
      Flow.Window.fixed(5, :minute, fn %{"timestamp" => timestamp} -> timestamp * 1000 end)
      |> Flow.Window.allowed_lateness(1, :minute)

    unsorted_src_data =
      File.read!("test/test_data/test_src_data.ndjson")
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)

    sorted_src_data =
      unsorted_src_data
      |> Enum.sort_by(&Map.get(&1, "timestamp"))
    # act
    result_of_unsorted =
      unsorted_src_data
      |> Flow.from_enumerable
      |> Transformations.Flows.aggr(window)
      |> Enum.to_list
      |> Enum.sort_by(&{&1.interval, &1.wiki})

    result_of_sorted =
      sorted_src_data
      |> Flow.from_enumerable
      |> Transformations.Flows.aggr(window)
      |> Enum.to_list
      |> Enum.sort_by(&{&1.interval, &1.wiki})
    # assert
    assert result_of_unsorted == result_of_sorted
  end
end
