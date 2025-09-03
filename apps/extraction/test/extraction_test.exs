defmodule ExtractionTest do
  use ExUnit.Case
  doctest Extraction

  test "S3 config, write, read, delete" do

    test_file_name = ~s/test_file_#{DateTime.utc_now()}.txt/
    test_data = "test data"

    ExAws.S3.put_object("testbucket", test_file_name, test_data)
    |> ExAws.request!

    result =
      ExAws.S3.get_object("testbucket", test_file_name)
      |> ExAws.request!

    assert result.body == test_data

    # clean up
    ExAws.S3.delete_object("testbucket", test_file_name)
    |> ExAws.request!()

  end

  test "genserver flushes to storage after buffer limit is reached" do
    # Arrange
    folder_name = "/tmp/extraction_tests/flush_on_buffer_limit_reached"
    File.rm_rf!(folder_name)
    File.mkdir_p!(folder_name)
    genserver_config = %{
      storage: Extraction.Storage.Local,
      formatter: Extraction.Storage.Formatter.Plain,
      flush_count: 10,
      flush_interval_ms: 100,
      path_prefix: folder_name,
      file_ext: "bin",
      name: :test_extractions_flush_buffer_limit
    }

    {:ok, pid} = Extraction.Storage.GenServer.start_link(genserver_config)
    # Act
    for event <- 1..11 do
      GenServer.cast(pid, {:add_item, event})
    end
    # Assert
    Process.sleep(1100)
    [ result_1_file_name, result_2_file_name ] =
      folder_name
      |> File.ls!
      |> Enum.sort

    {:ok, written_result_1} = File.read("#{folder_name}/#{result_1_file_name}")
    assert written_result_1 == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
    {:ok, written_result_2} = File.read("#{folder_name}/#{result_2_file_name}")
    assert written_result_2 == "11\n"
    # # clean
    Process.exit(pid, :kill)
    # File.rm_rf!(folder_name)
  end

  test "genserver flushes to storage after timeout is reached" do
    # Arrange
    folder_name = "/tmp/extraction_tests/flush_on_timeout"
    File.rm_rf!(folder_name)
    File.mkdir_p!(folder_name)
    genserver_config = %{
      storage: Extraction.Storage.Local,
      formatter: Extraction.Storage.Formatter.Plain,
      flush_count: 1_000_000,
      flush_interval_ms: 100,
      path_prefix: folder_name,
      file_ext: "bin",
      name: :test_extractions_flush_timeout
    }

    {:ok, pid} = Extraction.Storage.GenServer.start_link(genserver_config)
    # Act
    for event <- 1..11 do
      GenServer.cast(pid, {:add_item, event})
    end
    # Assert
    Process.sleep(1100)
    [ result_file_name ] =
      folder_name
      |> File.ls!
    {:ok, written_result} = File.read("#{folder_name}/#{result_file_name}")
    assert written_result == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n"
    # clean
    Process.exit(pid, :kill)
    # File.rm_rf!(folder_name)
  end

end
