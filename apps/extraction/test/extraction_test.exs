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
    defmodule MockStorage do
      require Logger
      @destination self()
      def write(data) do
        send(@destination, data)
        :ok
      end
    end
    genserver_config = %{
      storage: MockStorage,
      formatter: Extraction.Storage.Formatter.Plain,
      flush_count: 10,
      flush_interval_ms: 100,
    }

    {:ok, pid} = Extraction.Storage.GenServer.start_link(genserver_config)
    # Act
    for event <- 1..11 do
      Extraction.Storage.GenServer.add_item(event)
    end
    # Assert
    assert_receive "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
    Process.sleep(1)
    assert_receive "11\n"
    # clean
    Process.exit(pid, :kill)
  end

  test "genserver flushes to storage after timeout is reached" do
    # Arrange
    defmodule MockStorage do
      require Logger
      @destination self()
      def write(data) do
        send(@destination, data)
        :ok
      end
    end
    genserver_config = %{
      storage: MockStorage,
      formatter: Extraction.Storage.Formatter.Plain,
      flush_count: 1_000_000,
      flush_interval_ms: 100,
    }

    {:ok, pid} = Extraction.Storage.GenServer.start_link(genserver_config)
    # Act
    for event <- 1..11 do
      Extraction.Storage.GenServer.add_item(event)
    end
    # Assert
    Process.sleep(110)
    assert_received "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n"
    # clean
    Process.exit(pid, :kill)
  end

end
