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
end
