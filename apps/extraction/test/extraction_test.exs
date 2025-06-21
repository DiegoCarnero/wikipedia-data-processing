defmodule ExtractionTest do
  use ExUnit.Case
  doctest Extraction

  test "greets the world" do
    assert Extraction.hello() == :world
  end
end
