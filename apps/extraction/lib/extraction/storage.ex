
defmodule Extraction.Storage.Local do

  def send(data) do
    json = Jason.encode!(data)
    {:ok, file} = File.open("/data/output_test.bin", [:append, :binary])
    IO.binwrite(file, json)
    IO.binwrite(file, "\n")
    File.close(file)
  end

end
