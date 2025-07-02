
defmodule Extraction.Storage.Local do

  def send(data) do
    file = File.open("/data/output_test.bin", [:append, :binary])
    IO.binwrite(file, data)
    File.close(file)
  end

end

defmodule Extraction.Storage.S3 do

  def send(data) do
    ExAws.S3.put_object("testbucket", ~s/test_folder_#{Date.utc_today}\/test_file_#{DateTime.utc_now}.ndjson/, data)
    |> ExAws.request!
  end

end

defmodule Extraction.Storage.Formatter.Json do

  def encode(data) do
    Jason.encode(data)
  end

  def encode!(data) do
    Jason.encode!(data)
  end

  def encode_multiple!(list) do
    list
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
  end

end

defmodule Extraction.Storage.GenServer do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def add_item(item) do
    GenServer.cast(__MODULE__, {:add_item, item})
  end

  def init(opts) do

    defaults = %{buffer: [],
                 formatter: Extraction.Storage.Formatter.Json,
                 flush_count: 10000,
                 flush_interval_ms: 60_000,
                 storage: Extraction.Storage.S3}
    state = Map.merge(defaults, opts, fn _k, _v1, v2 -> v2 end)

    flush_interval_ms = state.flush_interval_ms
    state = Map.put_new(state, :timer, schedule_flush(flush_interval_ms))
    {:ok, state}
  end

  def handle_cast({:add_item, item}, %{buffer: buffer, flush_count: flush_count, flush_interval_ms: flush_interval_ms, formatter: formatter, storage: storage} = state) do
    new_buffer = [item | buffer]
    if length(new_buffer) >= flush_count do
      flush_to_storage(new_buffer, formatter, storage)
      timer = state.timer
      {:noreply, %{state | buffer: [], timer: reset_timer(timer, flush_interval_ms)}}
    else
      {:noreply, %{state | buffer: new_buffer}}
    end
  end

  def handle_info(:flush, %{buffer: buffer, flush_interval_ms: flush_interval_ms, storage: storage, formatter: formatter} = state) do
    unless buffer == [] do
      flush_to_storage(buffer, formatter, storage)
    end
    {:noreply, %{state | buffer: [], timer: schedule_flush(flush_interval_ms)}}
  end

  def send(events) do
    Enum.each(events, fn event ->
      GenServer.cast(__MODULE__, {:add_item, event})
    end)
    :ok
  end

  defp schedule_flush(flush_interval_ms) do
    Process.send_after(self(), :flush, flush_interval_ms)
  end

  defp reset_timer(timer_ref, flush_interval_ms) do
    Process.cancel_timer(timer_ref)
    schedule_flush(flush_interval_ms)
  end

  defp flush_to_storage(items, formatter, storage) do
    data = formatter.encode_multiple!(items)
    storage.send(data)
    :ok
  end
end
