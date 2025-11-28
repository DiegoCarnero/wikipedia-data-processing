
defmodule Extraction.Storage.Local do

  def init(opts) do
    File.mkdir_p!(opts.path_prefix)
    :ok
  end

  def write(%{ data: data, path_prefix: path_prefix, file_ext: file_ext}) do
    File.open!(~s"#{path_prefix}/test_file_#{DateTime.utc_now}#{file_ext}", [:write, :binary], fn file -> IO.binwrite(file, data) end)
  end

end

defmodule Extraction.Storage.S3 do

  def init(_opts) do
    :ok
  end

  def write(%{ data: data, path_prefix: path_prefix, file_ext: file_ext}) do
    ExAws.S3.put_object("testbucket", ~s/#{path_prefix}_#{Date.utc_today}\/test_file_#{DateTime.utc_now}#{file_ext}/, data)
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

defmodule Extraction.Storage.Formatter.Plain do

  def encode(data) do
    to_string(data)
  end

  def encode!(data) do
    to_string(data)
  end

  def encode_multiple!(list) do
    [ list, "" ]
    |> List.flatten
    |> Enum.map(&to_string/1)
    |> Enum.join("\n")
  end

end

defmodule Extraction.Storage.GenServer do
  use GenServer

  def start_link(%{ name: name } = opts) do
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def add_item(item) do
    GenServer.cast(__MODULE__, {:add_item, item})
  end

  defp prepend_dot(""), do: ""
  defp prepend_dot(str), do: "." <> str

  def init(opts) do

    defaults = %{buffer: [],
                  formatter: Extraction.Storage.Formatter.Plain,
                  flush_count: 10000,
                  flush_interval_ms: 60_000,
                  storage: Extraction.Storage.Local,
                  path_prefix: "",
                  file_ext: ""
                }

    storage = Map.get(opts, :storage, Extraction.Storage.Local)
    :ok = storage.init(opts)

    path_prefix =
      opts
      |> Map.get(:path_prefix, "")
      |> String.trim_trailing("/")

    file_ext =
      opts
      |> Map.get(:file_ext, "")
      |> String.trim_leading(".")
      |> prepend_dot


    state = Map.merge(defaults, opts, fn _k, _v1, v2 -> v2 end)
    state = %{ state | file_ext: file_ext, path_prefix: path_prefix }
    flush_interval_ms = state.flush_interval_ms
    state = Map.put_new(state, :timer, schedule_flush(flush_interval_ms))
    {:ok, state}
  end

  def handle_cast({:add_item, item}, %{buffer: buffer, flush_count: flush_count, flush_interval_ms: flush_interval_ms, formatter: formatter, storage: storage, path_prefix: path_prefix, file_ext: file_ext} = state) do
    new_buffer = [item | buffer]
    if length(new_buffer) >= flush_count do
      flush_to_storage(new_buffer, formatter, storage, path_prefix, file_ext)
      timer = state.timer
      {:noreply, %{state | buffer: [], timer: reset_timer(timer, flush_interval_ms)}}
    else
      {:noreply, %{state | buffer: new_buffer}}
    end
  end

  def handle_info(:flush, %{buffer: buffer, flush_interval_ms: flush_interval_ms, storage: storage, formatter: formatter, path_prefix: path_prefix, file_ext: file_ext} = state) do
    unless buffer == [] do
      flush_to_storage(buffer, formatter, storage, path_prefix, file_ext)
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

  defp flush_to_storage(items, formatter, storage, path_prefix, file_ext) do
    data =
      items
       |> Enum.reverse
       |> formatter.encode_multiple!
    storage.write(%{ data: data, path_prefix: path_prefix, file_ext: file_ext})
    :ok
  end

end

defmodule Extraction.Storage.GenStage do
  use GenStage

  def start_link(%{ name: name } = opts) do
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  defp prepend_dot(""), do: ""
  defp prepend_dot(str), do: "." <> str

  def init(opts) do

    defaults = %{buffer: [],
                  formatter: Extraction.Storage.Formatter.Plain,
                  flush_count: 10_000,
                  flush_interval_ms: 60_000,
                  storage: Extraction.Storage.Local,
                  path_prefix: "",
                  file_ext: ""
                }

    storage = Map.get(opts, :storage, Extraction.Storage.Local)
    :ok = storage.init(opts)

    path_prefix =
      opts
      |> Map.get(:path_prefix, "")
      |> String.trim_trailing("/")

    file_ext =
      opts
      |> Map.get(:file_ext, "")
      |> String.trim_leading(".")
      |> prepend_dot

    state = Map.merge(defaults, opts, fn _k, _v1, v2 -> v2 end)
    state = %{ state | file_ext: file_ext, path_prefix: path_prefix }
    flush_interval_ms = state.flush_interval_ms
    state = Map.put_new(state, :timer, schedule_flush(flush_interval_ms))

    subscribe_opts = Map.get(state, :subscribe_opts, nil)
    if subscribe_opts do
      {:consumer, state, subscribe_to: subscribe_opts}
    else
      {:consumer, state}
    end
  end

  def handle_events(events, _from, state) do
    for event <- events, do: send(state.name, {:add_item, event})
    {:noreply, [], state}
  end

  def handle_info({:add_item, item}, state) do
    GenStage.cast(state.name, {:add_item, item})
    {:noreply, [], state}
  end

  def handle_info(:flush, %{buffer: buffer, flush_interval_ms: flush_interval_ms, storage: storage, formatter: formatter, path_prefix: path_prefix, file_ext: file_ext} = state) do
    unless buffer == [] do
      flush_to_storage(buffer, formatter, storage, path_prefix, file_ext)
    end
    {:noreply, [],  %{state | buffer: [], timer: schedule_flush(flush_interval_ms)}}
  end

  def handle_cast({:add_item, item}, %{buffer: buffer, flush_count: flush_count, flush_interval_ms: flush_interval_ms, formatter: formatter, storage: storage, path_prefix: path_prefix, file_ext: file_ext} = state) do
    new_buffer = [item | buffer]
    if length(new_buffer) >= flush_count do
      flush_to_storage(new_buffer, formatter, storage, path_prefix, file_ext)
      timer = state.timer
      {:noreply,  [], %{state | buffer: [], timer: reset_timer(timer, flush_interval_ms)}}
    else
      {:noreply, [], %{state | buffer: new_buffer}}
    end
  end

  defp schedule_flush(flush_interval_ms) do
    Process.send_after(self(), :flush, flush_interval_ms)
  end

  defp reset_timer(timer_ref, flush_interval_ms) do
    Process.cancel_timer(timer_ref)
    schedule_flush(flush_interval_ms)
  end

  defp flush_to_storage(items, formatter, storage, path_prefix, file_ext) do
    data =
      items
       |> Enum.reverse
       |> formatter.encode_multiple!
    storage.write(%{ data: data, path_prefix: path_prefix, file_ext: file_ext})
    :ok
  end

end
