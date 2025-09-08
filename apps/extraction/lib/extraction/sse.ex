defmodule Extraction.Sse do

  def stream_sse(sse_url, process_func) do
    Req.get!(sse_url,
             into: process_func,
             retry: fn _request, _result -> {:delay, 1} end
            )
  end

  def process_sse({:data, data}, {req, res}) do
    buffer = Req.Request.get_private(req, :sse_buffer, "")
    {events, buffer} = ServerSentEvents.parse(buffer <> data)
    Req.Request.put_private(req, :sse_buffer, buffer)
    if events != [] do
      send(:broadcaster, events)
    end
    {:cont, {req, res}}
  end

end

defmodule Extraction.Sse.GenServer do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(args) do
    {:ok, args, {:continue, :stream}}
  end

  def handle_continue(:stream, _args) do
    Extraction.Sse.stream_sse("https://stream.wikimedia.org/v2/stream/recentchange", &Extraction.Sse.process_sse/2)
  end
end
