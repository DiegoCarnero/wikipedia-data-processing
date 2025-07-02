defmodule Extraction.Sse do

  @destination Extraction.Storage.GenServer

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
      @destination.send(events)
    end
    {:cont, {req, res}}
  end

end
