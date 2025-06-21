defmodule Extraction.Sse do

  @destination Extraction.Storage.Local

  def stream_sse(sse_url, process_func) do
    Req.get!(sse_url, into: process_func)
  end

  def process_sse({:data, data}, {req, res}) do
    buffer = Req.Request.get_private(req, :sse_buffer, "")
    {events, buffer} = ServerSentEvents.parse(buffer <> data)
    Req.Request.put_private(req, :sse_buffer, buffer)
    if events != [] do
      # Process the events (e.g., send to a GenServer)
      Enum.map(events, fn event ->
        @destination.send(event)
      end)
    end
    {:cont, {req, res}}
  end

end
