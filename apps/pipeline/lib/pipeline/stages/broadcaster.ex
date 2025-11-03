defmodule Pipeline.Stages.Broadcaster do
  use GenStage

  def start_link(%{name: name} = initial) do
    GenStage.start_link(__MODULE__, initial, name: name)
  end

  def init(initial) do
    state = %{pending_demand: 0, buffer: []}
    subscribe_options = Map.get(initial, :subscribe_options, nil)

    if subscribe_options do
      {:producer_consumer, state,
       subscribe_to: subscribe_options, dispatcher: GenStage.BroadcastDispatcher}
    else
      {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
    end
  end

  defp get_events_to_emit_and_next_state(state) do
    pending_demand = state.pending_demand
    {to_emit, remaining} = Enum.split(state.buffer, pending_demand)

    next_state =
      state
      |> Map.put(:buffer, remaining)
      |> Map.put(:pending_demand, pending_demand - length(to_emit))

    {to_emit, next_state}
  end

  def handle_demand(new_demand, state) do
    pending_demand = state.pending_demand
    demand = pending_demand + new_demand
    state = %{state | pending_demand: demand}
    {to_emit, next_state} = get_events_to_emit_and_next_state(state)

    {:noreply, to_emit, next_state}
  end

  def handle_info(events, state) when is_list(events), do: handle_events(events, self(), state)

  def handle_info(event, state), do: handle_events([event], self(), state)

  def handle_events(new_events, _from, state) do
    buffer = state.buffer
    new_state = %{state | buffer: buffer ++ new_events}
    {to_emit, next_state} = get_events_to_emit_and_next_state(new_state)

    {:noreply, to_emit, next_state}
  end

end
