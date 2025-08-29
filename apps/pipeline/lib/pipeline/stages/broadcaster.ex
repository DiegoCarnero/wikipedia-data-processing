defmodule Pipeline.Stages.Broadcaster do
  use GenStage

  def start_link(%{name: name} = initial) do
    GenStage.start_link(__MODULE__, initial, name: name)
  end

  def init(initial) do
    state = %{pending_demand: 0, buffer: []}
    subscribe_options = Map.get(initial, :subscribe_options, nil)
    if subscribe_options do
      {:producer_consumer, state, subscribe_to: subscribe_options, dispatcher: GenStage.BroadcastDispatcher}
    else
      {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
    end
  end

  def handle_demand(new_demand, state) do
    pending_demand = state.pending_demand
    to_take = pending_demand + new_demand
    {to_emit, remaining} = Enum.split(state.buffer, to_take)

    next_state =
      state
      |> Map.put(:pending_demand, to_take - length(to_emit))
      |> Map.put(:buffer, remaining)

    {:noreply, to_emit, next_state}
  end

  def handle_info(new_events, old_state) do
    buffer = old_state.buffer
    new_state = %{ old_state | buffer: buffer ++ new_events}
    {:noreply, [], new_state}
  end
end
