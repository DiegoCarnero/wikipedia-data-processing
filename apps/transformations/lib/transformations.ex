defmodule Transformations do

  def aggr_grouped(groups) do
    Enum.map(groups, fn {key, events} ->
        Transformations.aggr(events)
        |> Map.put(:wiki, key)
    end)
  end

  def aggr(events) do
    ids = get_in(events, [Access.all(), "id"])
    %{
      n_elems: Enum.count(events),
      ids: Enum.sort(ids)
    }
  end

end
