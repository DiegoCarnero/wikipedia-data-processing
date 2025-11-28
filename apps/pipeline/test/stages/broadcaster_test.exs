defmodule BroadcasterTest do
  use ExUnit.Case
  doctest Pipeline

  defmodule Forwarder do
    use GenStage

    # Starts the consumer process
    def start_link(%{name: name, parent: parent, subscribe_to: subscribe_to}) do
      GenStage.start_link(__MODULE__, %{parent: parent, subscribe_to: subscribe_to}, name: name)
    end

    def init(%{parent: parent, subscribe_to: subscribe_opts}) do
      {:consumer, parent, subscribe_to: subscribe_opts}
    end

    def handle_events(events, _from, parent) do
      send(parent, {self(), events})
      {:noreply, [], parent}
    end
  end

  defmodule DummyProducer do
    use GenStage

    # Starts the consumer process
    def start_link() do
      GenStage.start_link(__MODULE__, nil, name: __MODULE__)
    end

    def init(state) do
      {:producer, state}
    end

    def handle_demand(_demand, _state) do
      {:noreply, [], nil}
    end
  end

  test "broadcaster as producer" do
    # arrange
    proc_under_test_name = :broadcaster_test
    subscribe_opts = [{proc_under_test_name, max_demand: 10}]

    {:ok, bc_pid} = Pipeline.Stages.Broadcaster.start_link(%{name: proc_under_test_name})
    {:ok, fwd_1_pid} = Forwarder.start_link(%{name: :fwd_1, parent: self(), subscribe_to: subscribe_opts})
    {:ok, fwd_2_pid} = Forwarder.start_link(%{name: :fwd_2, parent: self(), subscribe_to: subscribe_opts})

    # act
    send(bc_pid, Enum.to_list(1..11))
    # assert
    assert_receive {^fwd_1_pid, [1, 2, 3, 4, 5]}
    assert_receive {^fwd_2_pid, [1, 2, 3, 4, 5]}
    assert_receive {^fwd_1_pid, [6, 7, 8, 9, 10]}
    assert_receive {^fwd_2_pid, [6, 7, 8, 9, 10]}
    assert_receive {^fwd_1_pid, [11]}
    assert_receive {^fwd_2_pid, [11]}
    assert %{state: %{pending_demand: 19}} = :sys.get_state(bc_pid)
    # clean
    Process.unlink(fwd_1_pid)
    Process.exit(fwd_1_pid, :kill)
    Process.unlink(fwd_2_pid)
    Process.exit(fwd_2_pid, :kill)
    Process.unlink(bc_pid)
    Process.exit(bc_pid, :kill)
  end

  test "broadcaster as producer-consumer" do
    # arrange
    proc_under_test_name = :broadcaster_prod_cons_test
    subscribe_opts = [{proc_under_test_name, max_demand: 10, min_demand: 5}]

    {:ok, dummy_pid} = DummyProducer.start_link()
    {:ok, bc_pid} = Pipeline.Stages.Broadcaster.start_link(%{name: proc_under_test_name, subscribe_options: [{DummyProducer, max_demand: 1}]})

    # act
    for n <- 1..4 do
      send(bc_pid, n)
    end
    assert %{buffer: {{[4, 3, 2], [1]}, _a, _b}} = :sys.get_state(bc_pid)
    send(bc_pid, 5)
    {:ok, fwd_pid} = Forwarder.start_link(%{name: :fwd, parent: self(), subscribe_to: subscribe_opts})
    # assert
    assert_receive {^fwd_pid, [1, 2, 3, 4, 5]}
    send(bc_pid, Enum.to_list(6..11))
    assert_receive {^fwd_pid, [6, 7, 8, 9, 10]}
    assert_receive {^fwd_pid, [11]}
    # clean
    Process.unlink(fwd_pid)
    Process.exit(fwd_pid, :kill)
    Process.unlink(bc_pid)
    Process.exit(bc_pid, :kill)
    Process.unlink(dummy_pid)
    Process.exit(dummy_pid, :kill)
  end

end
