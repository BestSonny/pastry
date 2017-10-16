defmodule GSP do

  @default_state %{
    numJoined: 0,
    numRouted: 0,
  }

  defp parse_args(args) do
    {_, str, _} = args |> OptionParser.parse
    str
  end

  def main(args) do
    str = args |> parse_args
    [numNodes, numRequests] = str
    numNodes = String.to_integer(numNodes)
    numRequests = String.to_integer(numRequests)
    pastry(numNodes, numRequests)
  end

  def pastry(numNodes, numRequests, base \\ 2) do
    #process registry
    {:ok,server_pid} = ProcessRegistry.start_link

    base = round(:math.pow(2, base))
    logBase = round(:math.ceil(:math.log(numNodes) / :math.log(base)))
    nodeIDSpace = round(:math.pow(base, logBase))
    numFirst = if numNodes <= 1024 do numNodes else 1024 end
    IO.puts "Number Of Nodes: #{numNodes}"
    IO.puts "Node ID Space: 0 ~ #{nodeIDSpace - 1}"
    IO.puts "Number Of Request Per Node: #{numRequests}"
    nodeIDs = for node_id <- 0..nodeIDSpace-1 do
      node_id
    end
    nodeIDs = Enum.shuffle(nodeIDs)
    IO.inspect length(nodeIDs)
    IO.inspect numFirst
    IO.inspect logBase
    firstGroup = for node_id <- 0..numFirst-1 do
      Enum.at(nodeIDs, node_id)
    end
    pids = for node_id <- 0..numNodes-1 do
      id = Enum.at(nodeIDs, node_id)
      emptyTable = for row <- 0..logBase-1 do
        for column <- 0..base-1 do
          -1
        end
      end
      pid = PastryActor.new(%{registry: server_pid, master: self(), numNodes: numNodes, numRequests: numRequests, id: id, table: emptyTable, nodeIDSpace: nodeIDSpace, base: base, length: logBase})
      ProcessRegistry.register_name(id, pid)
      pid
    end

    ProcessRegistry.register_name(-1, self())

    send self(), :go
    run(Map.merge(@default_state, %{firstGroup: firstGroup, pids: pids, numFirst: numFirst, numNodes: numNodes}))
  end

  defp run(state) do
    receive do
      :go ->
        IO.puts "Join Begins..."
        for node_id <- 0..state.numFirst-1 do
          pid = Enum.at(state.pids, node_id)
          send pid, {:first_join, state.firstGroup}
        end
        run(state)

      :second_join ->
        run(state)
      :begin_route ->
        IO.puts "Join Finished!"
        IO.puts "Routing Begins..."
        for pid <- state.pids do
          send pid, :begin_route
        end
        run(state)

      :join_finish ->
        numJoined = state.numJoined + 1
        new_state = Map.put(state, :numJoined, numJoined)
        if numJoined >= state.numNodes do
          send self(), :begin_route
        else
          send self(), :second_join
        end
        run(new_state)

      {:route_finished, fromId, toId, hops} ->
        run(state)
    end
  end
end


defmodule PastryActor do

  @default_state %{
    id: 0,
    lessLeaf: [],
    largerLeaf: [],
    table: [],
    numOfBack: 0,
    nodeIDSpace: 0,
    base: 0,
  }

  def new(state \\ %{}) do
    spawn_link fn ->
      Map.merge(@default_state, state) |> run
    end
  end

  defp run(state) do
    receive do
      :go ->
        run(state)
      {:first_join, firstGroup} ->
        firstGroup = List.delete(firstGroup, state.id)
        #IO.inspect "ID #{state.id} and #{inspect firstGroup}"
        new_state = addBuffer(firstGroup, state, length(firstGroup)-1)
        IO.inspect new_state
        send state.master, :join_finish
        run(new_state)

      {msg, fromId, toId, hops} ->
        case msg do
          "route" ->
          if fromId == toId do
            send state.master, {:route_finished, fromId, toId, hops+1}
          else
            fromIdString = toBaseString(fromId, state.base, state.length)
            toIdString = toBaseString(toId, state.base, state.length)
            samePre = String.length(commonPrefix([fromIdString, toIdString]))
            value = String.to_integer(String.at(toIdString, samePre))
            lengthLessLeaf =  length(state.lessLeaf)
            lengthLargerLeaf =  length(state.largerLeaf)

            cond do
              (lengthLessLeaf > 0 && toId >= Enum.min(state.lessLeaf) && toId < state.id) || (lengthLargerLeaf > 0 && toId <= Enum.max(state.largerLeaf) && toId > state.id) ->
                diff = abs(toId - state.id)
                if toId < state.id do
                  nearest_value = Enum.min_by(state.lessLeaf, &abs(&1 - toId))
                  index = Enum.find_index(state.lessLeaf, fn(x) -> x== nearest_value end)
                else
                  nearest_value = Enum.min_by(state.largerLeaf, &abs(&1 - toId))
                  index = Enum.find_index(state.largerLeaf, fn(x) -> x== nearest_value end)
                end
                leafDiff = abs(nearest_value - toId)
                if diff > leafDiff do
                  send ProcessRegistry.whereis_name(nearest_value), {msg, fromId, toId, hops + 1}
                else
                  send state.master, {:route_finished, fromId, toId, hops+1}
                end

              lengthLessLeaf < 4 && lengthLessLeaf > 0 && toId < Enum.min(state.lessLeaf) ->

                send ProcessRegistry.whereis_name(Enum.min(state.lessLeaf)), {msg, fromId, toId, hops + 1}

              lengthLargerLeaf < 4 && lengthLargerLeaf > 0 && toId > Enum.max(state.largerLeaf) ->

                send ProcessRegistry.whereis_name(Enum.max(state.largerLeaf)), {msg, fromId, toId, hops + 1}

              (lengthLessLeaf == 0 && toId < state.id) || (lengthLargerLeaf == 0 && toId > state.id)  ->

                send state.master, {:route_finished, fromId, toId, hops+1}

              Enum.at(Enum.at(state.table, samePre), value) != -1 ->

                send ProcessRegistry.whereis_name(Enum.at(Enum.at(state.table, samePre), value)), {msg, fromId, toId, hops + 1}

              toId > state.id ->

                send ProcessRegistry.whereis_name(Enum.max(state.largerLeaf)), {msg, fromId, toId, hops + 1}

              toId <state.id ->

                send ProcessRegistry.whereis_name(Enum.min(state.lessLeaf)), {msg, fromId, toId, hops + 1}

              true ->

            end
          end
        end
        run(state)

      {:periodical, msg} ->
        random = Enum.random(0..state.nodeIDSpace-1)
        send self(), {msg, state.id, random, -1}
        Process.send_after(self(), {:periodical, msg}, 5)
        run(state)

      :begin_route ->
        for index <- 0..state.numRequests-1 do
          send self(), {:periodical,"route"}
        end
        run(state)
    end
  end

  def commonPrefix([]), do: ""
  def commonPrefix(strs) do
    min = Enum.min(strs)
    max = Enum.max(strs)
    index = Enum.find_index(0..String.length(min), fn i -> String.at(min,i) != String.at(max,i) end)
    if index, do: String.slice(min, 0, index), else: min
  end


  def toBaseString(id, base, length) do
    Integer.to_string(id, base) |> String.pad_leading(length, "0")
  end

  def addBuffer(firstGroup, state, index) when index <= 0 do
    node_id = Enum.at(firstGroup, index)
    cond do
      node_id > state.id && !Enum.member?(state.largerLeaf, node_id) ->
        if length(state.largerLeaf) < state.base do
          state = Map.put(state, :largerLeaf, [node_id | state.largerLeaf])
        else
          max_value = Enum.max(state.largerLeaf)
          if node_id < max_value do
            new_leaf = List.delete(state.largerLeaf, max_value)
            new_leaf = [node_id | new_leaf]
            state = Map.put(state, :largerLeaf, new_leaf)
          end
        end
      node_id < state.id && !Enum.member?(state.lessLeaf, node_id) ->
        if length(state.lessLeaf) < state.base do
          state = Map.put(state, :lessLeaf, [node_id | state.lessLeaf])
        else
          min_value = Enum.min(state.lessLeaf)
          if node_id > min_value do
            new_leaf = List.delete(state.lessLeaf, min_value)
            new_leaf = [node_id | new_leaf]
            state = Map.put(state, :lessLeaf, new_leaf)
          end
        end
    end
    id_string = toBaseString(state.id, state.base, state.length)
    node_string = toBaseString(node_id, state.base, state.length)
    samePre = String.length(commonPrefix([id_string, node_string]))
    value = String.to_integer(String.at(node_string, samePre))
    if Enum.at(Enum.at(state.table, samePre), value) == -1 do
      new_table = List.replace_at(state.table, samePre, List.replace_at(Enum.at(state.table, samePre), value, node_id))
      state = Map.put(state, :table, new_table)
    end
    state
  end

  def addBuffer(firstGroup, state, index) do
      node_id = Enum.at(firstGroup, index)
      cond do
        node_id > state.id && !Enum.member?(state.largerLeaf, node_id) ->
          if length(state.largerLeaf) < state.base do
            state = Map.put(state, :largerLeaf, [node_id | state.largerLeaf])
          else
            max_value = Enum.max(state.largerLeaf)
            if node_id < max_value do
              new_leaf = List.delete(state.largerLeaf, max_value)
              new_leaf = [node_id | new_leaf]
              state = Map.put(state, :largerLeaf, new_leaf)
            end
          end
        node_id < state.id && !Enum.member?(state.lessLeaf, node_id) ->
          if length(state.lessLeaf) < state.base do
            state = Map.put(state, :lessLeaf, [node_id | state.lessLeaf])
          else
            min_value = Enum.min(state.lessLeaf)
            if node_id > min_value do
              new_leaf = List.delete(state.lessLeaf, min_value)
              new_leaf = [node_id | new_leaf]
              state = Map.put(state, :lessLeaf, new_leaf)
            end
          end
      end
      id_string = toBaseString(state.id, state.base, state.length)
      node_string = toBaseString(node_id, state.base, state.length)
      samePre = String.length(commonPrefix([id_string, node_string]))
      value = String.to_integer(String.at(node_string, samePre))
      if Enum.at(Enum.at(state.table, samePre), value) == -1 do
        new_table = List.replace_at(state.table, samePre, List.replace_at(Enum.at(state.table, samePre), value, node_id))
        state = Map.put(state, :table, new_table)
      end
      addBuffer(firstGroup, state, index-1)
  end

end


defmodule ProcessRegistry do
  import Kernel, except: [send: 2]

  use GenServer

  # Client API #
  def start_link do
    GenServer.start_link(__MODULE__, nil, name: :registry)
  end

  def register_name(key, pid) when is_pid(pid) do
    GenServer.call(:registry, {:register_name, key, pid})
  end

  def unregister_name(key) do
    GenServer.call(:registry, {:unregister_name, key})
  end

  def whereis_name(key) do
    GenServer.call(:registry, {:whereis_name, key})
  end

  def send(key, msg) do
    case whereis_name(key) do
      pid when is_pid(pid) ->
        Kernel.send(pid, msg)
        pid

      :undefined -> {:badarg, {key, msg}}
    end
  end

  # Server API #
  def init(nil) do
    {:ok, %{}}
  end

  def handle_call({:unregister_name, key}, _from, registry) do
    {:reply, key, deregister(registry, key)}
  end

  def handle_call({:register_name, key, pid}, _from, registry) do
    case Map.get(registry, key, nil) do
      nil ->
        Process.monitor(pid)
        registry = Map.put(registry, key, pid)
        {:reply, :yes, registry}

      _ -> {:reply, :no, registry}
    end
  end

  def handle_call({:whereis_name, key}, _from, registry) do
    {:reply, Map.get(registry, key, :undefined), registry}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, registry) do
    {:noreply, deregister(registry, pid)}
  end

  def handle_info(_info, registry), do: {:noreply, registry}

  # Helper Functions #
  defp deregister(registry, pid) when is_pid(pid) do
    case Enum.find(registry, nil, fn({_key, cur_pid}) -> cur_pid == pid end) do
      nil -> registry
      {key, _pid} -> deregister(registry, key)
    end
  end

  defp deregister(registry, key) do
    Map.delete(registry, key)
  end
end
