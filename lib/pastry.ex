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
      PastryActor.new(%{master: self(), numNodes: numNodes, numRequests: numRequests, id: id, table: emptyTable, nodeIDSpace: nodeIDSpace, base: base, length: logBase})
    end

    send self(), :go
    run(Map.merge(@default_state, %{firstGroup: firstGroup, pids: pids, numFirst: numFirst}))
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

      :join_finish ->
        numJoined = state.numJoined + 1
        new_state = Map.put(state, :numJoined, numJoined)
        #IO.puts numJoined
        run(new_state)

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
    pid = spawn_link fn ->
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
