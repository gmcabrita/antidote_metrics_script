defmodule AntidoteMetricsScript do

  require Logger

  @folder "results/"
  @num_operations 3000
  @cookie :antidote
  @events [{:topkd_add, 95}, {:topkd_del, 100}]
  #@events [{:topk_add, 100}]
  @replicas 3
  @nodes 3
  @ops_per_metric div(@num_operations, @nodes)
  @ops_per_metric_per_node div(@ops_per_metric, 5)

  defmodule State do
    defstruct [
      target: :'antidote1@127.0.0.1',
      num_players: 25000,
      ccrdt_metrics: [],
      crdt_metrics: [],
      last_commit: :ignore
    ]
  end

  def main(_args \\ []) do
    targets = ['antidote1@127.0.0.1', 'antidote2@127.0.0.1', 'antidote3@127.0.0.1', 'antidote4@127.0.0.1', 'antidote5@127.0.0.1']
    |> Enum.take(@nodes)
    |> Enum.map(fn(x) -> :erlang.list_to_atom(x) end)

    num_players = 25000

    initial_states = Enum.map(targets, fn(t) ->
      %State{target: t, num_players: num_players}
    end)

    # start our node
    {:ok, _} = :net_kernel.start([my_name(), :longnames])

    # set cookie
    :erlang.set_cookie(my_name(), @cookie)
    Enum.each(targets, fn(target) -> :erlang.set_cookie(target, @cookie) end)

    # seed random number
    :rand.seed(:exsplus, {:erlang.phash2([my_name()]), :erlang.monotonic_time(), :erlang.unique_integer()})

    states = Enum.map(initial_states, fn(initial_state) ->
      Task.async(fn ->
        Enum.reduce(0..div(@num_operations, @nodes), initial_state, fn(op_number, state) ->
          event = get_random_event()
          run(event, op_number, state)
        end)
      end)
    end)

    final_states = Enum.map(states, fn(s) -> Task.await(s, :infinity) end)

    store_metrics(final_states)
  end

  def run(:topkd_add, op_number, state) do
    key = :topkd
    target = state.target
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(1000000000)
    object_ccrdt = {key, :antidote_ccrdt_topk_with_deletes, :topkd_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
    element = {player_id, score}
    updates = [{object_ccrdt, :add, element}, {object_crdt, :add, element}]

    time = rpc(target, :antidote, :update_objects, [state.last_commit, [], updates])

    {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

    %{state | ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics, last_commit: time}
  end

  def run(:topkd_del, op_number, state) do
    key = :topkd
    target = state.target
    object_ccrdt = {key, :antidote_ccrdt_topk_with_deletes, :topkd_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
    {[result], time} = rpc(state.target, :antidote, :read_objects, [state.last_commit, [], [object_ccrdt]])
    {_, internal, _, _} = result
    player_id = case Map.keys(internal) do
      [] -> nil
      list -> Enum.random(list)
    end

    if is_nil(player_id) do
      state
    else
      elements = Map.get(internal, player_id)
      |> :gb_sets.to_list()
      |> Enum.map(fn({id, score, _}) -> {id, score} end)

      updates = [{object_ccrdt, :del, player_id}, {object_crdt, :remove_all, elements}]

      time = rpc(target, :antidote, :update_objects, [time, [], updates])

      {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

      %{state | ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics, last_commit: time}
    end
  end

  def run(:topk_add, op_number, state) do
    key = :topk
    target = state.target
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(1000000000)
    object_ccrdt = {key, :antidote_ccrdt_topk, :topk_ccrdt}
    object_crdt = {key, :antidote_crdt_gset, :topk_crdt}
    element = {player_id, score}
    updates = [{object_ccrdt, :add, element}, {object_crdt, :add, element}]

    # ignore result of the rpc, if there's some error the program will exit
    time = rpc(target, :antidote, :update_objects, [state.last_commit, [], updates])

    {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

    %{state | ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics, last_commit: time}
  end

  # generates a random event from a weighted list of events
  defp get_random_event() do
    roll = :rand.uniform(101) - 1
    {event, _} = @events
    |> Enum.drop_while(fn({_, p}) -> roll > p end)
    |> List.first()

    event
  end

  # wraps erlang rpc call function, if there's an error it logs the error and exits the application
  defp rpc(target, module, function, args) do
    case :rpc.call(target, module, function, args) do
      {:ok, result, _time} -> {result, :ignore}
      {:ok, time} -> time
      {:error, reason} ->
        Logger.error("Error #{inspect([reason])}")
        graceful_shutdown()
      :error ->
        Logger.error("Error.")
        graceful_shutdown()
      {:badrpc, reason} ->
        Logger.error("Bad RPC #{inspect([reason])}")
        graceful_shutdown()
      {result1, result2} ->
        {result1, result2}
    end
  end

  # returns the size of an object
  defp get_size(object) do
    byte_size(:erlang.term_to_binary(object))
  end

  defp get_replica_size(:antidote_ccrdt_topk_with_deletes, {_, hidden, _, _, _}) do
    hidden
    |> :maps.values()
    |> Enum.reduce(0, fn(x, acc) -> acc + :sets.size(x) end)
  end

  defp get_replica_size(:antidote_crdt_orset, orset) do
    :orddict.size(orset)
  end

  defp get_replica_size(:antidote_ccrdt_topk, {top, _, _}) do
    :maps.size(top)
  end

  defp get_replica_size(:antidote_crdt_gset, set) do
    :ordsets.size(set)
  end

  # checks if metrics need to be updateds given the current op_number
  defp update_metrics(op_number, state, object_ccrdt, object_crdt) do
    if rem(op_number + 1, 100) == 0, do: Logger.info("Op number: #{op_number + 1}")
    if rem(op_number + 1, @ops_per_metric_per_node) == 0 do
      {ccrdt, crdt} = get_metrics(state, object_ccrdt, object_crdt)
      {state.ccrdt_metrics ++ [ccrdt], state.crdt_metrics ++ [crdt]}
    else
      {state.ccrdt_metrics, state.crdt_metrics}
    end
  end

  # retrieves metrics
  defp get_metrics(state, {_, typecc, _} = object_ccrdt, {_, typec, _} = object_crdt) do
    # get average replica sizes
    {res, _} = rpc(state.target, :antidote, :read_objects, [state.last_commit, [], [object_ccrdt, object_crdt]])
    [value_ccrdt, value_crdt] = res
    {sizes_ccrdt, sizes_crdt} = {get_replica_size(typecc, value_ccrdt), get_replica_size(typec, value_crdt)}

    # get total message payloads
    {ccrdt_payload, crdt_payload} = rpc(state.target, :antidote, :message_payloads, [])

    {%{size: sizes_ccrdt, payload: ccrdt_payload}, %{size: sizes_crdt, payload: crdt_payload}}
  end

  defp store_metrics(states) do
    empty = [0, 0, 0, 0, 0]
    {ccrdt_sizes, ccrdt_payloads, crdt_sizes, crdt_payloads} = states
    |> Stream.map(fn(state) ->
      {ccs, ccp} = Enum.reduce(state.ccrdt_metrics, {[], []}, fn(m, {s, p}) ->
        {s ++ [m.size], p ++ [m.payload]}
      end)

      {cs, cp} = Enum.reduce(state.crdt_metrics, {[], []}, fn(m, {s, p}) ->
        {s ++ [m.size], p ++ [m.payload]}
      end)

      {ccs, ccp, cs, cp}
    end)
    |> Enum.reduce({empty, empty, empty, empty}, fn ({ccs, ccp, cs, cp}, {ccsa, ccpa, csa, cpa}) ->
      ccsr = Stream.zip(ccs, ccsa) |> Enum.map(fn({i,j}) -> i + j end)
      ccpr = Stream.zip(ccp, ccpa) |> Enum.map(fn({i,j}) -> i + j end)
      csr = Stream.zip(cs, csa) |> Enum.map(fn({i,j}) -> i + j end)
      cpr = Stream.zip(cp, cpa) |> Enum.map(fn({i,j}) -> i + j end)
      {ccsr, ccpr, csr, cpr}
    end)

    ccrdt_sizes = Enum.map(ccrdt_sizes, fn (i) -> i / @replicas end)
    crdt_sizes = Enum.map(crdt_sizes, fn (i) -> i / @replicas end)

    File.mkdir_p(@folder)

    Logger.info("Total message payloads:")
    {:ok, file} = File.open("#{@folder}payload.dat", [:append])
    Stream.zip(ccrdt_payloads, crdt_payloads)
    |> Enum.reduce(1, fn({m1,m2}, acc) ->
      line = "#{acc * @ops_per_metric_per_node * @nodes}\t#{m1}\t#{m2}\n"
      IO.binwrite(file, line)
      Logger.info(line)

      acc + 1
    end)
    File.close(file)

    Logger.info("Average replica sizes:")
    {:ok, file} = File.open("#{@folder}size.dat", [:append])
    Stream.zip(ccrdt_sizes, crdt_sizes)
    |> Enum.reduce(1, fn({m1,m2}, acc) ->
      line = "#{acc * @ops_per_metric_per_node * @nodes}\t#{m1}\t#{m2}\n"
      IO.binwrite(file, line)
      Logger.info(line)

      acc + 1
    end)
    File.close(file)
  end

  defp my_name() do
    :erlang.list_to_atom('metrics' ++ '@127.0.0.1')
  end

  defp graceful_shutdown() do
    Logger.flush
    System.halt(0)
  end

end
