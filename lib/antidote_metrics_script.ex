defmodule AntidoteMetricsScript do

  require Logger

  @num_operations 50000
  @cookie :antidote
  @mode :topkd
  #@mode :topk
  @events [{:topkd_add, 95}, {:topkd_del, 100}]
  #@events [{:topk_add, 100}]
  @nodes 20
  @ops_per_metric 5000
  @rpc_timeout 1000*1000

  defmodule State do
    defstruct [
      target: :'antidote1@127.0.0.1',
      num_players: 10000
    ]
  end

  def main(_args \\ []) do
    targets = ['antidote@34.250.54.226', 'antidote@34.248.200.69', 'antidote@34.250.113.128', 'antidote@34.250.31.55', 'antidote@34.250.121.179']
    |> Enum.map(fn(x) -> :erlang.list_to_atom(x) end)

    num_players = 10000

    initial_states = targets
    |> Stream.cycle()
    |> Stream.take(@nodes)
    |> Enum.map(fn(t) ->
      %State{target: t, num_players: num_players}
    end)

    # start our node
    {:ok, _} = :net_kernel.start([my_name(), :longnames])

    # set cookie
    :erlang.set_cookie(my_name(), @cookie)
    Enum.each(targets, fn(target) -> :erlang.set_cookie(target, @cookie) end)

    # seed random number
    :rand.seed(:exsplus, {:erlang.phash2([my_name()]), :erlang.monotonic_time(), :erlang.unique_integer()})

    Coordinator.start_link(%{ops: 0, total: @num_operations, ops_per_metric: @ops_per_metric, mode: @mode, nodes: targets})
    states = Enum.map(initial_states, fn(initial_state) ->
      Task.async(fn ->
        Enum.reduce(0..div(@num_operations, @nodes), initial_state, fn(op_number, state) ->
          event = get_random_event()
          state = run(event, op_number, state)
          Coordinator.inc()
          state
        end)
      end)
    end)

    Enum.map(states, fn(s) -> Task.await(s, :infinity) end)

    :timer.sleep(10000)
  end

  def run(:topkd_add, _op_number, state) do
    key = :topkd
    target = state.target
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(250000)
    object_ccrdt = {key, :antidote_ccrdt_topk_rmv, :topkd_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
    element = {player_id, score}
    updates = [{object_ccrdt, :add, element}, {object_crdt, :add, element}]

    rpc(target, :antidote, :update_objects, [:ignore, [], updates])

    state
  end

  def run(:topkd_del, _op_number, state) do
    key = :topkd
    target = state.target
    object_ccrdt = {key, :antidote_ccrdt_topk_rmv, :topkd_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
    {[result], _} = rpc(state.target, :antidote, :read_objects, [:ignore, [], [object_crdt]])
    result = :orddict.fetch_keys(result)
    element = case result do
      [] -> nil
      list -> Enum.random(list)
    end

    if !is_nil(element) do
      {element_id, _} = element
      elements = result
      |> Enum.filter(fn({id, _}) -> id == element_id end)

      updates = [{object_ccrdt, :rmv, element_id}, {object_crdt, :remove_all, elements}]

      rpc(target, :antidote, :update_objects, [:ignore, [], updates])
    end

    state
  end

  def run(:topk_add, _op_number, state) do
    key = :topk
    target = state.target
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(250000)
    object_ccrdt = {key, :antidote_ccrdt_topk, :topk_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topk_crdt}
    element = {player_id, score}
    element_crdt = {score, player_id} # inverted order so we get sorting in :gb_sets for free

    {[orset], _} = rpc(target, :antidote, :read_objects, [:ignore, [], [object_crdt]])
    set = :gb_sets.from_list(:orddict.fetch_keys(orset))

    maxk = :gb_sets.add(element_crdt, set) |> max_k()

    # this essentially does the computation for the or-set top-k on the "client" side
    if :gb_sets.is_member(element_crdt, maxk) do
      rem = :gb_sets.difference(set, maxk) |> :gb_sets.to_list()

      rpc(target,
          :antidote,
          :update_objects,
          [:ignore,
           [],
           [{object_ccrdt, :add, element},
            {object_crdt, :add, element_crdt},
            {object_crdt, :remove_all, rem}]])
    else
      rpc(target, :antidote, :update_objects, [:ignore, [], [{object_ccrdt, :add, element}]])
    end

    state
  end

  defp max_k(set) do
    {top, _, _} = set
    |> :gb_sets.to_list()
    |> Enum.reverse()
    |> Enum.reduce_while({:gb_sets.new(), MapSet.new(), 100}, fn({_, id} = e, {top, cache, remaining_k}) ->
      {t, c, k} = if MapSet.member?(cache, id) do
        {top, cache, remaining_k}
      else
        {:gb_sets.add(e, top), MapSet.put(cache, id), remaining_k - 1}
      end

      if k > 0 do
        {:cont, {t, c, k}}
      else
        {:halt, {t, c, k}}
      end
    end)

    top
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
  def rpc(target, module, function, args) do
    case :rpc.call(target, module, function, args, @rpc_timeout) do
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

  defp my_name() do
    :erlang.list_to_atom('metrics' ++ '@127.0.0.1')
  end

  defp graceful_shutdown() do
    Logger.flush
    System.halt(0)
  end

end
