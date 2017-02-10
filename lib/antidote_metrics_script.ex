defmodule AntidoteMetricsScript do

  require Logger

  @folder "results"
  @num_operations 500000
  @cookie :antidote
  @events [{:topkd_add, 95}, {:topkd_del, 100}]
  #@events [{:topk_add, 100}]
  @ops_per_metric 100000
  defmodule State do
    defstruct [
      targets: ['antidote1@127.0.0.1', 'antidote2@127.0.0.1', 'antidote3@127.0.0.1', 'antidote4@127.0.0.1', 'antidote5@127.0.0.1'],
      num_players: 25000,
      added_elements: %{},
      ccrdt_metrics: [],
      crdt_metrics: []
    ]
  end

  def main(_args \\ []) do
    targets = ['antidote1@127.0.0.1', 'antidote2@127.0.0.1', 'antidote3@127.0.0.1', 'antidote4@127.0.0.1', 'antidote5@127.0.0.1']
    |> Enum.map(fn(x) -> :erlang.list_to_atom(x) end)
    num_players = 25000

    # start our node
    {:ok, _} = :net_kernel.start([my_name(), :longnames])

    # set cookie
    :erlang.set_cookie(my_name(), @cookie)
    Enum.each(targets, fn(target) -> :erlang.set_cookie(target, @cookie) end)

    # seed random number
    :rand.seed(:exsplus, {:erlang.phash2([my_name()]), :erlang.monotonic_time(), :erlang.unique_integer()})

    initial_state = %State{targets: targets, num_players: num_players}
    final_state = Enum.reduce(0..@num_operations, initial_state, fn (op_number, state) ->
      event = get_random_event()
      run(event, op_number, state)
    end)

    store_metrics(final_state)
  end

  def run(:topkd_add, op_number, state) do
    key = :topkd
    target = Enum.random(state.targets)
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(1000000000)
    object_ccrdt = {key, :antidote_ccrdt_topk_with_deletes, :topkd_ccrdt}
    object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
    element = {player_id, score}
    updates = [{object_ccrdt, :add, element}, {object_crdt, :add, element}]

    # ignore result of the rpc, if there's some error the program will exit
    rpc(target, :antidote, :update_objects, [:ignore, [], updates])
    added_elements = Map.update(
      state.added_elements,
      player_id,
      [element],
      fn (old) -> [element | old] end
    )

    {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

    %{state | added_elements: added_elements, ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics}
  end

  def run(:topkd_del, op_number, state) do
    key = :topkd
    target = Enum.random(state.targets)
    player_id = case Map.keys(state.added_elements) do
      [] -> nil
      list -> Enum.random(list)
    end

    if is_nil(player_id) do
      state
    else
      object_ccrdt = {key, :antidote_ccrdt_topk_with_deletes, :topkd_ccrdt}
      object_crdt = {key, :antidote_crdt_orset, :topkd_crdt}
      elements = Map.get(state.added_elements, player_id)
      updates = [{object_ccrdt, :del, player_id}, {object_crdt, :remove_all, elements}]

      # ignore result of the rpc, if there's some error the program will exit
      rpc(target, :antidote, :update_objects, [:ignore, [], updates])
      added_elements = Map.delete(state.added_elements, player_id)

      {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

      %{state | added_elements: added_elements, ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics}
    end
  end

  def run(:topk_add, op_number, state) do
    key = :topk
    target = Enum.random(state.targets)
    player_id = :rand.uniform(state.num_players)
    score = :rand.uniform(1000000000)
    object_ccrdt = {key, :antidote_ccrdt_topk, :topk_ccrdt}
    object_crdt = {key, :antidote_crdt_gset, :topk_crdt}
    element = {player_id, score}
    updates = [{object_ccrdt, :add, element}, {object_crdt, :add, element}]

    # ignore result of the rpc, if there's some error the program will exit
    rpc(target, :antidote, :update_objects, [:ignore, [], updates])

    {ccrdt_metrics, crdt_metrics} = update_metrics(op_number, state, object_ccrdt, object_crdt)

    %{state | ccrdt_metrics: ccrdt_metrics, crdt_metrics: crdt_metrics}
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
      {:ok, result} -> result
      {:error, reason} ->
        Logger.error("Error #{inspect([reason])}")
        graceful_shutdown()
      :error ->
        Logger.error("Error.")
        graceful_shutdown()
      {:badrpc, reason} ->
        Logger.error("Bad RPC #{inspect([reason])}")
        graceful_shutdown()
    end
  end

  # returns the size of an object
  defp get_size(object) do
    byte_size(:erlang.term_to_binary(object))
  end

  # checks if metrics need to be updateds given the current op_number
  defp update_metrics(op_number, state, object_ccrdt, object_crdt) do
    if rem(op_number + 1, @ops_per_metric) == 0 do
      Logger.info("Op number: #{op_number + 1}")
      {ccrdt, crdt} = get_metrics(state, object_ccrdt, object_crdt)
      {state.ccrdt_metrics ++ [ccrdt], state.crdt_metrics ++ [crdt]}
    else
      {state.ccrdt_metrics, state.crdt_metrics}
    end
  end

  # retrieves metrics
  defp get_metrics(state, object_ccrdt, object_crdt) do
    # get average replica sizes
    {sizes_ccrdt, sizes_crdt} = Enum.map(state.targets, fn (t) ->
      {:ok, res, _} = rpc(t, :antidote, :read_objects, [:ignore, [], [object_ccrdt, object_crdt]])
      [value_ccrdt, value_crdt] = res
      {get_size(value_ccrdt), get_size(value_crdt)}
    end)
    |> Enum.unzip()

    avg_size_ccrdt = sizes_ccrdt / Enum.count(sizes_ccrdt)
    avg_size_crdt = sizes_crdt / Enum.count(sizes_crdt)

    # get total message payloads
    {ccrdt_payload, crdt_payload} = Enum.map(state.targets, fn (t) ->
      rpc(t, :antidote, :message_payloads, [])
    end)

    {%{size: avg_size_ccrdt, payload: ccrdt_payload}, %{size: avg_size_crdt, payload: crdt_payload}}
  end

  defp store_metrics(state) do
    File.mkdir_p(@folder)

    Logger.info("Total message payloads:")
    {:ok, file} = File.open("#{@folder}payload.dat", [:append])
    Enum.zip(state.ccrdt_metrics.payload, state.crdt_metrics.payload)
    |> Enum.reduce(1, fn({m1,m2}, acc) ->
      line = "#{acc * @ops_per_metric}\t#{m1}\t#{m2}\n"
      IO.binwrite(file, line)
      Logger.info(line)
    end)
    File.close(file)

    Logger.info("Average replica sizes:")
    {:ok, file} = File.open("#{@folder}size.dat", [:append])
    Enum.zip(state.ccrdt_metrics.size, state.crdt_metrics.size)
    |> Enum.reduce(1, fn({m1,m2}, acc) ->
      line = "#{acc * @ops_per_metric}\t#{m1}\t#{m2}\n"
      IO.binwrite(file, line)
      Logger.info(line)
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
