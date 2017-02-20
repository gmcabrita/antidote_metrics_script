defmodule Coordinator do

  alias AntidoteMetricsScript, as: Metrics

  use GenServer
  require Logger

  @folder "results/"

  # API

  def start_link() do
    GenServer.start_link(__MODULE__, 0, name: __MODULE__)
  end

  def inc() do
    GenServer.cast(__MODULE__, :inc)
  end

  # Internals

  def init(state \\ %{ops: 0, total: 100000, ops_per_metric: 20000, mode: :topkd, nodes: []}) do
    {:ok, state}
  end

  def handle_cast(:inc, _from, state) do
    ops = state[:ops] + 1
    if rem(ops, 1000) == 0, do: Logger.info("Currently at: #{ops}")
    if rem(ops, state[:ops_per_metric]) == 0 do
      if ops == state[:total] do
        get_metrics(ops, state[:mode], state[:servers])
        graceful_shutdown()
      else
        spawn(fn -> get_metrics(ops, state[:mode], state[:servers]) end)
      end
    end
    {:noreply, %{state | ops: ops}}
  end

  # returns the size of an object
  defp get_size(object) do
    byte_size(:erlang.term_to_binary(object))
  end

  defp get_replica_size(:antidote_ccrdt_topk, topk) do
    get_size(topk)
  end

  defp get_replica_size(:antidote_crdt_orset, orset) do
    get_size(orset)
  end

  defp get_replica_size(:antidote_ccrdt_topk_rmv, {_, all, removals, vc, min, size}) do
    all = all
    |> :maps.values()
    |> Enum.reduce(:gb_sets.new(), fn(x, acc) -> :gb_sets.union(x, acc) end)
    |> :gb_sets.to_list()

    get_size({all, removals, vc, min, size})
  end

  defp get_num_elements(:antidote_ccrdt_topk_rmv, {_, all, _, _, _, _}) do
    all = all
    |> :maps.values()
    |> Enum.reduce(:gb_sets.new(), fn(x, acc) -> :gb_sets.union(x, acc) end)
    :gb_sets.size(all)
  end

  defp get_num_elements(:antidote_crdt_orset, orset) do
    :orddict.size(orset)
  end

  defp get_num_elements(:antidote_ccrdt_topk, {top, _, _}) do
    :maps.size(top)
  end

  defp get_metrics(op_number, mode, servers) do
    {ccrdt, crdt} = case mode do
      :topkd ->
        ccrdt = {:topkd, :antidote_ccrdt_topk_rmv, :topkd_ccrdt}
        crdt = {:topkd, :antidote_crdt_orset, :topkd_crdt}
        {ccrdt, crdt}
      :topk ->
        ccrdt = {:topk, :antidote_ccrdt_topk, :topk_ccrdt}
        crdt = {:topk, :antidote_crdt_orset, :topk_crdt}
        {ccrdt, crdt}
    end

    {_, typecc, _} = ccrdt
    {_, typec, _} = crdt

    {ccrdt_sizes, ccrdt_payloads, ccrdt_num, crdt_sizes, crdt_payloads, crdt_num} =
    Enum.map(servers, fn (s) ->
      # get average replica sizes
      {res, _} = Metrics.rpc(s, :antidote, :read_objects, [:ignore, [], [ccrdt, crdt]])
      [value_ccrdt, value_crdt] = res
      {num_ccrdt, num_crdt} = {get_num_elements(typecc, value_ccrdt), get_num_elements(typec, value_crdt)}
      {sizes_ccrdt, sizes_crdt} = {get_replica_size(typecc, value_ccrdt), get_replica_size(typec, value_crdt)}

      # get total message payloads
      {ccrdt_payload, crdt_payload} = Metrics.rpc(s, :antidote, :message_payloads, [])

      {sizes_ccrdt, ccrdt_payload, num_ccrdt, sizes_crdt, crdt_payload, num_crdt}
    end)
    |> Enum.reduce({0, 0, 0, 0, 0, 0}, fn({scc, pcc, ncc, sc, pc, nc}, {scca, pcca, ncca, sca, pca, nca}) ->
      {scca + scc, pcca + pcc, ncca + ncc, sca + sc, pca + pc, nca + nc}
    end)

    n = Enum.count(servers)
    ccrdt_sizes = ccrdt_sizes / n
    crdt_sizes = crdt_sizes / n
    ccrdt_num = ccrdt_num / n
    crdt_num = crdt_num / n

    store_metrics(op_number, {ccrdt_sizes, ccrdt_payloads, ccrdt_num, crdt_sizes, crdt_payloads, crdt_num})
  end

  defp store_metrics(op_number, {ccrdt_sizes, ccrdt_payloads, ccrdt_num, crdt_sizes, crdt_payloads, crdt_num}) do
    File.mkdir_p(@folder)

    Logger.info("Total message sizes:")
    {:ok, file} = File.open("#{@folder}payload.dat", [:append])
    line = "#{op_number}\t#{ccrdt_payloads}\t#{crdt_payloads}\n"
    IO.binwrite(file, line)
    File.close(file)
    Logger.info(line)

    Logger.info("Mean replica sizes:")
    {:ok, file} = File.open("#{@folder}size.dat", [:append])
    line = "#{op_number}\t#{ccrdt_sizes}\t#{crdt_sizes}\n"
    IO.binwrite(file, line)
    File.close(file)
    Logger.info(line)

    Logger.info("Mean # elements per replica:")
    {:ok, file} = File.open("#{@folder}nums.dat", [:append])
    line = "#{op_number}\t#{ccrdt_num}\t#{crdt_num}\n"
    IO.binwrite(file, line)
    File.close(file)
    Logger.info(line)
  end

  defp graceful_shutdown() do
    Logger.flush
    System.halt(0)
  end

end