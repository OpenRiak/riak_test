%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc
%% Test of nextgenrepl where the writes are conditional PUTs

-module(nextgenrepl_rtq_condput).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 2).
-define(C_NVAL, 3).

-define(SNK_WORKERS, 4).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(REPL_SLEEP, 2048).
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
-define(NGR_INIT_TIMEOUT, 10000).

-define(STATS_WAIT, 1000).

-define(CONFIG(RingSize, NVal, SrcQueueDefns, TokenMode), [
    {riak_core, [
        {ring_creation_size, RingSize},
        {handoff_concurrency,       max(8, RingSize div 16)},
        {forced_ownership_handoff,  max(8, RingSize div 16)},
        {vnode_inactivity_timeout,  4000},
        {vnode_management_timer,    4000},
        {default_bucket_props, [
            {n_val, NVal},
            {allow_mult, true},
            {dvv_enabled, true}
        ]}
    ]},
    {riak_kv, [
        {anti_entropy, {off, []}},
        {tictacaae_active, active},
        {tictacaae_parallelstore, leveled_ko},
        % if backend not leveled will use parallel key-ordered store
        {tictacaae_rebuildwait, 4},
        {tictacaae_rebuilddelay, 3600},
        {tictacaae_exchangetick, 120 * 1000},
        {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
        {ttaaefs_maxresults, 128},
        {delete_mode, keep},
        {replrtq_enablesrc, true},
        {replrtq_srcqueue, SrcQueueDefns},
        {ngr_initial_timeout, ?NGR_INIT_TIMEOUT},
        {conditional_put_mode, prefer_token},
        {token_request_mode, TokenMode}
    ]}
]).

-define(SNK_CONFIG(ClusterName, PeerList), [
    {riak_kv, [
        {replrtq_enablesink, true},
        {replrtq_sinkqueue, ClusterName},
        {replrtq_sinkpeers, PeerList},
        {replrtq_sinkworkers, ?SNK_WORKERS}
    ]}
]).

confirm() ->
    ASrcQ = "cluster_b:any|cluster_c:any",
    BSrcQ = "cluster_a:any|cluster_c:any",
    CSrcQ = "cluster_b:any|cluster_a:any",

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters(
            [
                {2, ?CONFIG(?A_RING, ?A_NVAL, ASrcQ, basic_consensus)},
                {2, ?CONFIG(?B_RING, ?B_NVAL, BSrcQ, basic_consensus)},
                {2, ?CONFIG(?C_RING, ?C_NVAL, CSrcQ, basic_consensus)}
            ]
        ),

    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfigHTTP =
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":http"
        end,
    reset_peer_config(FoldToPeerConfigHTTP, ClusterA, ClusterB, ClusterC),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB ++ ClusterC
    ),
    rt:wait_until_transfers_complete(ClusterA),
    rt:wait_until_transfers_complete(ClusterB),
    rt:wait_until_transfers_complete(ClusterC),

    timer:sleep(?NGR_INIT_TIMEOUT),

    ?LOG_INFO("Ready for test - with http client for rtq."),
    HTTPClientFun = fun(Node) -> {rhc, rt:httpc(Node)} end,
    pass =
        test_rtqrepl_between_clusters(
            ClusterA,
            ClusterB,
            ClusterC,
            HTTPClientFun
        ),

    ?LOG_INFO("Discover Peer IP/ports for pb and restart with peer config"),
    FoldToPeerConfigPB =
        fun(Node, Acc) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":" ++ "pb"
        end,

    rt:clean_cluster(ClusterA),
    rt:clean_cluster(ClusterB),
    rt:clean_cluster(ClusterC),

    [ClusterApb, ClusterBpb, ClusterCpb] =
        rt:deploy_clusters(
            [
                {2, ?CONFIG(?A_RING, ?A_NVAL, ASrcQ, head_only)},
                {2, ?CONFIG(?B_RING, ?B_NVAL, BSrcQ, head_only)},
                {2, ?CONFIG(?C_RING, ?C_NVAL, CSrcQ, head_only)}
            ]
        ),
    reset_peer_config(FoldToPeerConfigPB, ClusterApb, ClusterBpb, ClusterCpb),
    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterApb),
    rt:wait_until_ring_converged(ClusterBpb),
    rt:wait_until_ring_converged(ClusterCpb),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterApb ++ ClusterBpb ++ ClusterCpb
    ),
    rt:wait_until_transfers_complete(ClusterApb),
    rt:wait_until_transfers_complete(ClusterBpb),
    rt:wait_until_transfers_complete(ClusterCpb),

    timer:sleep(?NGR_INIT_TIMEOUT),

    ?LOG_INFO("Ready for test - with protocol buffers client for rtq."),
    PBClientFun = fun(Node) -> {riakc_pb_socket, rt:pbc(Node)} end,
    pass =
        test_rtqrepl_between_clusters(
            ClusterApb,
            ClusterBpb,
            ClusterCpb,
            PBClientFun
        ),
    pass.


reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB, ClusterC) ->
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB ++ ClusterC),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterC),
    ClusterCSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterB),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    ClusterCSNkCfg = ?SNK_CONFIG(cluster_c, ClusterCSnkPL),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end,
                    ClusterA),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end,
                    ClusterB),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterCSNkCfg) end,
                    ClusterC),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC).


test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC, ClientFun) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    ?LOG_INFO("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    ?LOG_INFO(
        "Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
        [IPA, PortA, IPB, PortB, IPC, PortC]
    ),

    true =
        check_all_insync(
            {NodeA, IPA, PortA},
            {NodeB, IPB, PortB},
            {NodeC, IPC, PortC}
        ),

    ?LOG_INFO("Test 1000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do have them.
    write_to_cluster(ClientFun(NodeA), 1, 1000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, 0),
    true =
        check_all_insync(
            {NodeA, IPA, PortA},
            {NodeB, IPB, PortB},
            {NodeC, IPC, PortC}
        ),

    ?LOG_INFO("Test replicating modified objects"),
    write_to_cluster(ClientFun(NodeB), 1, 100, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeC, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeA, 101, 1000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 101, 1000, ?COMMMON_VAL_INIT, 0),
    true =
        check_all_insync(
            {NodeA, IPA, PortA},
            {NodeB, IPB, PortB},
            {NodeC, IPC, PortC}
        ),
    pass.


check_all_insync(
    {NodeA, IPA, PortA},
    {NodeB, IPB, PortB},
    {NodeC, IPC, PortC}
) ->
    {root_compare, 0} = 
        fullsync_check(
            {NodeA, IPA, PortA, ?A_NVAL},
            {NodeB, IPB, PortB, ?B_NVAL},
            cluster_a
        ),
    {root_compare, 0} =
        fullsync_check(
            {NodeB, IPB, PortB, ?B_NVAL},
            {NodeC, IPC, PortC, ?C_NVAL},
            cluster_c
        ),
    {root_compare, 0} =
        fullsync_check(
            {NodeC, IPC, PortC, ?C_NVAL},
            {NodeA, IPA, PortA, ?A_NVAL},
            cluster_a
        ),
    true.

fullsync_check(
    {SrcNode, _SrcIP, _SrcPort, SrcNVal},
    {_SinkNode, SinkIP, SinkPort, SinkNVal},
    SnkClusterName
) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = erpc:call(SrcNode, ModRef, pause, []),
    ok = erpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = erpc:call(SrcNode, ModRef, set_queuename, [SnkClusterName]),
    ok = erpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    erpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster({ClientMod, Client}, Start, End, CommonValBin) ->
    ?LOG_INFO("Writing ~b keys with client ~0p.", [End - Start + 1, Client]),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            {Obj, Opts} =
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        {
                            riakc_obj:new(
                                ?TEST_BUCKET,
                                Key,
                                <<N:32/integer, CVB/binary>>
                            ),
                            [if_none_match]
                        };
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} =
                            ClientMod:get(Client, ?TEST_BUCKET, Key),
                        {
                            riakc_obj:update_value(PrevObj, UPDV),
                            [if_not_modified]
                        }
                end,
            try ClientMod:put(Client, Obj, Opts) of
                ok ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Errors = lists:foldl(F, [], lists:seq(Start, End)),
    ?LOG_WARNING("~b errors while writing: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    read_from_cluster(Node, Start, End, CommonValBin, Errors, false).

read_from_cluster(Node, Start, End, CommonValBin, Errors, _LogErrors) ->
    ?LOG_INFO("Reading ~b keys from node ~0p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  riak_client:get(?TEST_BUCKET, Key, C) of
                {ok, Obj} ->
                    ExpectedVal = <<N:32/integer, CommonValBin/binary>>,
                    case riak_object:get_value(Obj) of
                        ExpectedVal ->
                            Acc;
                        UnexpectedVal ->
                            [{wrong_value, Key, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error, Key}|Acc]
            end
        end,
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    case length(ErrorsFound) of
        L when L > 0 ->
            ?LOG_INFO("First read error ~0p", [hd(ErrorsFound)]);
        0 ->
            ok
    end,

    ?assertEqual(Errors, length(ErrorsFound)).
