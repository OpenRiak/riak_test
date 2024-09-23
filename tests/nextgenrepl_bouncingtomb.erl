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
%% This module implements a riak_test to prove real-time repl
%% works as expected with automated discovery of peers

-module(nextgenrepl_bouncingtomb).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 3).
-define(C_NVAL, 1).

-define(SNK_WORKERS, 12).
-define(PEER_LIMIT, 6).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).

-define(INIT_MAX_DELAY, 10).
-define(STND_MAX_DELAY, 3600).
-define(DELETE_TIMEOUT, 1000).
-define(BIG_REPL_SLEEP, ?DELETE_TIMEOUT + 200).

-define(LOOP_COUNT, 8).
-define(STATS_WAIT, 500).


-define(CORE_CONFIG(RingSize, NVal),[
        {riak_core,
            [
                {ring_creation_size, RingSize},
                {default_bucket_props,
                    [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                    ]}
            ]
        }]).

-define(AAE_CONFIG,
        [
            {anti_entropy, {off, []}},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
            {tictacaae_storeheads, true},
                % Necessary to count tombstones using reap_tombs query
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 15000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {tictacaae_suspend, true}
        ]).

-define(CONFIG(RingSize, NVal, Q, DeleteMode), 
        ?CORE_CONFIG(RingSize, NVal) ++
        [{riak_kv,
            ?AAE_CONFIG ++
            [
                {ttaaefs_maxresults, 1024},
                {ttaaefs_queuename, Q},
                {replrtq_enablesrc, true},
                {replrtq_srcqueue, atom_to_list(Q) ++ ":any"},
                {delete_mode, DeleteMode}
            ]
        }]).

-define(SNK_CONFIG(RingSize, Nval, Q, ClusterName, IP, P, DeleteMode), 
        ?CORE_CONFIG(RingSize, Nval) ++
        [{riak_kv,
            ?AAE_CONFIG ++
            [
                {ttaaefs_maxresults, 1024},
                {ttaaefs_queuename, Q},
                {replrtq_enablesrc, true},
                {replrtq_srcqueue, atom_to_list(Q) ++ ":any"},
                {delete_mode, DeleteMode},
                {replrtq_enablesink, true},
                {replrtq_prompt_max_seconds, ?INIT_MAX_DELAY},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, IP ++ ":" ++ integer_to_list(P) ++ ":pb"},
                {replrtq_sinkworkers, ?SNK_WORKERS},
                {replrtq_sinkpeerlimit, ?PEER_LIMIT},
                {replrtq_peer_discovery, true},
                {ttaaefs_scope, all},
                {ttaaefs_peerip, IP},
                {ttaaefs_peerport, P},
                {ttaaefs_peerprotocol, pb},
                {ttaaefs_allcheck, 0},
                {ttaaefs_nocheck, 24}
            ]}
        ]).

confirm() ->

    ?LOG_INFO("***************"),
    ?LOG_INFO("Testing rotating tombs with key insomnia"),
    ?LOG_INFO("***************"),
    
    [ClusterA1, ClusterB1] =
        rt:deploy_clusters([
            {3, ?CONFIG(?A_RING, ?A_NVAL, cluster_b, ?DELETE_TIMEOUT)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, cluster_a, keep)}]),

    pass = with_insomnia_test(ClusterA1, ClusterB1),
    
    rt:clean_cluster(ClusterA1),
    rt:clean_cluster(ClusterB1),

    ?LOG_INFO("***************"),
    ?LOG_INFO("Testing rotating tombs without key insomnia"),
    ?LOG_INFO("***************"),

    [ClusterA2, ClusterB2, ClusterC2] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, cluster_b, keep)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, cluster_a, ?DELETE_TIMEOUT)},
            {1, ?CONFIG(?C_RING, ?C_NVAL, q1_ttaaefs, keep)}]),
    
    no_insomnia_test(ClusterA2, ClusterB2, ClusterC2).

no_insomnia_test(ClusterA, ClusterB, ClusterC) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),

    [NodeA1, NodeA2] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,
    [NodeC1] = ClusterC,

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {IP, Port}
        end,
    
    reset_peer_config(
        NodeA1, cluster_a, ?A_RING, ?A_NVAL, cluster_b,
        PeerConfigFun(NodeB1), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeA2, cluster_a, ?A_RING, ?A_NVAL, cluster_b,
        PeerConfigFun(NodeB2), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA1), keep),
    reset_peer_config(
        NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA2), keep),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    ?LOG_INFO("Confirm riak_kv is up on all nodes."),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    ?LOG_INFO("Wait for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),

    ?LOG_INFO("Ready for test - writing keys"),
    write_to_cluster(NodeC1, 1, 1000, new_obj),

    ?LOG_INFO("Manual push of keys to A and B"),
    {IPc, Portc} = PeerConfigFun(NodeC1),
    {ok, SrcC} = riakc_pb_socket:start(IPc, Portc),
    {ok, SnkA} = riak:client_connect(NodeA1),
    {ok, SnkB} = riak:client_connect(NodeB1),
    ?assertMatch(1000, drain_queue(SrcC, SnkA)),

    ?LOG_INFO("Ready for test - deleting keys"),
    delete_from_cluster(NodeC1, 1, 1000),

    ?assertMatch(1000, drain_queue(SrcC, SnkB)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),

    riakc_pb_socket:stop(SrcC),

    ?LOG_INFO("Resetting delete_mode on Cluster B"),
    reset_peer_config(
        NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a, 
        PeerConfigFun(NodeA1), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA2), ?DELETE_TIMEOUT),

    GetStatsFun =
        fun() ->
            ?LOG_INFO("Cluster A stats"),
            get_stats(NodeA1),
            get_stats(NodeA2),
            ?LOG_INFO("Cluster B stats"),
            get_stats(NodeB1),
            get_stats(NodeB2)
        end,
    
    ?LOG_INFO("Cluster B has tombstones, but Cluster A has objects"),
    ?LOG_INFO("Enabling AAE to avoid inconsistent results on coverage"),
    rpc:multicall(ClusterA, riak_client, tictacaae_resume_node, []),
    rpc:multicall(ClusterB, riak_client, tictacaae_resume_node, []),
    {_InitACount, _InitBCount} = log_fun(NodeA1, NodeB1),
    
    GetStatsFun(),

    rotating_full_sync(
        NodeA1, NodeB1, GetStatsFun, fun log_fun/2, ?LOOP_COUNT),

    ?assertMatch({0, 0}, log_fun(NodeA1, NodeB1)),

    pass.

log_fun(NodeA, NodeB) ->
    {ok, A1C} =
        rpc:call(NodeA,
            riak_client,
            aae_fold,
            [{find_tombs, ?TEST_BUCKET, all, all, all}]),
    ?LOG_INFO("Cluster A ~w tombs", [length(A1C)]),

    {ok, B1C} =
        rpc:call(NodeB,
            riak_client,
            aae_fold,
            [{find_tombs, ?TEST_BUCKET, all, all, all}]),
    ?LOG_INFO("Cluster B ~w tombs", [length(B1C)]),

    case {length(A1C), length(B1C)} of
        {0, 0} ->
            ok;
        {N, M} when N < 10, M < 10 ->
            ?LOG_INFO("Cluster A tombs ~0p", [A1C]),
            ?LOG_INFO("Cluster B tombs ~0p", [B1C]);
        _ ->
            ok
    end,

    {length(A1C), length(B1C)}.



with_insomnia_test(ClusterA, ClusterB) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),

    [NodeA1, NodeA2, NodeA3] = ClusterA,
    [NodeB1, NodeB2, NodeB3] = ClusterB,

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {IP, Port}
        end,
    
    reset_peer_config(
        NodeA1, cluster_a, ?A_RING, ?A_NVAL, cluster_b,
        PeerConfigFun(NodeB1), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeA2, cluster_a, ?A_RING, ?A_NVAL, cluster_b,
        PeerConfigFun(NodeB2), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeA3, cluster_a, ?A_RING, ?A_NVAL, cluster_b,
        PeerConfigFun(NodeB3), ?DELETE_TIMEOUT),
    reset_peer_config(
        NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA1), keep),
    reset_peer_config(
        NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA2), keep),
    reset_peer_config(
        NodeB3, cluster_b, ?B_RING, ?B_NVAL, cluster_a,
        PeerConfigFun(NodeA3), keep),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    ?LOG_INFO("Confirm riak_kv is up on all nodes."),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB),

    ?LOG_INFO("Wait for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),

    ?LOG_INFO("Ready for test"),
    write_to_cluster(NodeA1, 1, 1000, new_obj),
    rt:wait_until(count_keys_fun(NodeA1, 1000, erase_keys)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, erase_keys)),
    ?LOG_INFO("Deleting objects from other cluster"),
    delete_from_cluster(NodeB1, 1, 1000),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),
    rt:wait_until(count_keys_fun(NodeA1, 0, reap_tombs)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),

    ?LOG_INFO("Switching delete mode from keep on Cluster B"),
    reset_peer_config(NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA1), 1000),
    reset_peer_config(NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA2), 1000),
    reset_peer_config(NodeB3, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA3), 1000),

    GetStatsFun =
        fun() ->
            ?LOG_INFO("Cluster A stats"),
            get_stats(NodeA1),
            get_stats(NodeA2),
            get_stats(NodeA3),
            ?LOG_INFO("Cluster B stats"),
            get_stats(NodeB1),
            get_stats(NodeB2),
            get_stats(NodeB3)
        end,

    ?LOG_INFO("Cluster B has tombstones, but Cluster A should have reaped"),
    ?LOG_INFO("Enabling AAE to avoid inconsistent results on coverage"),
    rpc:multicall(ClusterA, riak_client, tictacaae_resume_node, []),
    rpc:multicall(ClusterB, riak_client, tictacaae_resume_node, []),
    {_InitACount, _InitBCount} = log_fun(NodeA1, NodeB1),
    
    GetStatsFun(),

    rotating_full_sync(
        NodeA1, NodeB1, GetStatsFun, fun log_fun/2, ?LOOP_COUNT),

    ?assertMatch({0, 0}, log_fun(NodeA1, NodeB1)),

    pass.

rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, 0) ->
    LoopLogFun = 
        fun(X) ->
            ?LOG_INFO("Closing count loop ~w", [X]),
            _ = LogFun(NodeA, NodeB)
        end,
    GetStatsFun(),
    lists:foreach(LoopLogFun, lists:seq(1, ?LOOP_COUNT)),
    GetStatsFun();
rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, Rotations) ->
    ?LOG_INFO("Full sync from Cluster B - loops to go ~w", [Rotations]),
    rpc:call(NodeB, riak_client, ttaaefs_fullsync, [all_check, 60]),
    timer:sleep(?BIG_REPL_SLEEP),
    _ = LogFun(NodeA, NodeB),
    ?LOG_INFO("Full sync from Cluster A - loops to go ~w", [Rotations]),
    rpc:call(NodeA, riak_client, ttaaefs_fullsync, [all_check, 60]),
    timer:sleep(?BIG_REPL_SLEEP),
    _ = LogFun(NodeA, NodeB),
    GetStatsFun(),
    rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, Rotations - 1).

reset_peer_config(Node, ClusterName, RS, NV, Q, PeerX, DeleteMode) ->
    {IP, Port} = PeerX,
    ClusterSNkCfg = ?SNK_CONFIG(RS, NV, Q, ClusterName, IP, Port, DeleteMode),
    rt:set_advanced_conf(Node, ClusterSNkCfg),
    rt:wait_for_service(Node, [riak_kv]).

count_keys_fun(N, ExpectedC, Type) ->
    fun() ->
        {ok, C} =
            rpc:call(N,
                riak_client,
                aae_fold,
                [{Type, ?TEST_BUCKET, all, all, all, count}]),
        ?LOG_INFO("Count of ~w on ~w for ~w", [C, N, Type]),
        ExpectedC == C
    end.

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    ?LOG_INFO("Writing ~0p keys to node ~0p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = 
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riak_object:new(
                            ?TEST_BUCKET,
                            Key,
                            <<N:32/integer, CVB/binary>>
                        )
                end,
            try riak_client:put(Obj, C) of
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
    ?LOG_WARNING("~w errors while writing: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).

delete_from_cluster(Node, Start, End) ->
    ?LOG_INFO("Deleting ~0p keys from node ~0p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try riak_client:delete(?TEST_BUCKET, Key, C) of
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
    ?LOG_WARNING("~w errors while deleting: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).


drain_queue(SrcC, SnkA) ->
    drain_queue(SrcC, SnkA, 0).

drain_queue(SrcC, SnkA, N) ->
    case riakc_pb_socket:fetch(SrcC, <<"q1_ttaaefs">>) of
        {ok, queue_empty} ->
            N;
        {ok, {deleted, _TombClock, RObj}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkA),
            drain_queue(SrcC, SnkA, N + 1);
        {ok, RObj} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkA),
            drain_queue(SrcC, SnkA, N + 1)
    end.

get_stats(Node) ->
    S = rt:get_stats(Node, ?STATS_WAIT),
    {<<"ngrfetch_prefetch_total">>, PFT} =
        lists:keyfind(<<"ngrfetch_prefetch_total">>, 1, S),
    {<<"ngrfetch_tofetch_total">>, TFT} =
        lists:keyfind(<<"ngrfetch_tofetch_total">>, 1, S),
    {<<"ngrfetch_nofetch_total">>, NFT} =
        lists:keyfind(<<"ngrfetch_nofetch_total">>, 1, S),
    {<<"ngrrepl_object_total">>, FOT} =
        lists:keyfind(<<"ngrrepl_object_total">>, 1, S),
    {<<"ngrrepl_error_total">>, FErT} =
        lists:keyfind(<<"ngrrepl_error_total">>, 1, S),
    {<<"ngrrepl_empty_total">>, FEmT} =
        lists:keyfind(<<"ngrrepl_empty_total">>, 1, S),
    {<<"read_repairs_total">>, RRT} =
        lists:keyfind(<<"read_repairs_total">>, 1, S),
    ?LOG_INFO(
        "Stats for Node ~w, PFT=~w TFT=~w NFT=~w FOT=~w FErT=~w FEmT=~w RRT=~w",
        [Node, PFT, TFT, NFT, FOT, FErT, FEmT, RRT]).