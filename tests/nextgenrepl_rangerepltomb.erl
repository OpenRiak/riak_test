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
%% Will the range_repl command replicate tombstones

-module(nextgenrepl_rangerepltomb).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).

-define(SNK_WORKERS, 4).

-define(DELETE_WAIT, 5000).

-define(COMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(CONFIG(RingSize, NVal, DeleteMode), [
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
        },
        {riak_kv,
          [
            {anti_entropy, {off, []}},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
            {tictacaae_storeheads, true},
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 120 * 1000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {ttaaefs_maxresults, 128},
            {delete_mode, DeleteMode}
          ]}
        ]).

-define(REPL_CONFIG(LocalClusterName, PeerList, SrcQueueDefns), [
    {riak_kv,
        [
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_enablesink, true},
            {replrtq_enablesrc, true},
            {replrtq_sinkqueue, LocalClusterName},
            {replrtq_sinkpeers, PeerList},
            {replrtq_sinkworkers, ?SNK_WORKERS}
        ]}
]).


repl_config(RemoteCluster1, LocalClusterName, PeerList) ->
    ?REPL_CONFIG(LocalClusterName,
                    PeerList,
                    atom_to_list(RemoteCluster1) ++ ":any").


confirm() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {1, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, keep)}]),

    ?LOG_INFO("Test run using PB protocol an a mix of delete modes"),
    test_range_repl(pb, [ClusterA, ClusterB]),

    pass.


test_range_repl(Protocol, [ClusterA, ClusterB]) ->

    [NodeA1] = ClusterA,
    [NodeB1, NodeB2, NodeB3] = ClusterB,

    FoldToPeerConfig =
        fun(Node, Acc) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
                ++ ":" ++ atom_to_list(Protocol)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),

    ACfg = repl_config(cluster_b, cluster_a, ClusterASnkPL),
    BCfg = repl_config(cluster_a, cluster_b, ClusterBSnkPL),
    rt:set_advanced_conf(NodeA1, ACfg),
    rt:set_advanced_conf(NodeB1, BCfg),
    rt:set_advanced_conf(NodeB2, BCfg),
    rt:set_advanced_conf(NodeB3, BCfg),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB),

    write_to_cluster(NodeA1, 1, 1000, new_obj),

    rpc:call(NodeA1, riak_kv_replrtq_src, suspend_rtq, [cluster_b]),

    delete_from_cluster(NodeA1, 901, 1000),

    {ok, CA1} = riak:client_connect(NodeA1),
    {ok, CB1} = riak:client_connect(NodeB1),

    {ok, TLA1} =
        riak_client:aae_fold({find_tombs, ?TEST_BUCKET, all, all, all}, CA1),
    {ok, TLB1} =
        riak_client:aae_fold({find_tombs, ?TEST_BUCKET, all, all, all}, CB1),

    ?assertMatch(100, length(TLA1)),
    ?assertMatch(0, length(TLB1)),

    rpc:call(NodeA1, riak_kv_replrtq_src, resume_rtq, [cluster_b]),

    {ok, ReplResult} =
        riak_client:aae_fold({repl_keys_range, ?TEST_BUCKET, all, all, cluster_b}, CA1),
    ?LOG_INFO("ReplResult ~0p", [ReplResult]),

    QueueEmpty =
        fun() ->
            QueueLength =
                rpc:call(NodeA1, riak_kv_replrtq_src, length_rtq, [cluster_b]),
            ?LOG_INFO("Queue length ~w", [QueueLength]),
            QueueLength == {cluster_b, {0, 0, 0}}
        end,
    rt:wait_until(QueueEmpty),
    timer:sleep(1000),

    {ok, TLB2} =
        riak_client:aae_fold({find_tombs, ?TEST_BUCKET, all, all, all}, CB1),
    ?assertMatch(100, length(TLB2)),

    pass.

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    ?LOG_INFO("Writing ~b keys to node ~0p.", [End - Start + 1, Node]),
    ?LOG_WARNING("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj =
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMON_VAL_INIT,
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
    ?LOG_WARNING("~b errors while writing: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).

delete_from_cluster(Node, Start, End) ->
    ?LOG_INFO("Deleting ~b keys from node ~0p.", [End - Start + 1, Node]),
    ?LOG_WARNING("Note that only utf-8 keys are used"),
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
    ?LOG_WARNING("~b errors while deleting: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).
