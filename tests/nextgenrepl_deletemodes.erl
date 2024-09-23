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
%% What happens when we run AAE full-sync between clusters with different
%% delete_modes.  The answer is problematic - tombstones differ from nothing
%% (as that is the point of a tombstone), and so we can't expect two clusters
%% that have had the same operations to agree.
-module(nextgenrepl_deletemodes).
-behavior(riak_test).

-export([confirm/0]).
-export([read_from_cluster/5, length_find_tombs/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).

-define(KEY_COUNT, 10000).
-define(LOOP_COUNT, 4).

-define(SNK_WORKERS, 4).

-define(DELETE_WAIT, 8000).
%% This must be increased, otherwise tombstones may be reaped before their
%% presence can be checked in the test
-define(TOMB_PAUSE, 2).

-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

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
            {tictacaae_exchangetick, 3600 * 1000}, % don't exchange during test
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {ttaaefs_maxresults, 128},
            {tombstone_pause, ?TOMB_PAUSE},
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


repl_config(RemoteCluster1, RemoteCluster2, LocalClusterName, PeerList) ->
    ?REPL_CONFIG(LocalClusterName,
                    PeerList,
                    atom_to_list(RemoteCluster1) ++ ":any|"
                        ++ atom_to_list(RemoteCluster2) ++ ":any").


confirm() ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, immediate)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, ?DELETE_WAIT)}]),

    ?LOG_INFO("Test run using PB protocol an a mix of delete modes"),
    test_repl(pb, [ClusterA, ClusterB, ClusterC]),

    pass.


test_repl(Protocol, [ClusterA, ClusterB, ClusterC]) ->

    [NodeA1, NodeA2] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,
    [NodeC1, NodeC2] = ClusterC,

    FoldToPeerConfig =
        fun(Node, Acc) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
                ++ ":" ++ atom_to_list(Protocol)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB ++ ClusterC),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterC),
    ClusterCSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterB),

    ACfg = repl_config(cluster_b, cluster_c, cluster_a, ClusterASnkPL),
    BCfg = repl_config(cluster_a, cluster_c, cluster_b, ClusterBSnkPL),
    CCfg = repl_config(cluster_b, cluster_a, cluster_c, ClusterCSnkPL),
    rt:set_advanced_conf(NodeA1, ACfg),
    rt:set_advanced_conf(NodeA2, ACfg),
    rt:set_advanced_conf(NodeB1, BCfg),
    rt:set_advanced_conf(NodeB2, BCfg),
    rt:set_advanced_conf(NodeC1, CCfg),
    rt:set_advanced_conf(NodeC2, CCfg),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB ++ ClusterC),

    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),

    {Protocol, {NodeB1ip, NodeB1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeB1)),
    {Protocol, {NodeC1ip, NodeC1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeC1)),
    ?LOG_INFO("Following deletes, and waiting for delay - B and C equal"),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    ?LOG_INFO("A should differ from B as tombstones not empty"),
    {clock_compare, Delta1} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_b},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    ?LOG_INFO("A should differ from C as tombstones have gone after wait"),
    {clock_compare, Delta2} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    ?LOG_INFO(
        "Wait less then delete timeout to ensure replication, but presence"),
    timer:sleep(?DELETE_WAIT div 4),
    ?LOG_INFO("Now that tombstones have been re-replicated - B and C differ"),
    {clock_compare, Delta3} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    ?LOG_INFO("Delta A to B ~0p A to C ~0p and B to C ~0p",
                [Delta1, Delta2, Delta3]),
    ?LOG_INFO("Full sync will have re-replicated tombstones"),
    ?LOG_INFO("B initially should have tombstones, but A should not"),
    ?LOG_INFO("After delete wait, B and C should end up the same"),
    timer:sleep(2 * ?DELETE_WAIT),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),

    ?LOG_INFO("Find all tombstones in cluster A"),
    {ok, BKdhL} = find_tombs(NodeA1, all, all),
    ?assertMatch(?KEY_COUNT, length(BKdhL)),

    reap_from_cluster(NodeA1, 1, ?KEY_COUNT),

    rpc:call(NodeB1,
                application,
                set_env,
                [riak_kv, ttaaefs_logrepairs, true]),

    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),

    rpc:call(NodeB1,
                application,
                set_env,
                [riak_kv, ttaaefs_logrepairs, false]),

    ?LOG_INFO("Confirm no tombstones in any cluster"),
    {ok, BKdhLA} = find_tombs(NodeA1, all, all),
    {ok, BKdhLB} = find_tombs(NodeB1, all, all),
    {ok, BKdhLC} = find_tombs(NodeC1, all, all),
    ?LOG_INFO(
        "Tomb counts A=~b B=~b C=~b",
        [length(BKdhLA), length(BKdhLB), length(BKdhLC)]),
    ?assertMatch(0, length(BKdhLA)),
    ?assertMatch(0, length(BKdhLC)),
    ?assert(length(BKdhLB)< 5),
    {ok, BKdhLBr1} = find_tombs(NodeB1, all, all),
    {ok, BKdhLBr2} = find_tombs(NodeB1, all, all),
    {ok, BKdhLBr3} = find_tombs(NodeB1, all, all),
    {ok, BKdhLBr4} = find_tombs(NodeB1, all, all),
    Found = lists:usort(BKdhLBr1 ++ BKdhLBr2 ++ BKdhLBr3 ++ BKdhLBr4),
    ?LOG_INFO("Found ~b tombstones in Cluster B", [length(Found)]),
    ?LOG_INFO("Re-reaping ~0p", [Found]),
    ?LOG_INFO("Immediate delete not always successful"),
    reap_from_cluster(NodeB1, Found),
    timer:sleep(1000),
    {ok, BKdhLBretry} = find_tombs(NodeB1, all, all),
    ?assertMatch(0, length(BKdhLBretry)),

    ?LOG_INFO("As tombstones reaped A, B and C the same"),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_b},
        {NodeB1ip, NodeB1port, ?B_NVAL}),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),

    ?LOG_INFO(
        "*** Re-write and re-delete after initial tombstones reaped ***"),
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    ?LOG_INFO("Find all tombstones in cluster A"),
    {ok, BKdhL1} = find_tombs(NodeA1, all, all),
    ?assertMatch(?KEY_COUNT, length(BKdhL1)),
    reap_from_cluster(NodeA1, BKdhL1),
    timer:sleep(2 * ?DELETE_WAIT),

    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    ?LOG_INFO("As tombstones reaped A, B and C the same"),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_b},
        {NodeB1ip, NodeB1port, ?B_NVAL}),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),

    ?LOG_INFO("Confirm no tombstones in any cluster"),
    {ok, BKdhLA} = find_tombs(NodeA1, all, all),
    ?assertMatch(0, length(BKdhLA)),
    {ok, 0} = find_tombs(NodeB1, all, all, return_count),
    {ok, BKdhLC} = find_tombs(NodeC1, all, all),
    ?assertMatch(0, length(BKdhLC)),

    ?LOG_INFO(
        "*** Re-re-write and re-re-delete after initial tombstones reaped ***"),
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    reap_from_cluster(NodeA1, {job, 1}),
    ?LOG_INFO("Immediate reap count ~w after fsm managed reap",
                [length_find_tombs(NodeA1, all, all)]),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeA1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeB1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeC1, all, all], 0, 20),

    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    reap_from_cluster(NodeA1, local),
    ?LOG_INFO("Immediate reap count ~w after distributed reap",
                [length_find_tombs(NodeA1, all, all)]),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeA1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeB1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeC1, all, all], 0, 20),

    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    ?LOG_INFO("As tombstones reaped A, B and C the same"),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_b},
        {NodeB1ip, NodeB1port, ?B_NVAL}),
    root_compare(
        Protocol,
        {NodeA1, ?A_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_c},
        {NodeC1ip, NodeC1port, ?C_NVAL}),

    pass.

fullsync_check(Protocol, {SrcNode, SrcNVal, SnkCluster},
                {SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkCluster]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [Protocol, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),
    AAEResult.


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    ?LOG_INFO("Writing ~b keys to node ~0p.", [End - Start + 1, Node]),
    ?LOG_WARNING("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
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
    ?LOG_WARNING("~b errors while writing: ~0p", [length(Errors), Errors]),
    ?assertEqual([], Errors).

delete_from_cluster(Node, Start, End) ->
    ?LOG_INFO("Deleting ~b keys from node ~0p.", [End - Start + 1, Node]),
    ?LOG_WARNING("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
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

reap_from_cluster(Node, Start, End) ->
    ?LOG_INFO("Reaping ~b keys from node ~0p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
            try riak_client:reap(?TEST_BUCKET, Key, C) of
                true ->
                    Acc;
                false ->
                    [{N, false} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Aborts = lists:foldl(F, [], lists:seq(Start, End)),
    ?LOG_WARNING("~b aborts while reaping: ~0p", [length(Aborts), Aborts]),
    ?assertEqual([], Aborts).

reap_from_cluster(Node, BKdhL) when is_list(BKdhL) ->
    ?LOG_INFO("Reaping ~b found tombs from node ~0p.", [length(BKdhL), Node]),
    {ok, C} = riak:client_connect(Node),
    F =
        fun({B, K, DH}, Acc) ->
            try riak_client:reap(B, K, DH, C) of
                true ->
                    Acc;
                false ->
                    [{K, false} | Acc]
            catch
                What:Why ->
                    [{K, {What, Why}} | Acc]
            end
        end,
    Aborts = lists:foldl(F, [], BKdhL),
    ?LOG_WARNING("~b aborts while reaping: ~0p", [length(Aborts), Aborts]),
    ?assertEqual([], Aborts);
reap_from_cluster(Node, Job) ->
    ?LOG_INFO("Auto-reaping found tombs from node ~0p Job ~0p", [Node, Job]),
    {ok, C} = riak:client_connect(Node),
    Query = {reap_tombs, ?TEST_BUCKET, all, all, all, Job},
    {ok, Count} = riak_client:aae_fold(Query, C),
    ?assertEqual(?KEY_COUNT, Count).


read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    ?LOG_INFO("Reading ~b keys from node ~0p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
            case riak_client:get(?TEST_BUCKET, Key, C) of
                {ok, Obj} ->
                    ExpectedVal = <<N:32/integer, CommonValBin/binary>>,
                    case riak_object:get_values(Obj) of
                        [ExpectedVal] ->
                            Acc;
                        Siblings when length(Siblings) > 1 ->
                            ?LOG_INFO(
                                "Siblings for Key ~s:~n ~0p", [Key, Obj]),
                            [{wrong_value, Key, siblings}|Acc];
                        [UnexpectedVal] ->
                            [{wrong_value, Key, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error, Key}|Acc]
            end
        end,
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    case Errors of
        undefined ->
            ?LOG_INFO("Errors Found in read_from_cluster ~b",
                        [length(ErrorsFound)]),
            length(ErrorsFound);
        _ ->
            ?assertEqual(Errors, length(ErrorsFound))
    end.

length_find_tombs(Node, KR, MR) ->
    {ok, L} = find_tombs(Node, KR, MR, return_count),
    L.

find_tombs(Node, KR, MR) ->
    find_tombs(Node, KR, MR, return_keys).

find_tombs(Node, KR, MR, ResultType) ->
    ?LOG_INFO("Finding tombstones from node ~0p.", [Node]),
    {ok, C} = riak:client_connect(Node),
    case ResultType of
        return_keys ->
            riak_client:aae_fold({find_tombs, ?TEST_BUCKET, KR, all, MR}, C);
        return_count ->
            riak_client:aae_fold({reap_tombs, ?TEST_BUCKET, KR, all, MR, count}, C)
    end.


wait_for_outcome(Module, Func, Args, ExpOutcome, Loops) ->
    wait_for_outcome(Module, Func, Args, ExpOutcome, 0, Loops).

wait_for_outcome(Module, Func, Args, _ExpOutcome, LoopCount, LoopCount) ->
    apply(Module, Func, Args);
wait_for_outcome(Module, Func, Args, ExpOutcome, LoopCount, MaxLoops) ->
    case apply(Module, Func, Args) of
        ExpOutcome ->
            ExpOutcome;
        NotRightYet ->
            ?LOG_INFO("~0p not yet ~0p ~0p", [Func, ExpOutcome, NotRightYet]),
            timer:sleep(LoopCount * 2000),
            wait_for_outcome(Module, Func, Args, ExpOutcome,
                                LoopCount + 1, MaxLoops)
    end.

write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2) ->
    ?LOG_INFO("Write ~b objects into A and read from B and C", [?KEY_COUNT]),
    write_to_cluster(NodeA1, 1, ?KEY_COUNT, new_obj),
    ?LOG_INFO("Waiting to read sample"),
    0 =
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, ?KEY_COUNT - 31, ?KEY_COUNT,
                                ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    ?LOG_INFO("Waiting to read all"),
    0 =
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    0 =
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeC1, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),

    ?LOG_INFO("Deleting ~b objects from B and read not_found from A and C", [?KEY_COUNT]),
    delete_from_cluster(NodeB2, 1, ?KEY_COUNT),
    ?LOG_INFO("Waiting for missing sample"),
    32 =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA2, ?KEY_COUNT - 31, ?KEY_COUNT,
                            ?COMMMON_VAL_INIT, undefined],
                        32,
                        ?LOOP_COUNT),
    ?LOG_INFO("Waiting for all missing"),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA2, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                        ?KEY_COUNT,
                        ?LOOP_COUNT),
    ?LOG_INFO(
        "Waiting for delete wait before reading from delete_mode ~w cluster "
        "as otherwise read may overlap with reap and prompt a repair",
        [?DELETE_WAIT]),
    timer:sleep(?DELETE_WAIT),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeC2, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                        ?KEY_COUNT,
                        ?LOOP_COUNT),
    ?LOG_INFO("Write and delete cycle confirmed").


root_compare(
    Protocol,
    {NodeX, XNVAL, QueueName},
    {NodeY, YPort, YNVAL}) ->
    timer:sleep(?DELETE_WAIT),
    R =
        fullsync_check(
            Protocol,
            {NodeX, XNVAL, QueueName},
            {NodeY, YPort, YNVAL}),
    {root_compare, 0} =
        case R of
            {Outcome, N} when N < 10, Outcome =/= root_compare ->
                %% There is a problem here with immediate mode delete
                %% in that it can sometimes fail to clean up the odd
                %% tombstone.
                %% It was for this reason the tombstone_delay was added
                %% but amending this cannot stop an intermittent issue
                %% Workaround for the purpose of this test is to permit
                %% a small discrepancy in this case
                ?LOG_WARNING(
                    "Immediate delete issue - ~w not cleared ~w",
                    [N, Outcome]),
                timer:sleep(2 * ?DELETE_WAIT),
                root_compare(
                    Protocol,
                    {NodeX, XNVAL, QueueName},
                    {NodeY, YPort, YNVAL});
            R ->
                R
        end.
