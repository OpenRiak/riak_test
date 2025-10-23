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
%% This module implements a riak_test to prove that we cna include and
%% exclude typed buckets from both real-time repl and from full-sync
-module(nextgenrepl_tree_exclude).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl-aae-systest">>).
-define(REPL_TYPE, <<"repl">>).
-define(LOCAL_TYPE, <<"no_repl">>).
-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(SNK_WORKERS, 4).
-define(PEER_LIMIT, 2).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(INIT_MIN_DELAY, 30).
-define(INIT_MAX_DELAY, 60).

-define(STND_MIN_DELAY, 120).
-define(STND_MAX_DELAY, 3600).

-define(REPL_SLEEP, 2048).
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
-define(INITIAL_TIMEOUT, 10000).

-define(KEYCOUNT_PERCYCLE, 10000).
-define(MAX_RESULTS, 128).

-define(CONFIG(RingSize, NVal, SrcQueueDefns), [
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
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 120 * 1000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {ttaaefs_maxresults, ?MAX_RESULTS},
            {delete_mode, keep},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_peer_discovery, true},
            {ngr_initial_timeout, ?INITIAL_TIMEOUT},
            {ttaaefs_logrepairs, false}
          ]}
        ]).

-define(SNK_CONFIG(ClusterName, PeerList),
        [{riak_kv,
            [{replrtq_enablesink, true},
                {replrtq_prompt_min_seconds, ?INIT_MIN_DELAY},
                {replrtq_prompt_max_seconds, ?INIT_MAX_DELAY},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, PeerList},
                {replrtq_sinkworkers, ?SNK_WORKERS},
                {replrtq_sinkpeerlimit, ?PEER_LIMIT}]}]).

confirm() ->
    ClusterASrcQ =
        lists:flatten(
            io_lib:format(
                "cluster_b:buckettype.~s|ttaaefs:block_rtq",
                [?REPL_TYPE]
            )
        ),
    ClusterBSrcQ =
        lists:flatten(
            io_lib:format(
                "cluster_a:buckettype.~s|ttaaefs:block_rtq",
            [?REPL_TYPE]
        )
    ),

    ?LOG_INFO("Running tests with pb based peer relationships"),

    [ClusterA1, ClusterB1] =
        rt:deploy_clusters(
            [
                {4, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ)},
                {2, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ)}
            ]
        ),

    pass = cluster_test(ClusterA1, ClusterB1, pb).

cluster_test(ClusterA, ClusterB, Protocol) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),

    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    lists:foreach(compare_peer_info(4, Protocol), ClusterA),
    lists:foreach(compare_peer_info(2, Protocol), ClusterB),

    [NodeA|_RestA] = ClusterA,
    [NodeB|_RestB] = ClusterB,

    PeerConfigFun =
        fun(Node) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            IP ++ ":" ++ integer_to_list(Port) ++ ":" ++ atom_to_list(Protocol)
        end,

    PeerA = PeerConfigFun(NodeA),
    PeerB = PeerConfigFun(NodeB),

    reset_peer_config(ClusterA, cluster_a, ttaaefs, PeerB),
    reset_peer_config(ClusterB, cluster_b, ttaaefs, PeerA),
    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    ?LOG_INFO("Confirm riak_kv is up on all nodes."),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB
    ),
    rt:create_activate_and_wait_for_bucket_type(
        ClusterA,
        ?REPL_TYPE,
        [{n_val, ?A_NVAL}]
    ),
    rt:create_activate_and_wait_for_bucket_type(
        ClusterB,
        ?REPL_TYPE,
        [{n_val, ?B_NVAL}]
    ),
    rt:create_activate_and_wait_for_bucket_type(
        ClusterA,
        ?LOCAL_TYPE,
        [{aae_tree_exclude, true}, {n_val, ?A_NVAL}]
    ),
    rt:create_activate_and_wait_for_bucket_type(
        ClusterB,
        ?LOCAL_TYPE,
        [{aae_tree_exclude, true}, {n_val, ?B_NVAL}]
    ),

    ?LOG_INFO("Wait for initial timeout on nextgenrepl services"),
    timer:sleep(?INITIAL_TIMEOUT),

    ?LOG_INFO("Ready for test - with ~w client for rtq.", [Protocol]),
    pass = test_rtqrepl(ClusterA, ClusterB),
    StatsA1 = get_stats(ClusterA),
    StatsB1 = get_stats(ClusterB),
    ?LOG_INFO("ClusterA stats ~w", [StatsA1]),
    ?LOG_INFO("ClusterB stats ~w", [StatsB1]),

    pass.

compare_peer_info(ExpectedPeers, Protocol) ->
    fun(Node) ->
        {Protocol, {IP, Port}} =
            lists:keyfind(Protocol, 1, rt:connection_info(Node)),
        MemberList =
            lists:map(
                fun({IPm, Portm}) ->
                    {list_to_binary(IPm), Portm}
                end,
                erpc:call(Node, riak_client, membership_request, [Protocol])),
        {Mod, RiakErlC} =
            case Protocol of
                pb ->
                    {ok, Pid} = riakc_pb_socket:start(IP, Port),
                    {riakc_pb_socket, Pid}
            end,
        {ok, MemberList} = Mod:peer_discovery(RiakErlC),
        ?LOG_INFO(
            "Discovered Member list (two ways) ~0p ~0p "
            "expecting ~w peers for node ~w",
            [MemberList, Protocol, ExpectedPeers, Node]),
        ?assert(lists:member({list_to_binary(IP), Port}, MemberList)),
        ?assertMatch(ExpectedPeers, length(MemberList)),
        case Protocol of
            pb ->
                Mod:stop(RiakErlC)
        end
    end.

reset_peer_config(SnkCluster, ClusterName, FullSyncQueue, PeerX) ->
    [HeadNode|Rest] = SnkCluster,
    rt:set_advanced_conf(HeadNode, ?SNK_CONFIG(FullSyncQueue, PeerX)),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ?SNK_CONFIG(ClusterName, PeerX)) end,
        Rest
    ).


test_rtqrepl(ClusterA, ClusterB) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),

    ?LOG_INFO("Sleep for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),
    set_max_delay(
        ClusterA ++ ClusterB,
        {?STND_MIN_DELAY, ?STND_MAX_DELAY}
    ),
    ?LOG_INFO("Sleep for next scheduled peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),

    ?LOG_INFO("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    ?LOG_INFO("Cluster A ~s ~w Cluster B ~s ~w", [IPA, PortA, IPB, PortB]),

    true = check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}),

    ?LOG_INFO(
        "Test loading ~w keys that should replicate",
        [?KEYCOUNT_PERCYCLE]
    ),
    % Write keys to cluster A, verify B has them.
    write_to_cluster(NodeA, ?REPL_TYPE, 1, ?KEYCOUNT_PERCYCLE, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(
        NodeA, ?REPL_TYPE, 1, ?KEYCOUNT_PERCYCLE, ?COMMMON_VAL_INIT, 0
    ),
    read_from_cluster(
        NodeB, ?REPL_TYPE, 1, ?KEYCOUNT_PERCYCLE, ?COMMMON_VAL_INIT, 0
    ),
    true = check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}),

    ?LOG_INFO("Test replicating 100 tombstones"),
    true = 100 < ?KEYCOUNT_PERCYCLE,
    TombStart = (?KEYCOUNT_PERCYCLE - 100) + 1,
    delete_from_cluster(NodeA, ?REPL_TYPE,  TombStart, ?KEYCOUNT_PERCYCLE),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(
        NodeA, ?REPL_TYPE, TombStart, ?KEYCOUNT_PERCYCLE, ?COMMMON_VAL_INIT, 100
    ),
    read_from_cluster(
        NodeB, ?REPL_TYPE, TombStart, ?KEYCOUNT_PERCYCLE, ?COMMMON_VAL_INIT, 100
    ),
    true =
        check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}),

    ?LOG_INFO("Test replicating modified objects"),
    write_to_cluster(NodeB, ?REPL_TYPE, 1, 100, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, ?REPL_TYPE, 1, 100, ?COMMMON_VAL_MOD, 0),
    true =
        check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}),

    ?LOG_INFO("Check peers stable"),
    ?assertNot(check_peers_stable(NodeA, cluster_a)),
    ?assertNot(check_peers_stable(NodeB, cluster_b)),

    NonReplKeyCount = ?KEYCOUNT_PERCYCLE div 4,
        % per-bucket repair is time-consuming

    ?LOG_INFO(
        "Test loading ~w keys that should replicate",
        [NonReplKeyCount]
    ),
    % Write keys to cluster A, verify B has them.
    write_to_cluster(NodeA, ?LOCAL_TYPE, 1, NonReplKeyCount, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(
        NodeA, ?LOCAL_TYPE, 1, NonReplKeyCount, ?COMMMON_VAL_INIT, 0
    ),
    read_from_cluster(
        NodeB,
        ?LOCAL_TYPE,
        1,
        NonReplKeyCount,
        ?COMMMON_VAL_INIT,
        NonReplKeyCount
    ),
    ?LOG_INFO(
        "All should appear in sync, even though replication has not occurred"
    ),
    true = check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}),

    BucketSyncFun =
        fun() ->
            perbucketsync_check(
                    {NodeA, IPA, PortA},
                    {NodeB, IPB, PortB},
                    [{?LOCAL_TYPE, ?TEST_BUCKET}],
                    ttaaefs
                )
        end,

    {clock_compare, CCN} = BucketSyncFun(),
    ?LOG_INFO("Per bucket sync resolved ~w issues", [CCN]),

    lists:foreach(
        fun(_I) ->
            R = BucketSyncFun(),                
            ?LOG_INFO("Latest bucket sync result ~0p", [R]),
            timer:sleep(100)
        end,
        lists:seq(1, NonReplKeyCount div ?MAX_RESULTS)
    ),
    rt:wait_until(
        fun() ->
            R = BucketSyncFun(),
            ?LOG_INFO("Latest bucket sync result ~0p", [R]),
            R == {tree_compare, 0}
        end,
        10,
        100
    ),

    {tree_compare, 0} = BucketSyncFun(),

    pass.

perbucketsync_check({SrcNode, _, _}, {_, SinkIP, SinkPort}, BucketL, Queue) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = erpc:call(SrcNode, ModRef, pause, []),
    ok = erpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = erpc:call(SrcNode, ModRef, set_queuename, [Queue]),
    ok = erpc:call(SrcNode, ModRef, set_bucketsync, [BucketL]),
    erpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]).

check_all_insync({NodeA, IPA, PortA}, {NodeB, IPB, PortB}) ->
    {root_compare, 0}
        = fullsync_check(
            {NodeA, IPA, PortA, ?A_NVAL},
            {NodeB, IPB, PortB, ?B_NVAL},
            ttaaefs
        ),
    {root_compare, 0}
        = fullsync_check(
            {NodeB, IPB, PortB, ?B_NVAL},
            {NodeA, IPA, PortA, ?A_NVAL},
            ttaaefs
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
write_to_cluster(Node, Type, Start, End, CommonValBin) ->
    ?LOG_INFO(
        "Writing ~b keys to node ~0p of type ~s",
        [End - Start + 1, Node, Type]
    ),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj =
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riak_object:new(
                            {Type, ?TEST_BUCKET},
                            Key,
                            <<N:32/integer, CVB/binary>>
                        );
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} =
                            riak_client:get({Type, ?TEST_BUCKET}, Key, C),
                        riak_object:update_value(PrevObj, UPDV)
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

delete_from_cluster(Node, Type, Start, End) ->
    ?LOG_INFO(
        "Deleting ~b keys from node ~0p with type ~s", 
        [End - Start + 1, Node, Type]
    ),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try riak_client:delete({Type, ?TEST_BUCKET}, Key, C) of
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


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Type, Start, End, CommonValBin, Errors) ->
    read_from_cluster(Node, Type, Start, End, CommonValBin, Errors, false).

read_from_cluster(Node, Type, Start, End, CommonValBin, Errors, _LogErrors) ->
    ?LOG_INFO(
        "Reading ~b keys from node ~0p of type ~s",
        [End - Start + 1, Node, Type]
    ),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  riak_client:get({Type, ?TEST_BUCKET}, Key, C) of
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
    ?assertEqual(Errors, length(ErrorsFound)).


get_stats(Cluster) ->
    Stats = {0, 0, 0, 0, 0, 0},
        % {prefetch, tofetch, nofetch, object, error, empty}
    lists:foldl(
        fun(N, {PFAcc, TFAcc, NFAcc, FOAcc, FErAcc, FEmAcc}) ->
            S = rt:get_stats(N),
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
            {PFT + PFAcc, TFT + TFAcc, NFT + NFAcc,
                FOT + FOAcc, FErT + FErAcc, FEmAcc + FEmT}
        end,
        Stats,
        Cluster
    ).

set_max_delay([], S) ->
    ?LOG_INFO("Set Max Delay on all clusters to ~w", [S]);
set_max_delay([Node|Rest], {Min, Max}) ->
    rpc:call(
        Node,
        application,
        set_env,
        [riak_kv, replrtq_prompt_min_seconds, Min]),
    rpc:call(
        Node,
        application,
        set_env,
        [riak_kv, replrtq_prompt_max_seconds, Max]),
    set_max_delay(Rest, {Min, Max}).

check_peers_stable(Node, QueueName) ->
    rpc:call(Node, riak_kv_replrtq_peer, update_discovery, [QueueName]).
