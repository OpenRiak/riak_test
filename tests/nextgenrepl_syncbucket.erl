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
%% This module implements a riak_test to test and time the scripted sync
%% of buckets added in Riak 3.4.1

-module(nextgenrepl_syncbucket).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).
-define(NGR_INIT_TIMEOUT, 10000).
-define(REPL_PAUSE, 2000).
-define(KEY_COUNT, 50000).
-define(SNK_WORKERS, 8).

-define(CONFIG(RingSize, NVal, SrcQueueDefns, LocalQueue, PeerQueue), 
    [
        {
            riak_core,
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
        {
            riak_kv,
            [
                {anti_entropy, {off, []}},
                {tictacaae_active, active},
                {tictacaae_parallelstore, leveled_ko},
                {tictacaae_rebuildwait, 4},
                {tictacaae_rebuilddelay, 3600},
                {tictacaae_exchangetick, 120 * 1000},
                {tictacaae_rebuildtick, 3600000}, 
                {ttaaefs_maxresults, 8},
                {ttaaefs_rangeboost, 16},
                {ttaaefs_queuename, LocalQueue},
                {ttaaefs_queuename_peer, PeerQueue},
                {delete_mode, keep},
                {replrtq_enablesrc, true},
                {replrtq_srcqueue, SrcQueueDefns},
                {ngr_initial_timeout, ?NGR_INIT_TIMEOUT}
            ]
        }
    ]
).

-define(SNK_CONFIG(ClusterName, PeerList, IP, Port), [
    {riak_kv, [
        {replrtq_enablesink, true},
        {replrtq_sinkqueue, ClusterName},
        {replrtq_sinkpeers, PeerList},
        {replrtq_sinkworkers, ?SNK_WORKERS},
        {replrtq_peer_discovery, true},
        {ttaaefs_peerip, IP},
        {ttaaefs_peerport, Port},
        {ttaaefs_peerprotocol, pb}
    ]}
]).

confirm() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters(
            [
                {
                    4,
                    ?CONFIG(
                        ?A_RING,
                        ?A_NVAL,
                        "cluster_b:any",
                        cluster_b,
                        cluster_a
                    )
                },
                {
                    2,
                    ?CONFIG(
                        ?B_RING,
                        ?B_NVAL,
                        "cluster_a:any",
                        cluster_a,
                        cluster_b
                    )
                }
            ]
        ),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    wait_for_convergence(ClusterA, ClusterB),

    ?LOG_INFO("Clusters formed - ready for test"),
    test_repl_between_clusters(ClusterA, ClusterB).


test_repl_between_clusters(ClusterA, ClusterB) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),

    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    lists:foreach(compare_peer_info(4), ClusterA),
    lists:foreach(compare_peer_info(2), ClusterB),

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {IP ++ ":" ++ integer_to_list(Port) ++ ":pb", IP, Port}
        end,

    {PeerA, IPA, PortA} = PeerConfigFun(NodeA),
    {PeerB, IPB, PortB} = PeerConfigFun(NodeB),

    reset_peer_config(ClusterA, cluster_a, PeerB, IPB, PortB),
    reset_peer_config(ClusterB, cluster_b, PeerA, IPA, PortA),
    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    ?LOG_INFO("Confirm riak_kv is up on all nodes."),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB
    ),

    timer:sleep(?REPL_PAUSE),
    ?LOG_INFO("Update discovery of peers on Cluster B"),
    update_discovery(ClusterB, cluster_b),

    ?LOG_INFO(
        "Replication enabled - adding ~w objects to A to be replicated",
        [?KEY_COUNT]
    ),

    CVB = <<"CommonValueForInitialInsert">>,
    write_to_cluster(NodeA, 1, ?KEY_COUNT, <<"Bucket1">>, true, CVB),
    
    CheckQueueEmptyFun =
        fun(Cluster, QueueName) ->
            ?LOG_INFO("Waiting for real-time queues to drain"),
            {P1, P2, P3} =
                get_all_queue_lengths(Cluster, QueueName, {0, 0, 0}),
            Total = P1 + P2 + P3,
            ?LOG_INFO("Real time queues at ~w", [Total]),
            0 == Total
        end,

    CheckFun =
        fun(Node) ->
            ?LOG_INFO("Wait for clusters to be in sync"),
            ok = 
                erpc:call(Node, riak_kv_ttaaefs_manager, set_allsync, [3, 1]),
            CheckResult =
                erpc:call(Node, riak_client, ttaaefs_fullsync, [all_check]),
            ?LOG_INFO("All check returned ~0p", [CheckResult]),
            CheckResult == {root_compare, 0}
        end,

    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterA, cluster_b) end),
    rt:wait_until(fun() -> CheckFun(NodeA) end),
    ?LOG_INFO("Clusters in sync after initial write to A"),

    ?LOG_INFO("Update discovery of peers on Cluster A"),
    update_discovery(ClusterA, cluster_a),

    ?LOG_INFO(
        "Replication enabled - adding ~w objects to B to be replicated",
        [?KEY_COUNT]
    ),

    write_to_cluster(NodeB, 1, ?KEY_COUNT, <<"Bucket2">>, true, CVB),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterB, cluster_a) end),
    rt:wait_until(fun() -> CheckFun(NodeB) end),
    ?LOG_INFO("Clusters in sync after initial write to B"),

    ?LOG_INFO(
        "Suspend RTQ on a single node - some PUTs will replicate, some not"
    ),
    erpc:call(NodeA, riak_kv_replrtq_src, suspend_rtq, [cluster_b]),
    erpc:call(NodeB, riak_kv_replrtq_src, suspend_rtq, [cluster_a]),
    write_to_cluster(
        NodeA,
        ?KEY_COUNT + 1,
        2 * ?KEY_COUNT,
        <<"Bucket1">>,
        true,
        CVB
    ),
    write_to_cluster(
        NodeB,
        ?KEY_COUNT + 1,
        2 * ?KEY_COUNT,
        <<"Bucket2">>,
        true,
        CVB
    ),
    ?LOG_INFO("Re-enable queues, and check queues have drained to empty"),
    erpc:call(NodeA, riak_kv_replrtq_src, resume_rtq, [cluster_b]),
    erpc:call(NodeB, riak_kv_replrtq_src, resume_rtq, [cluster_a]),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterA, cluster_b) end),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterB, cluster_a) end),
    ?LOG_INFO("Update discovery of peers on following resumption"),
    update_discovery(ClusterA, cluster_a),
    update_discovery(ClusterB, cluster_b),

    ?LOG_INFO("Write more keys that should replicate"),
    write_to_cluster(
        NodeA,
        2 * ?KEY_COUNT + 1,
        3 * ?KEY_COUNT,
        <<"Bucket1">>,
        true,
        CVB
    ),
    write_to_cluster(
        NodeB,
        2 * ?KEY_COUNT + 1,
        3 * ?KEY_COUNT,
        <<"Bucket2">>,
        true,
        CVB
    ),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterA, cluster_b) end),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterB, cluster_a) end),

    ?LOG_INFO("Confirm clusters are out of sync"),
    false = CheckFun(NodeA),

    ?LOG_INFO("Launch resync of Bucket1 on A"),
    erpc:call(NodeA, riak_client, resync_bucket, [<<"Bucket1">>]),
    ?LOG_INFO("Resync of Bucket1 complete"),

    ?LOG_INFO("Launch resync of Bucket2 on A"),
    erpc:call(NodeA, riak_client, resync_bucket, [<<"Bucket2">>]),
    ?LOG_INFO("Resync of Bucket2 complete"),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterA, cluster_b) end),
    rt:wait_until(fun() -> CheckQueueEmptyFun(ClusterB, cluster_a) end),
    ?LOG_INFO("Confirm clusters are now in-sync"),
    rt:wait_until(fun() -> CheckFun(NodeA) end),

    pass.

wait_for_convergence(ClusterA, ClusterB) ->
    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB
    ).

compare_peer_info(ExpectedPeers) ->
    fun(Node) ->
        {pb, {IP, Port}} = lists:keyfind(pb, 1, rt:connection_info(Node)),
        MemberList =
            lists:map(
                fun({IPm, Portm}) ->
                    {list_to_binary(IPm), Portm}
                end,
                erpc:call(Node, riak_client, membership_request, [pb])
            ),
        {ok, Pid} = riakc_pb_socket:start(IP, Port),
        {ok, MemberList} = riakc_pb_socket:peer_discovery(Pid),
        ?LOG_INFO("Discovered Member list (two ways) ~0p", [MemberList]),
        ?assert(lists:member({list_to_binary(IP), Port}, MemberList)),
        ?assertMatch(ExpectedPeers, length(MemberList)),
        riakc_pb_socket:stop(Pid)
    end.

reset_peer_config(SnkCluster, ClusterName, Peer, IP, Port) ->
    ClusterSNkCfg = ?SNK_CONFIG(ClusterName, Peer, IP, Port),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterSNkCfg) end,
        SnkCluster
    ).


write_to_cluster(Node, Start, End, Bucket, NewObj, CVB) ->
    ?LOG_INFO("Writing ~b keys to node ~0p.", [End - Start + 1, Node]),
    ?LOG_WARNING("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F =
        fun(N, Acc) ->
            Key = key(N),
            Obj =
                case NewObj of
                    true ->
                        riak_object:new(
                            Bucket,
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

key(N) ->
    list_to_binary(io_lib:format("~8..0B~n", [N])).


get_all_queue_lengths([], _QueueName, Acc) ->
    Acc;
get_all_queue_lengths([Node|Rest], QueueName, {A1, A2, A3}) ->
    {QueueName, {P1, P2, P3}} =
        erpc:call(Node, riak_kv_replrtq_src, length_rtq, [QueueName]),
    get_all_queue_lengths(Rest, QueueName, {A1 + P1, A2 + P2, A3 + P3}).

update_discovery([], _QueueName) ->
    ok;
update_discovery([Node|Rest], QueueName) ->
    erpc:call(Node, riak_kv_replrtq_peer, update_discovery, [QueueName]),
    update_discovery(Rest, QueueName).