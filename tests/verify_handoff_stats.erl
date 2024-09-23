%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
%% Copyright (c) 2018-2023 Workday, Inc.
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
-module(verify_handoff_stats).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%%
%% This test checks to make sure that the handoff statistics we have added
%% to Riak are incremented when ownership, hinted, and repair handoff
%% events occur.
%%
%% NB.  Repair stat checks are currently disabled but will be fixed soon.
%%
%% This test uses intercepts in the riak_kv_worker process to send start
%% and stop events back to a local rt_kv_worker_proc process running
%% locally within the riak test process (VM) space.  This local process
%% records starts and completion of riak_kv_worker instances, giving
%% us a way to track that handoff events have occurred.
%%
%% In addition, the ownership, hinted, and repair stats are compared
%% before and after handoff has completed, to ensure they have (at least)
%% incremented.
%%
%% Due to the sensitivity of the distribution of partition ownership to
%% nodes based on cluster size, no attempts are made in this test to ensure
%% that handoffs from specific {Node, Partition} pairs are made, only
%% that _some_ handoffs occur.
%%
%% TODO Instrument the servers with an intercept that records which
%% partitions are handed off from which nodes, and use that information
%% to verify the exact number of transfers from nodes in the cluster.
%%

confirm() ->

    NObjs = 1000,
    NumNodes = 3,
    Config = [
        {riak_core, [
            {ring_creation_size, 8},
            {vnode_inactivity_timeout, 5000},
            {handoff_acksync_threshold, 20},
            {handoff_receive_timeout, 2000}
        ]}
    ],

    run_test(Config, NumNodes, NObjs).

%% @private
run_test(Config, NumNodes, NObjs) ->
    ?LOG_INFO("Testing handoff (NObjs=~b)", [NObjs]),

    ?LOG_INFO("Spinning up ~b test nodes"),
    [RootNode | TestNodes] = Nodes = rt:deploy_nodes(NumNodes, Config),

    rt:wait_for_service(RootNode, riak_kv),

    %% These intercepts will be used to signal back to the rt_kv_worker_proc
    %% running in this test's BEAM when handoff work has started and
    %% completed throughout the cluster.
    rt_kv_worker_proc:add_intercept(Nodes),

    ?LOG_INFO("Populating root node."),
    rt:systest_write(RootNode, NObjs),

    %% write one object with a bucket type, why not
    rt:create_and_activate_bucket_type(RootNode, <<"type">>, []),
    rt:systest_write(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),

    %% Test ownership handoff on each node:
    ?LOG_INFO("Testing ownership handoff for cluster."),
    Cluster = lists:foldl(
        fun(TestNode, CurrentNodes) ->
            ok = test_ownership_handoff(RootNode, TestNode, CurrentNodes, NObjs),
            [TestNode | CurrentNodes]
        end,
        [RootNode],
        TestNodes
    ),
    % might as well do the check, just because
    ?assertEqual(lists:sort(Nodes), lists:sort(Cluster)),

    %% Test hinted handoff by stopping the root node and forcing some handoff
    ok = test_hinted_handoff(Nodes, NObjs),

    %% Test repair handoff by stopping the root node deleting the bitcask directory
    %% on that node, restarting, and issuing a repair on that node.
    ok = test_repair_handoff(Nodes),

    pass.

%% @private
test_ownership_handoff(RootNode, NewNode, CurrentNodes, _NObjs) ->
    ?LOG_INFO("Testing ownership handoff by adding node ~0p to ~0p ...", [NewNode, CurrentNodes]),

    ?LOG_INFO("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    BeforeHandoffStats = get_handoff_stats(ownership, CurrentNodes),

    start_kv_worker_proc(ownership),

    ?LOG_INFO("Joining new node with cluster."),
    rt:join(NewNode, RootNode),
    NewCluster = [NewNode | CurrentNodes],
    ?assertEqual(ok, rt:wait_until_nodes_ready(NewCluster)),
    rt:wait_until_no_pending_changes(NewCluster),

    %% verify that all transfers have completed (accum must be empty)
    %% and that there have been some transfers
    {ok, {TotalTransfers, []}} = rt_kv_worker_proc:stop(),
    ?assert(TotalTransfers > 0),

    AfterHandoffStats = get_handoff_stats(ownership, NewCluster),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, ownership, bytes_sent),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, ownership, objects_sent),

    ok.

%% @private
test_hinted_handoff([RootNode | OtherNodes] = Nodes, NObjs) ->
    ?LOG_INFO("Testing hinted handoff by stopping ~0p, doing some writes, and restarting ...", [
        RootNode
    ]),

    BeforeHandoffStats = get_handoff_stats(hinted, OtherNodes),

    start_kv_worker_proc(hinted),

    rt:stop(RootNode),

    AnotherNode = rt_util:random_element(OtherNodes),
    ?LOG_INFO("Writing ~b objects to ~0p ...", [NObjs, AnotherNode]),
    rt:systest_write(AnotherNode, NObjs),
    Results = rt:systest_read(AnotherNode, NObjs),
    ?assertEqual(0, length(Results)),

    rt:start(RootNode),
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    %% verify that all transfers have completed (accum must be empty)
    %% and that there have been some transfers
    {ok, {TotalTransfers, []}} = rt_kv_worker_proc:stop(),
    ?assert(TotalTransfers > 0),

    AfterHandoffStats = get_handoff_stats(hinted, OtherNodes),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, hinted, bytes_sent),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, hinted, objects_sent),

    ok.

%% @private
test_repair_handoff(Nodes) ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    case verify_handoff:backend_dir(Backend) of
        [_|_] = BackendDir ->
            test_repair_handoff(Nodes, BackendDir);
        _ ->
            ?LOG_INFO(
                "Skipping handoff repair test for unsupported backend: ~0p",
                [Backend])
    end.

%% @private
test_repair_handoff([RootNode | OtherNodes] = Nodes, BackendDir) ->
    ?LOG_INFO(
        "Testing repair handoff by stopping ~0p, deleting data,"
        " and issuing a repair ...", [RootNode]),

    BeforeHandoffStats = get_handoff_stats(repair, OtherNodes),

    start_kv_worker_proc(repair),

    rt:stop(RootNode),
    rt:clean_data_dir(RootNode, BackendDir),
    rt:start(RootNode),

    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    issue_repair(RootNode),
    rt:wait_until_transfers_complete(Nodes),

    %% verify that all transfers have completed (accum must be empty)
    %% and that there have been some transfers
    {ok, {TotalTransfers, []}} = rt_kv_worker_proc:stop(),
    ?assert(TotalTransfers > 0),

    AfterHandoffStats = get_handoff_stats(repair, OtherNodes),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, repair, bytes_sent),
    check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, repair, objects_sent),

    ok.

%% @private
issue_repair(Node) ->
    ?LOG_INFO("Issuing repairs on node ~0p", [Node]),
    rpc:call(Node, riak_client, repair_node, []),
    %% give handoff a few seconds to kick off xfers...
    ?LOG_INFO("Sleeping 5secs ..."),
    timer:sleep(5000).

%% @private
%% Start the rt_kv_worker_proc.
%% This proc will receive messages from riak_kv_workers running in vnodes
%% indicating when handoff work is started and completed.
%% We track the total number of worker events, in addition to recording
%% any uncompleted events.
start_kv_worker_proc(Type) ->
    SCb = fun(NodePid, Accum) ->
        work_started(NodePid, Accum, Type)
    end,
    CCb = fun(NodePid, Accum) ->
        work_completed(NodePid, Accum, Type)
    end,
    Opts = [
        {started_callback, SCb},
        {completed_callback, CCb},
        {accum, {0, []}}
    ],
    ?assertMatch(
        {ok, Pid} when erlang:is_pid(Pid),
        rt_kv_worker_proc:start_link(Opts) ).

%% @private
get_handoff_stats(Type, Node) when is_atom(Node) ->
    Stats = rt:get_stats(Node),
    BytesSentStatName = stat_name(Type, bytes_sent),
    ObjectsSentStatName = stat_name(Type, objects_sent),
    ?LOG_DEBUG("Stats: ~0p ~0p", [BytesSentStatName, ObjectsSentStatName]),
    [
        {BytesSentStatName, rt:get_stat(Stats, BytesSentStatName)},
        {ObjectsSentStatName, rt:get_stat(Stats, ObjectsSentStatName)}
    ];
get_handoff_stats(Type, Nodes) ->
    [{Node, get_handoff_stats(Type, Node)} || Node <- Nodes].

%% @private
stat_name(Type, Moniker) ->
    list_to_atom(atom_to_list(Type) ++ "_handoff_" ++ atom_to_list(Moniker)).

%% @private
check_any_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, Type, Stat) ->
    StatName = stat_name(Type, Stat),
    ?LOG_INFO("Verifying ~0p stat increased...", [StatName]),
    ?assert(
        lists:any(
            fun({Node, BeforeStats}) ->
                case proplists:get_value(Node, AfterHandoffStats) of
                    undefined ->
                        false;
                    AfterStats ->
                        BeforeStat = proplists:get_value(StatName, BeforeStats),
                        AfterStat = proplists:get_value(StatName, AfterStats),
                        BeforeStat < AfterStat
                end
            end,
            BeforeHandoffStats
        )
    ).

%% @private
work_started({Node, Pid}, {TotalTransfers, Accum}, Type) ->
    ?LOG_INFO(
        "Started ~0p handoff on node ~0p pid ~0p TotalTransfers=~0p",
        [Type, Node, Pid, TotalTransfers]),
    {TotalTransfers + 1, increment_counter(Accum, Node)}.

%% @private
work_completed({Node, Pid}, {TotalTransfers, Accum}, Type) ->
    ?LOG_INFO(
        "Completed ~0p handoff on node ~0p pid ~0p TotalTransfers=~0p",
        [Type, Node, Pid, TotalTransfers]),
    {ok, {TotalTransfers, decrement_counter(Accum, Node)}}.

%% @private
increment_counter(Accum, Node) ->
    case proplists:get_value(Node, Accum) of
        undefined ->
            [{Node, 1} | Accum];
        Value ->
            [{Node, Value + 1} | proplists:delete(Node, Accum)]
    end.

%% @private
decrement_counter(Accum, Node) ->
    case proplists:get_value(Node, Accum) of
        undefined ->
            {error, {not_found, Node, Accum}};
        1 ->
            proplists:delete(Node, Accum);
        Value ->
            [{Node, Value - 1} | proplists:delete(Node, Accum)]
    end.
