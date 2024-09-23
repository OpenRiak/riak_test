%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
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
-module(replication2_dirty).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 4),
    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),
    {[AFirst|_] = ANodes, [BFirst|_] = BNodes} = lists:split(ClusterASize, Nodes),

    AllNodes = ANodes ++ BNodes,
    rt:log_to_nodes(AllNodes, "Starting replication2_dirty test"),

    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    rt:log_to_nodes(AllNodes, "Building and connecting Clusters"),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% get the leader for the first cluster
    repl_util:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    %LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    % nothing should be dirty initially
    ?LOG_INFO("Waiting until all nodes clean"),
    wait_until_all_nodes_clean(LeaderA),

    rt:log_to_nodes(AllNodes, "Test basic realtime replication from A -> B"),

    %% write some data on A
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    % ?LOG_INFO("~0p", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    ?LOG_INFO("Writing 2000 more keys to ~0p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 101, 2000, TestBucket, 2)),

    %% verify data is replicated to B
    ?LOG_INFO("Reading 2000 keys written to ~0p from ~0p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 2000, TestBucket, 2)),

    [ ?assertEqual(0, get_dirty_stat(Node)) || Node <- ANodes],
    [ ?assertEqual(0, get_dirty_stat(Node)) || Node <- BNodes],
    [ ?assertEqual({0,0}, get_rt_errors(Node)) || Node <- ANodes],
    [ ?assertEqual({0,0}, get_rt_errors(Node)) || Node <-BNodes],

    ?LOG_INFO("Waiting until all nodes clean"),
    wait_until_all_nodes_clean(LeaderA),

    rt:log_to_nodes(AllNodes, "Verify fullsync after manual dirty flag set"),

    ?LOG_INFO("Manually setting rt_dirty state"),

    % manually set this for now to simulate source errors
    Result = rpc:call(LeaderA, riak_repl_stats, rt_source_errors, []),
    ?LOG_INFO("Result = ~0p", [Result]),

    ?LOG_INFO("Waiting until dirty"),
    wait_until_coord_has_dirty(LeaderA),

    ?LOG_INFO("Starting fullsync"),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),
    ?LOG_INFO("Wait for all nodes to show up clean"),
    wait_until_all_nodes_clean(LeaderA),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    rt:log_to_nodes(AllNodes, "Multiple node test"),
    ?LOG_INFO("Multiple node test"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% test multiple nodes dirty
    [DirtyA , DirtyB | _] = ANodes,
    % manually set this for now to simulate source errors
    ResultA = rpc:call(DirtyA, riak_repl_stats, rt_source_errors, []),
    ResultB = rpc:call(DirtyB, riak_repl_stats, rt_sink_errors, []),
    ?LOG_INFO("Result = ~0p", [ResultA]),
    ?LOG_INFO("Result = ~0p", [ResultB]),

    ?LOG_INFO("Waiting until dirty"),
    wait_until_coord_has_dirty(DirtyA),
    wait_until_coord_has_dirty(DirtyB),

    ?LOG_INFO("Starting fullsync"),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),
    ?LOG_INFO("Wait for all nodes to show up clean"),
    wait_until_all_nodes_clean(LeaderA),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    rt:log_to_nodes(AllNodes, "Multiple node test, one failed during fullsync"),
    ?LOG_INFO("Multiple node test, one failed during fullsync"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% test multiple nodes dirty
    [DirtyC , DirtyD | _] = ANodes,
    % manually set this for now to simulate source errors
    ResultC = rpc:call(DirtyC, riak_repl_stats, rt_source_errors, []),
    ?LOG_INFO("ResultC = ~0p", [ResultC]),

    ?LOG_INFO("Waiting until dirty"),
    wait_until_coord_has_dirty(DirtyC),

    ?LOG_INFO("Starting fullsync"),
    spawn(fun() ->
                timer:sleep(1000),
                ?LOG_INFO("Marking node as dirty during a fullsync"),
                ResultC = rpc:call(DirtyD, riak_repl_stats, rt_source_errors, []),
                ?LOG_INFO("Result = ~0p", [ResultC])
           end),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    ?LOG_INFO("Checking to see if C is still clean"),
    wait_until_node_clean(DirtyC),
    ?LOG_INFO("Checking to see if D is still dirty"),
    wait_until_coord_has_dirty(DirtyD),

    % Clear out all dirty state
    %repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    rt:log_to_nodes(AllNodes, "rt_dirty test completed"),
    pass.

get_dirty_stat(Node) ->
    Stats = rpc:call(Node, riak_repl_stats, get_stats, []),
    %?LOG_INFO("RT_DIRTY = ~w", [proplists:get_value(rt_dirty, Stats, -1)]),
    proplists:get_value(rt_dirty, Stats, -1).

get_rt_errors(Node) ->
    Stats = rpc:call(Node, riak_repl_stats, get_stats, []),
    SourceErrors = proplists:get_value(rt_source_errors, Stats, -1),
    SinkErrors = proplists:get_value(rt_sink_errors, Stats, -1),
    ?LOG_INFO("Source errors = ~0p, sink errors = ~0p", [SourceErrors, SinkErrors]),
    {SourceErrors, SinkErrors}.

wait_until_coord_has_dirty(Node) ->
    Res = rt:wait_until(Node,
                        fun(_) ->
                    ?LOG_INFO("Checking dirty for node ~0p", [Node]),
                    Status = rpc:call(Node, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> false;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            ?LOG_INFO("Nodes = ~0p",[Nodes]),
                            lists:member(erlang:atom_to_list(Node), Nodes)
                    end
            end),
    ?assertEqual(ok, Res).

%wait_until_coord_has_any_dirty(SourceLeader) ->
%    Res = rt:wait_until(SourceLeader,
%                        fun(_) ->
%                    ?LOG_INFO("Checking for any dirty nodes"),
%                    Status = rpc:call(SourceLeader, riak_repl2_fscoordinator, status, []),
%                    case Status of
%                        {badrpc, _} -> false;
%                        [] -> false;
%                        [{_,Stats}|_Rest] ->
%                            NodeString = proplists:get_value(fullsync_suggested, Stats),
%                            Nodes = string:tokens(NodeString,","),
%                            ?LOG_INFO("Nodes = ~0p",[Nodes]),
%                            length(Nodes) > 0
%                    end
%            end),
%    ?assertEqual(ok, Res).
%
%write_until_coord_has_any_dirty(SourceLeader, TestBucket) ->
%    Res = rt:wait_until(SourceLeader,
%                        fun(_) ->
%                    ?LOG_INFO("Writing data while checking for any dirty nodes"),
%                    ?assertEqual([], repl_util:do_write(SourceLeader, 0, 5000, TestBucket, 2)),
%                    Status = rpc:call(SourceLeader, riak_repl2_fscoordinator, status, []),
%                    case Status of
%                        {badrpc, _} -> false;
%                        [] -> false;
%                        [{_,Stats}|_Rest] ->
%                            NodeString = proplists:get_value(fullsync_suggested, Stats),
%                            Nodes = string:tokens(NodeString,","),
%                            ?LOG_INFO("Nodes = ~0p",[Nodes]),
%                            length(Nodes) > 0
%                    end
%            end),
%    ?assertEqual(ok, Res).



%% yeah yeah, copy paste, I know
wait_until_node_clean(Node) ->
    Res = rt:wait_until(Node,
                        fun(_) ->
                    ?LOG_INFO("Checking dirty for node ~0p", [Node]),
                    Status = rpc:call(Node, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> false;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            ?LOG_INFO("Nodes = ~0p",[Nodes]),
                            not lists:member(erlang:atom_to_list(Node), Nodes)
                    end
            end),
    ?assertEqual(ok, Res).

wait_until_all_nodes_clean(Leader) ->
    Res = rt:wait_until(Leader,
                        fun(L) ->
                    ?LOG_INFO("Checking for all nodes clean"),
                    Status = rpc:call(L, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> true;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            ?LOG_INFO("Nodes = ~0p",[Nodes]),
                            Nodes == []

                    end
            end),
    ?assertEqual(ok, Res).


