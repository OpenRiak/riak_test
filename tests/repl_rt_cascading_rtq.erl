%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
-module(repl_rt_cascading_rtq).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"rt-cascading-rtq-systest-a">>).
-define(RING_SIZE, 32).

setup() ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    {SourceLeader, SinkLeaderA, SinkLeaderB, _, _, _} = ClusterNodes = make_clusters(),

    connect_clusters(SourceLeader, SinkLeaderA, "SinkA"),
    connect_clusters(SourceLeader, SinkLeaderB, "SinkB"),
    ClusterNodes.

confirm() ->
    SetupData = setup(),
    rtq_data_buildup_test(SetupData),
    pass.

%% This test case is designed to ensure that there is no realtime
%% queue buildup on sink nodes that do not serve as source nodes for
%% any other clusters. It constructs a simple toplogy with a single
%% source cluster replicating to two sinks. The toplogy for this test
%% is as follows:
%%     +--------+
%%     | Source |
%%     +--------+
%%       ^   ^
%%      /     \
%%     V       V
%% +-------+  +-------+
%% | SinkA |  | SinkB |
%% +-------+  +-------+
rtq_data_buildup_test(ClusterNodes) ->
    {SourceLeader, SinkLeaderA, SinkLeaderB, SourceNodes, _SinkANodes, _SinkBNodes} = ClusterNodes,

    %% Enable RT replication from source cluster "SinkA"
    ?LOG_INFO("Enabling realtime between ~0p and ~0p", [SourceLeader, SinkLeaderB]),
    enable_rt(SourceLeader, SourceNodes, "SinkA"),
    %% Enable RT replication from source cluster "SinkB"
    ?LOG_INFO("Enabling realtime between ~0p and ~0p", [SourceLeader, SinkLeaderA]),
    enable_rt(SourceLeader, SourceNodes, "SinkB"),

    %% Get the baseline byte count for the rtq for each sink cluster
    SinkAInitialQueueSize = rtq_bytes(SinkLeaderA),
    SinkBInitialQueueSize = rtq_bytes(SinkLeaderB),

    %% Write keys to source cluster A
    KeyCount = 1001,
    write_to_cluster(SourceLeader, 1, KeyCount),
    read_from_cluster(SinkLeaderA, 1, KeyCount, 0),
    read_from_cluster(SinkLeaderB, 1, KeyCount, 0),

    %% Verify the rt queue is still at the initial size for both sink clusters
    ?assertEqual(SinkAInitialQueueSize, rtq_bytes(SinkLeaderA)),
    ?assertEqual(SinkBInitialQueueSize, rtq_bytes(SinkLeaderB)).

rtq_bytes(Node) ->
        RtqStatus = rpc:call(Node, riak_repl2_rtq, status, []),
        proplists:get_value(bytes, RtqStatus).

make_clusters() ->
    NodeCount = rt_config:get(num_nodes, 6),
    ?LOG_INFO("Deploy ~b nodes", [NodeCount]),
    Nodes = deploy_nodes(NodeCount, true),

    {SourceNodes, SinkNodes} = lists:split(2, Nodes),
    {SinkANodes, SinkBNodes} = lists:split(2, SinkNodes),
    ?LOG_INFO("SinkANodes: ~0p", [SinkANodes]),
    ?LOG_INFO("SinkBNodes: ~0p", [SinkBNodes]),

    ?LOG_INFO("Build source cluster"),
    repl_util:make_cluster(SourceNodes),

    ?LOG_INFO("Build sink cluster A"),
    repl_util:make_cluster(SinkANodes),

    ?LOG_INFO("Build sink cluster B"),
    repl_util:make_cluster(SinkBNodes),

    SourceFirst = hd(SourceNodes),
    AFirst = hd(SinkANodes),
    BFirst = hd(SinkBNodes),

    %% Name the clusters
    repl_util:name_cluster(SourceFirst, "Source"),
    repl_util:name_cluster(AFirst, "SinkA"),
    repl_util:name_cluster(BFirst, "SinkB"),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkANodes),
    rt:wait_until_ring_converged(SinkBNodes),

    ?LOG_INFO("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkANodes),
    rt:wait_until_transfers_complete(SinkBNodes),

    %% get the leader for the source cluster
    ?LOG_INFO("waiting for leader to converge on the source cluster"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SourceNodes)),

    %% get the leader for the first sink cluster
    ?LOG_INFO("waiting for leader to converge on sink cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkANodes)),

    %% get the leader for the second cluster
    ?LOG_INFO("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkBNodes)),

    SourceLeader = repl_util:get_leader(SourceFirst),
    ALeader = repl_util:get_leader(AFirst),
    BLeader = repl_util:get_leader(BFirst),

    %% Uncomment the following 2 lines to verify that pre-2.0 versions
    %% of Riak behave as expected if cascading writes are disabled for
    %% the sink clusters.
    %% disable_cascading(ALeader, SinkANodes),
    %% disable_cascading(BLeader, SinkBNodes),

    ?LOG_INFO("Source Leader: ~0p SinkALeader: ~0p SinkBLeader: ~0p", [SourceLeader, ALeader, BLeader]),
    {SourceLeader, ALeader, BLeader, SourceNodes, SinkANodes, SinkBNodes}.

%% @ doc Connect two clusters using a given name.
%%connect_cluster(Source, Port, Name) ->
%%    ?LOG_INFO("Connecting ~0p to ~0p for cluster ~0p.",
%%               [Source, Port, Name]),
%%    repl_util:connect_cluster(Source, "127.0.0.1", Port),
%%    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(SourceLeader, SinkLeader, SinkName) ->
    SinkPort = repl_util:get_port(SinkLeader),
    ?LOG_INFO("connect source cluster to ~0p on port ~0p", [SinkName, SinkPort]),
    repl_util:connect_cluster(SourceLeader, "127.0.0.1", SinkPort),
    ?assertEqual(ok, repl_util:wait_for_connection(SourceLeader, SinkName)).

cluster_conf(_CascadingWrites) ->
    [
     {riak_repl,
      [
       %% turn off fullsync
       {fullsync_on_connect, false},
       {fullsync_interval, disabled},
       {max_fssource_cluster, 20},
       {max_fssource_node, 20},
       {max_fssink_node, 20},
       {rtq_max_bytes, 1048576}
      ]},
      {riak_core, [
        {ring_creation_size,        ?RING_SIZE},
        {handoff_concurrency,       8},
        {forced_ownership_handoff,  8},
        {vnode_inactivity_timeout,  4000},
        {vnode_management_timer,    2000}
    ]}
    ].

deploy_nodes(NumNodes, true) ->
    rt:deploy_nodes(NumNodes, cluster_conf(always), [riak_kv, riak_repl]).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(SourceLeader, SourceNodes, SinkName) ->
    repl_util:enable_realtime(SourceLeader, SinkName),
    rt:wait_until_ring_converged(SourceNodes),

    repl_util:start_realtime(SourceLeader, SinkName),
    rt:wait_until_ring_converged(SourceNodes).

%% @ doc Turn off Realtime replication on the cluster lead by LeaderA.
%%disable_cascading(Leader, Nodes) ->
%%    rpc:call(Leader, riak_repl_console, realtime_cascades, [["never"]]),
%%    rt:wait_until_ring_converged(Nodes).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    ?LOG_INFO("Writing ~b keys to node ~0p.", [End - Start, Node]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, ?TEST_BUCKET, 1)).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Errors) ->
    ?LOG_INFO("Reading ~b keys from node ~0p.", [End - Start, Node]),
    Res2 = rt:systest_read(Node, Start, End, ?TEST_BUCKET, 1),
    ?assertEqual(Errors, length(Res2)).
