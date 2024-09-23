%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(repl_rt_pending).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(RPC_TIMEOUT, 5000).
-define(HB_TIMEOUT,  2000).

%% Replication Realtime Heartbeat test
%% Valid for EE version 1.3.2 and up
%%
%% If both sides of an RT replication connection support it, a heartbeat
%% message is sent from the RT Source to the RT Sink every
%% {riak_repl, rt_heartbeat_interval} which default to 15s.  If
%% a response is not received in {riak_repl, rt_heartbeat_timeout}, also
%% default to 15s then the source connection exits and will be re-established
%% by the supervisor.
%%
%% RT Heartbeat messages are supported between EE releases 1.3.2 and up.
%%
%% Test:
%% -----
%% Change the heartbeat_interval and heartbeat_timeout to 2 seconds,
%% Start up two >1.3.2 clusters and connect them,
%% Enable RT replication,
%% Write some objects to the source cluster (A),
%% Verify they got to the sink cluster (B),
%% Verify that heartbeats are being acknowledged by the sink (B) back to source (A),
%% Interupt the connection so that packets can not flow from A -> B,
%% Verify that the connection is restarted after the heartbeat_timeout period,
%% Verify that heartbeats are being acknowledged by the sink (B) back to source (A),
%% Write some objects to the source cluster (A),
%% Verify they got to the sink cluster (B),
%% Have a cold beverage.

%% @doc riak_test entry point
confirm() ->
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, BNodes} = make_connected_clusters(),

    %% load intercepts. See ../intercepts/riak_repl_rt_intercepts.erl
    load_intercepts(LeaderA),

    %% Enable RT replication from cluster "A" to cluster "B"
    enable_bi_rt(LeaderA, ANodes, LeaderB, BNodes),

    %% Verify that heartbeats are being acknowledged by the sink (B) back to source (A)
    %%?assertEqual(verify_heartbeat_messages(LeaderA), true),

    %% Verify RT repl of objects
    write_n_keys(LeaderA, LeaderB, 1, 10000),

    write_n_keys(LeaderB, LeaderA, 10001, 20000),

    RTQStatus = rpc:call(LeaderA, riak_repl2_rtq, status, []),

    Consumers = proplists:get_value(consumers, RTQStatus),
    case proplists:get_value("B", Consumers) of
        undefined ->
            [];
            Consumer ->
                Unacked = proplists:get_value(unacked, Consumer, 0),
                ?LOG_INFO("unacked: ~0p", [Unacked]),
                ?assertEqual(0, Unacked)
    end,
    %% Cause heartbeat messages to not be delivered, but remember the current
    %% Pid of the RT connection. It should change after we stop heartbeats
    %% because the RT connection will restart if all goes well.
    %RTConnPid1 = get_rt_conn_pid(LeaderA),
    %?LOG_INFO("Suspending HB"),
    %suspend_heartbeat_messages(LeaderA),

    %% sleep longer than the HB timeout interval to force re-connection;
    %% and give it time to restart the RT connection. Wait an extra 2 seconds.
    %timer:sleep(?HB_TIMEOUT + 2000),

    %% Verify that RT connection has restarted by noting that it's Pid has changed
    %RTConnPid2 = get_rt_conn_pid(LeaderA),
    %?assertNotEqual(RTConnPid1, RTConnPid2),

    %% Verify that heart beats are not being ack'd
    %rt:log_to_nodes([LeaderA], "Verify suspended HB"),
    %?assertEqual(verify_heartbeat_messages(LeaderA), false),

    %% Resume heartbeat messages from source and allow some time to ack back.
    %% Wait one second longer than the timeout
    %rt:log_to_nodes([LeaderA], "Resuming HB"),
    %resume_heartbeat_messages(LeaderA),
    %timer:sleep(?HB_TIMEOUT + 1000),

    %% Verify that heartbeats are being acknowledged by the sink (B) back to source (A)
    %rt:log_to_nodes([LeaderA], "Verify resumed HB"),
    %?assertEqual(verify_heartbeat_messages(LeaderA), true),

    %% Verify RT repl of objects
    %verify_rt(LeaderA, LeaderB),

    pass.

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_bi_rt(LeaderA, ANodes, LeaderB, BNodes) ->
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:enable_realtime(LeaderB, "A"),
    rt:wait_until_ring_converged(BNodes),

    repl_util:start_realtime(LeaderB, "A"),
    rt:wait_until_ring_converged(ANodes).


%% @doc Verify that RealTime replication is functioning correctly by
%%      writing some objects to cluster A and checking they can be
%%      read from cluster B. Each call creates a new bucket so that
%%      verification can be tested multiple times independently.
write_n_keys(Source, Destination, M, N) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = M,
    Last = N,

    %% Write some objects to the source cluster (A),
    ?LOG_INFO("Writing ~b keys to ~0p, which should RT repl to ~0p",
               [Last-First+1, Source, Destination]),
    ?assertEqual([], repl_util:do_write(Source, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    ?LOG_INFO("Reading ~b keys written from ~0p", [Last-First+1, Destination]),
    ?assertEqual(0, repl_util:wait_for_reads(Destination, First, Last, TestBucket, 2)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IPB, PortB}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    ?LOG_INFO("connect cluster A:~0p to B on port ~b", [LeaderA, PortB]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", PortB),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    {ok, {_IPA, PortA}} = rpc:call(LeaderA, application, get_env,
                                 [riak_core, cluster_mgr]),
    ?LOG_INFO("connect cluster B:~0p to A on port ~b", [LeaderB, PortA]),
    repl_util:connect_cluster(LeaderB, "127.0.0.1", PortA),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderB, "A")).



%% @doc Create two clusters of 3 nodes each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_connected_clusters() ->
    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              %% override defaults for RT heartbeat so that we
              %% can see faults sooner and have a quicker test.
              {rt_heartbeat_interval, ?HB_TIMEOUT},
              {rt_heartbeat_timeout, ?HB_TIMEOUT}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    %% get the leader for the first cluster
    ?LOG_INFO("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    %% get the leader for the second cluster
    ?LOG_INFO("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    %% Name the clusters
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    %% Connect for replication
    connect_clusters(AFirst, BFirst),

    {AFirst, BFirst, ANodes, BNodes}.

%% @doc Load intercepts file from ../intercepts/riak_repl2_rtsource_helper_intercepts.erl
load_intercepts(Node) ->
    rt_intercept:load_code(Node).

%% @doc Suspend heartbeats from the source node
%suspend_heartbeat_messages(Node) ->
    %% disable forwarding of the heartbeat function call
%    ?LOG_INFO("Suspend sending of heartbeats from node ~0p", [Node]),
%    rt_intercept:add(Node, {riak_repl2_rtsource_helper,
%                            [{{send_heartbeat, 1}, drop_send_heartbeat}]}).

%% @doc Resume heartbeats from the source node
%resume_heartbeat_messages(Node) ->
    %% enable forwarding of the heartbeat function call
%    ?LOG_INFO("Resume sending of heartbeats from node ~0p", [Node]),
%    rt_intercept:add(Node, {riak_repl2_rtsource_helper,
%                            [{{send_heartbeat, 1}, forward_send_heartbeat}]}).

%% @doc Get the Pid of the first RT source connection on Node
%get_rt_conn_pid(Node) ->
%    [{_Remote, Pid}|Rest] = rpc:call(Node, riak_repl2_rtsource_conn_sup, enabled, []),
%    case Rest of
%        [] -> ok;
%        RR -> ?LOG_INFO("Other connections: ~0p", [RR])
%   end,
%    Pid.

%% @doc Verify that heartbeat messages are being ack'd from the RT sink back to source Node
%verify_heartbeat_messages(Node) ->
%    ?LOG_INFO("Verify heartbeats"),
%    Pid = get_rt_conn_pid(Node),
%    Status = rpc:call(Node, riak_repl2_rtsource_conn, status, [Pid], ?RPC_TIMEOUT),
%    HBRTT = proplists:get_value(hb_rtt, Status),
%    case HBRTT of
%        undefined ->
%            false;
%        RTT ->
%            is_integer(RTT)
%    end.
