%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%%
-module(replication2_rt_sink_connection).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(HB_TIMEOUT,  2000).

confirm() ->
    NumNodes = rt_config:get(num_nodes, 6),

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
    {ANodes, Rest} = lists:split(2, Nodes),
    {BNodes, CNodes} = lists:split(2, Rest),

    ?LOG_INFO("Loading intercepts."),
    CNode = hd(CNodes),
    rt_intercept:load_code(CNode),
    rt_intercept:add(CNode, {riak_repl_ring_handler,
                            [{{handle_event, 2}, slow_handle_event}]}),

    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),
    ?LOG_INFO("CNodes: ~0p", [CNodes]),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    % ?LOG_INFO("Waiting for cluster A to converge"),
    % rt:wait_until_ring_converged(ANodes),

    % ?LOG_INFO("Waiting for cluster B to converge"),
    % rt:wait_until_ring_converged(BNodes),

    ?LOG_INFO("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    ?LOG_INFO("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    ?LOG_INFO("Naming A"),
    repl_util:name_cluster(AFirst, "A"),

    ?LOG_INFO("Naming B"),
    repl_util:name_cluster(BFirst, "B"),

    connect_clusters(AFirst, BFirst),

    enable_rt(AFirst, ANodes),

    ?LOG_INFO("Adding 4th node to the A cluster"),
    rt:join(CNode, AFirst),

    [verify_connectivity(Node) || Node <- ANodes],

    verify_connectivity(CNode),

    pass.

%% @doc Verify connectivity between sources and sink.
verify_connectivity(Node) ->
    rt:wait_until(Node, fun(N) ->
                {ok, Connections} = rpc:call(N,
                                             riak_core_cluster_mgr,
                                             get_connections,
                                             []),
                ?LOG_INFO("Waiting for sink connections on ~0p: ~0p.",
                           [Node, Connections]),
                Connections =/= []
        end).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    ?LOG_INFO("Connect cluster A:~0p to B on port ~0p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    ?LOG_INFO("Enabling RT replication: ~0p ~0p.", [LeaderA, ANodes]),
    repl_util:enable_realtime(LeaderA, "B"),
    repl_util:start_realtime(LeaderA, "B").
