%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
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
-module(replication_upgrade).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    FromVersion = proplists:get_value(upgrade_version, TestMetaData, previous),

    ?LOG_INFO("Doing rolling replication upgrade test from ~0p to ~0p",
        [FromVersion, "current"]),

    NumNodes = rt_config:get(num_nodes, 6),

    UpgradeOrder = rt_config:get(repl_upgrade_order, "forwards"),

    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    NodeConfig = [{FromVersion, Conf} || _ <- lists:seq(1, NumNodes)],

    Nodes = rt:deploy_nodes(NodeConfig, [riak_kv, riak_repl]),

    NodeUpgrades = case UpgradeOrder of
        "forwards" ->
            Nodes;
        "backwards" ->
            lists:reverse(Nodes);
        "alternate" ->
            %% eg 1, 4, 2, 5, 3, 6
            lists:flatten(lists:foldl(fun(E, [A,B,C]) -> [B, C, A ++ [E]] end,
                    [[],[],[]], Nodes));
        "random" ->
            %% halfass randomization
            lists:sort(fun(_, _) -> rand:uniform(100) < 50 end, Nodes);
        Other ->
            ?LOG_ERROR("Invalid upgrade ordering ~0p", [Other]),
            erlang:error(case_clause, [Other])
    end,

    ClusterASize = rt_config:get(cluster_a_size, 3),
    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    ?LOG_INFO("Replication First pass...homogenous cluster"),

    %% initial replication run, homogeneous cluster
    replication:replication(ANodes, BNodes, false),

    ?LOG_INFO("Upgrading nodes in order: ~0p", [NodeUpgrades]),
    rt:log_to_nodes(Nodes, "Upgrading nodes in order: ~0p", [NodeUpgrades]),
    %% upgrade the nodes, one at a time
    ok = lists:foreach(fun(Node) ->
                               ?LOG_INFO("Upgrade node: ~0p", [Node]),
                               rt:log_to_nodes(Nodes, "Upgrade node: ~0p", [Node]),
                               rt:upgrade(Node, current),
                               rt:wait_until_pingable(Node),
                               rt:wait_for_service(Node, [riak_kv, riak_pipe, riak_repl]),
                               [rt:wait_until_ring_converged(N) || N <- [ANodes, BNodes]],
                               %% Prior to 1.4.8 riak_repl registered
                               %% as a service before completing all
                               %% initialization including establishing
                               %% realtime connections.
                               %%
                               %% @TODO Ideally the test would only wait
                               %% for the connection in the case of the
                               %% node version being < 1.4.8, but currently
                               %% the rt API does not provide a
                               %% harness-agnostic method do get the node
                               %% version. For now the test waits for all
                               %% source cluster nodes to establish a
                               %% connection before proceeding.
                               case lists:member(Node, ANodes) of
                                   true ->
                                       replication:wait_until_connection(Node);
                                   false ->
                                       ok
                               end,
                               ?LOG_INFO("Replication with upgraded node: ~0p", [Node]),
                               rt:log_to_nodes(Nodes, "Replication with upgraded node: ~0p", [Node]),
                               replication:replication(ANodes, BNodes, true)
                       end, NodeUpgrades),
    pass.
