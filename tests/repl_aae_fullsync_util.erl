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
%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters and starts a single fullsync worker for a single AAE tree.
-module(repl_aae_fullsync_util).

-export([
    make_clusters/3,
    prepare_cluster_data/5
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(rt, [deploy_nodes/3,
             join/2,
             log_to_nodes/2,
             log_to_nodes/3]).

make_clusters(NumNodesWanted, ClusterSize, Conf) ->
    NumNodes = rt_config:get(num_nodes, NumNodesWanted),
    ClusterASize = rt_config:get(cluster_a_size, ClusterSize),
    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    Nodes = deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),
    {ANodes, BNodes}.

prepare_cluster_data(TestBucket, NumKeysAOnly, _NumKeysBoth, [AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    AllNodes = ANodes ++ BNodes,
    log_to_nodes(AllNodes, "Starting AAE Fullsync test"),

    %% clusters are not connected, connect them

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    %% we'll need to wait for cluster names before continuing
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    ?LOG_INFO("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    ?LOG_INFO("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    %% get the leader for the first cluster
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                                 [riak_core, cluster_mgr]),

    ?LOG_INFO("connect cluster A:~0p to B on port ~0p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %% make sure we are connected
    ?LOG_INFO("Wait for cluster connection A:~0p -> B:~0p:~0p", [LeaderA, BFirst, Port]),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %%---------------------------------------------------
    %% TEST: write data, NOT replicated by RT or fullsync
    %% keys: 1..NumKeysAOnly
    %%---------------------------------------------------

    ?LOG_INFO("Writing ~0p keys to A(~0p)", [NumKeysAOnly, AFirst]),
    ?assertEqual([], repl_util:do_write(AFirst, 1, NumKeysAOnly, TestBucket, 2)),

    %% check that the keys we wrote initially aren't replicated yet, because
    %% we've disabled fullsync_on_connect
    ?LOG_INFO("Check keys written before repl was connected are not present"),
    Res2 = rt:systest_read(BFirst, 1, NumKeysAOnly, TestBucket, 1, <<>>, true),
    ?assertEqual(NumKeysAOnly, length(Res2)),

    %% wait for the AAE trees to be built so that we don't get a not_built error
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),
    ok.
