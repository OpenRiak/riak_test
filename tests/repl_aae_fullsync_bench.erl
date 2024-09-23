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
%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters, runs a fullsync over all partitions, and verifies the missing keys
%% were replicated to the sink cluster.

-module(repl_aae_fullsync_bench).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    NumNodesWanted = 6,         %% total number of nodes needed
    ClusterASize = 3,           %% how many to allocate to cluster A
    NumKeysAOnly = 5000,        %% how many keys on A that are missing on B
    NumKeysBoth = 45000,        %% number of common keys on both A and B
    Conf = [                    %% riak configuration
            {riak_kv,
                [
                 %% Specify fast building of AAE trees
                 {anti_entropy, {on, []}},
                 {anti_entropy_build_limit, {100, 1000}},
                 {anti_entropy_concurrency, 100}
                ]
            },
            {riak_repl,
             [
              {fullsync_strategy, aae},
              {fullsync_on_connect, false},
              {fullsync_interval, disabled}
             ]}
           ],

    %% build clusters
    {ANodes, BNodes} = repl_aae_fullsync_util:make_clusters(NumNodesWanted, ClusterASize, Conf),

    %% run test
    aae_fs_test(NumKeysAOnly, NumKeysBoth, ANodes, BNodes),
    pass.

aae_fs_test(NumKeysAOnly, NumKeysBoth, ANodes, BNodes) ->
    %% populate them with data
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-systest_a">>,
    repl_aae_fullsync_util:prepare_cluster_data(TestBucket, NumKeysAOnly, NumKeysBoth, ANodes, BNodes),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    AllNodes = ANodes ++ BNodes,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    %%---------------------------------------------------------
    %% TEST: fullsync, check that non-RT'd keys get repl'd to B
    %% keys: 1..NumKeysAOnly
    %%---------------------------------------------------------

    rt:log_to_nodes(AllNodes, "Test fullsync from cluster A leader ~0p to cluster B", [LeaderA]),
    ?LOG_INFO("Test fullsync from cluster A leader ~0p to cluster B", [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    {Time,_} = timer:tc(repl_util,start_and_wait_until_fullsync_complete,[LeaderA]),
    ?LOG_INFO("Fullsync completed in ~w seconds", [Time/1000/1000]),

    %% verify data is replicated to B
    NumKeysToVerify = min(1000, NumKeysAOnly),
    rt:log_to_nodes(AllNodes, "Verify: Reading ~b keys repl'd from A(~0p) to B(~0p)",
                    [NumKeysToVerify, LeaderA, BFirst]),
    ?LOG_INFO("Verify: Reading ~b keys repl'd from A(~0p) to B(~0p)",
               [NumKeysToVerify, LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, NumKeysToVerify, TestBucket, 2)),

    ok.

