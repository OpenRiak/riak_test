%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.
%% Copyright (c) 2023 Workday, Inc.
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
-module(repl_cancel_fullsync).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET,
        <<"repl-cancel-fullsync-failures-systest_a">>).
-define(NUM_KEYS, 1000).

-define(CONF(Retries), [
        {riak_core,
            [
             {ring_creation_size, 8},
             {default_bucket_props,
                 [
                     {n_val, 1},
                     {allow_mult, true},
                     {dvv_enabled, true}
                 ]}
            ]
        },
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
          {fullsync_strategy, keylist},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_retries, Retries}
         ]}
        ]).

%% @doc Ensure we can cancel a fullsync and restart it.
confirm() ->
    rt:set_advanced_conf(all, ?CONF(5)),

    Nodes = [ANodes, BNodes] = rt:build_clusters([3, 3]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),

    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    ?LOG_INFO("Naming clusters."),
    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    ?LOG_INFO("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),

    ?LOG_INFO("Get leaders."),
    LeaderA = repl_util:get_leader(AFirst),
    LeaderB = repl_util:get_leader(BFirst),

    ?LOG_INFO("Finding connection manager ports."),
    BPort = repl_util:get_port(LeaderB),

    ?LOG_INFO("Connecting cluster A to B"),
    repl_util:connect_cluster_by_name(LeaderA, BPort, "B"),

    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS, ?TEST_BUCKET),

    repl_util:read_from_cluster(BFirst, 1, ?NUM_KEYS, ?TEST_BUCKET,
                                ?NUM_KEYS),

    ?LOG_INFO("Test fullsync from cluster A leader ~0p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    ?LOG_INFO("Starting fullsync."),
    rt:log_to_nodes(lists:flatten(Nodes), "Starting fullsync."),
    R1 = rpc:call(LeaderA, riak_repl_console, fullsync, [["start"]]),
    ?assertEqual(ok, R1),
    repl_util:wait_until_fullsync_started(LeaderA),
    ?LOG_INFO("Fullsync running."),

    %% Get all active keylist server pids
    Coordinators = [Pid || {"B", Pid} <-
        riak_repl2_fscoordinator_sup:started(LeaderA)],
    States = [sys:get_state(P) || P <- Coordinators],
    KeylistPids = lists:flatten([element(12, State) || State <- States]),
    KLStates = [sys:get_state(Pid) || {Pid, _} <- KeylistPids],
    [?assertEqual(state, element(1, State)) || State <- KLStates],

    ?LOG_INFO("Stopping fullsync."),
    rt:log_to_nodes(lists:flatten(Nodes), "Stopping fullsync."),
    R2 = rpc:call(LeaderA, riak_repl_console, fullsync, [["stop"]]),
    ?assertEqual(ok, R2),
    repl_util:wait_until_fullsync_stopped(LeaderA),
    ?LOG_INFO("Fullsync stopped."),

    %% Give keylist pids time to stop
    timer:sleep(500),
    %% Ensure keylist pids are actually gone
    Exits = [catch sys:get_state(Pid) || {Pid, _} <- KeylistPids],
    [?assertMatch({'EXIT', _}, Exit) || Exit <- Exits],

    [{"B", S1}] = rpc:call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({fullsyncs_completed, 0}, S1)),
    ?LOG_INFO("Fullsync not completed."),

    [{"B", S2}] = rpc:call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({in_progress, 0}, S2)),
    ?LOG_INFO("** ~0p", [S2]),

    ?LOG_INFO("Starting fullsync."),
    rt:log_to_nodes(lists:flatten(Nodes), "Starting fullsync."),
    PreCount = fullsync_count(LeaderA),
    R3 = rpc:call(LeaderA, riak_repl_console, fullsync, [["start"]]),
    ?assertEqual(ok, R3),
    repl_util:wait_until_fullsync_started(LeaderA),
    ?LOG_INFO("Fullsync running again."),

    Res =
        rt:wait_until(
            LeaderA,
            fun(_) ->
                    case fullsync_count(LeaderA) of
                        FC when FC > PreCount ->
                            true;
                        _ ->
                            false
                    end
            end
        ),
    ?assertEqual(ok, Res),
    repl_util:read_from_cluster(BFirst, 1, ?NUM_KEYS, ?TEST_BUCKET, 0),
    [{"B", S3}] = rpc:call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({fullsyncs_completed, 1}, S3)),
    ?LOG_INFO("Fullsync Complete"),

    rt:log_to_nodes(lists:flatten(Nodes), "Test completed."),
    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    pass.

fullsync_count(Node) ->
    Status =
        rpc:call(
            Node, riak_repl_console, status, [quiet]
        ),
    proplists:get_value(server_fullsyncs, Status).