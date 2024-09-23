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
-module(replication2).
-behavior(riak_test).

-export([confirm/0]).

%% shared
-export([replication/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(rt, [
    join/2,
    log_to_nodes/2,
    log_to_nodes/3,
    wait_until_nodes_ready/1,
    wait_until_no_pending_changes/1
]).

confirm() ->

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    Conf = [
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
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {diff_batch_size, 10}
             ]}
    ],

    ?LOG_INFO("Building Clusters A and B"),
    [ANodes, BNodes] = rt:build_clusters([{ClusterASize, Conf}, {NumNodes - ClusterASize, Conf}]),

    %?LOG_INFO("Skipping all tests"),
    replication(ANodes, BNodes, false),
    pass.


replication(ANodes, BNodes, Connected) ->

    log_to_nodes(ANodes ++ BNodes, "Starting replication2 test"),
    ?LOG_INFO("Connection Status: ~0p", [Connected]),

    ?LOG_INFO("Real Time Full Sync Replication test"),
    real_time_replication_test(ANodes, BNodes, Connected),

    ?LOG_INFO("Disconnected cluster Full Sync test"),
    disconnected_cluster_fsync_test(ANodes, BNodes),

    ?LOG_INFO("Failover tests"),
    master_failover_test(ANodes, BNodes),

    ?LOG_INFO("Network Partition test"),
    network_partition_test(ANodes, BNodes),

    ?LOG_INFO("Bucket Sync tests"),
    bucket_sync_test(ANodes, BNodes),

    ?LOG_INFO("Offline queueing tests"),
    offline_queueing_tests(ANodes, BNodes),

    ?LOG_INFO("Protocol Buffer writes during shutdown test"),
    pb_write_during_shutdown(ANodes, BNodes),

    ?LOG_INFO("HTTP writes during shutdown test"),
    http_write_during_shutdown(ANodes, BNodes),

    ?LOG_INFO("Tests passed"),

    fin.


%% @doc Real time replication test
%%      Test Cycle:
%%        Write some keys with full sync disabled.
%%        Write some keys with full sync enabled.
%%        Check for keys written prior to full sync.
%%        Check all keys.
%%        Delete some keys.
%%        Verify the deletions are propagated.
real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-real_time_replication_test">>,

    case Connected of
        false ->
            %% Before connecting clusters, write some initial data to Cluster A
            ?LOG_INFO("Writing 100 keys to ~0p", [AFirst]),
            ?assertEqual([], repl_util:do_write(AFirst, 1, 100, TestBucket, 2)),

            repl_util:name_cluster(AFirst, "A"),
            repl_util:name_cluster(BFirst, "B"),

            %% Wait for Cluster naming to converge.
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes),

            ?LOG_INFO("Waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            ?LOG_INFO("Waiting for leader to converge on cluster B"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

            %% Get the leader for the first cluster.
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []), %% Ask Cluster "A" Node 1 who the leader is.

            {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),

            ?LOG_INFO("connect cluster A:~0p to B on port ~b", [LeaderA, BFirstPort]),
            repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
            ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

            repl_util:enable_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:start_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:enable_fullsync(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes);

            %% Cluster "A" and Cluster "B" are now connected with real time and full sync enabled.
        _ ->
            ?LOG_INFO("Clusters should already be connected"),
            ?LOG_INFO("Waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            ?LOG_INFO("Waiting for leader to converge on cluster B"),

            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

            ?LOG_INFO("Leader on cluster A is ~0p", [LeaderA]),
            ?LOG_INFO("BFirst on cluster B is ~0p", [BFirst]),
            {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),
            ?LOG_INFO("B is ~0p with port ~b", [BFirst, BFirstPort])
    end,

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify replication to Cluster B via realtime"),
    ?LOG_INFO("Writing 100 keys to Cluster A-LeaderNode: ~0p", [LeaderA]), % This export from Case isn't my favorite.
    ?assertEqual([], repl_util:do_write(LeaderA, 101, 200, TestBucket, 2)),

    ?LOG_INFO("Reading 100 keys written to Cluster A-LeaderNode: ~0p from Cluster B-Node: ~0p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    case Connected of
        false ->
            %% Check that the keys we wrote initially aren't replicated yet as
            %% fullsync_on_connect is disabled.
            ?LOG_INFO("Check keys written before repl was connected are not present"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            log_to_nodes(ANodes++BNodes, "Test fullsync with leader ~0p", [LeaderA]),
            repl_util:start_and_wait_until_fullsync_complete(LeaderA),

            case rpc:call(LeaderA, init, script_id, []) of
                {"riak", Vsn} when Vsn > "1.4" ->
                    %% check that the number of successful FS source exists matches the number of partitions
                    NumExits = repl_util:get_fs_coord_status_item(LeaderA, "B", successful_exits),
                    NumPartitions = repl_util:num_partitions(LeaderA),
                    ?assertEqual(NumPartitions, NumExits);
                _ ->
                    ok
            end,

            ?LOG_INFO("Check keys written before repl was connected are present"),
            ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 200, TestBucket, 2));
        _ ->
            ok
    end,

    log_to_nodes(ANodes ++ BNodes, "Delete data from Cluster A, verify replication to Cluster B via realtime"),
    ?LOG_INFO("Deleting 100 keys on Cluster A"),

    ?assertEqual([], rt:systest_delete(LeaderA, 101, 200, TestBucket, 2)),
    ?assertEqual(0, repl_util:wait_for_all_notfound(BFirst, 101, 200, TestBucket, 2)).

%% @doc Disconnected Clusters Full Sync Test
%%      Test Cycle:
%%        Disconnect Clusters "A" and "B".
%%        Write 2000 keys to Cluster "A".
%%        Reconnect Clusters "A" and "B" and enable real time and full sync.
%%        Read 2000 keys from Cluster "B".
disconnected_cluster_fsync_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-discon_fsync_replication_tests">>,
    {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env,[riak_core, cluster_mgr]),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Starting disconnected full sync test"),

    %% Disconnect Cluster B to disablereal time sync
    ?LOG_INFO("Disconnect the 2 clusters"),
    repl_util:disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:disconnect_cluster(LeaderA, "B"),
    repl_util:wait_until_no_connection(LeaderA),
    rt:wait_until_ring_converged(ANodes),

    ?LOG_INFO("Write 2000 keys"),
    ?assertEqual([], repl_util:do_write(LeaderA, 50000, 52000, TestBucket, 2)),

    ?LOG_INFO("Reconnect the 2 clusters"),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    ?LOG_INFO("Read 2000 keys"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 50000, 52000, TestBucket, 2)).


%% @doc Master Failover Test
%%      Test Cycle:
%%        Stop Cluster "A" leader.
%%        Get new Cluster "A" leader.
%%        Write 100 keys to new Cluster "A" leader.
%%        Verify 100 keys are replicated to Cluster "B".
%%        Get Cluster "B" leader and shut it down.
%%        Get new Cluster "B" leader.
%%        Write 100 keys to Cluster "A".
%%        Verify 100 keys are replicated to Cluster "B".
master_failover_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-master_failover_test">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes , "Failover tests"),
    log_to_nodes(ANodes ++ BNodes, "Testing master failover: stopping ~0p", [LeaderA]),

    ?LOG_INFO("Testing master failover: stopping ~0p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    ASecond = hd(ANodes -- [LeaderA]),
    repl_util:wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_core_cluster_mgr, get_leader, []),

    ?LOG_INFO("New leader is ~0p", [LeaderA2]),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA2)),

    %% ASecond is all nodes w/o LeaderA
    ?LOG_INFO("Writing 100 more keys to ~0p now that the old leader is down", [ASecond]),
    ?assertEqual([], repl_util:do_write(ASecond, 201, 300, TestBucket, 2)),

    %% Verify data is replicated to Cluster "B"
    ?LOG_INFO("Reading 100 keys written to ~0p from ~0p", [ASecond, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 201, 300, TestBucket, 2)),

    %% Get the leader for Cluster "B"
    LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Testing client failover: stopping ~0p", [LeaderB]),

    ?LOG_INFO("Testing client failover: stopping ~0p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    repl_util:wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_core_cluster_mgr, get_leader, []),

    ?LOG_INFO("New leader is ~0p", [LeaderB2]),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA2)),

    ?LOG_INFO("Writing 100 more keys to ~0p now that the old leader is down", [ASecond]),
    ?assertEqual([], repl_util:do_write(ASecond, 301, 400, TestBucket, 2)),

    %% Verify data is replicated to Cluster B
    ?LOG_INFO("Reading 101 keys written to ~0p from ~0p", [ASecond, BSecond]),
    ?assertEqual(0, repl_util:wait_for_reads(BSecond, 301, 400, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Test fullsync with ~0p and ~0p down", [LeaderA, LeaderB]),
    ?LOG_INFO("Re-running fullsync with ~0p and ~0p down", [LeaderA, LeaderB]),

    repl_util:start_and_wait_until_fullsync_complete(LeaderA2),

    %% This says test full sync, but there's never a verification step.
    log_to_nodes(ANodes ++ BNodes, "Test fullsync after restarting ~0p", [LeaderA]),

    %% Put everything back to 'normal'.
    ?LOG_INFO("Nodes restarted"),
    ?LOG_INFO("Restarting down nodes ~0p, ~0p", [LeaderA, LeaderB]),
    rt:start(LeaderA),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderA),
    rt:wait_until_pingable(LeaderB),
    rt:wait_for_service(LeaderA, [riak_kv, riak_repl]),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA2).


%% @doc Network Partition Test
%%      Test Cycle:
%%        Connect the Cluster "A" leader and swap its cookie for a new cookie.
%%        Disconnect node with new cookie from the rest of the nodes.
%%        Nodes will elect a new leader.
%%        Reset cookie on disconnected node and reconnct.
%%        Write 2 keys to node that was reconnected.
%%        Verify replication of keys to Cluster "B".
network_partition_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-network_partition_test">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Starting network partition test"),

    %% @todo add stuff
    %% At this point, realtime sync should still work, but, it doesn't because of a bug in 1.2.1
    %% Check that repl leader is LeaderA
    %% Check that LeaderA2 has ceeded socket back to LeaderA

    %%?LOG_INFO("Leader: ~0p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    %%?LOG_INFO("LeaderA: ~0p", [LeaderA]),
    %%?LOG_INFO("LeaderA2: ~0p", [LeaderA2]),

    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    PInfo = rt:partition([LeaderA], ANodes -- [LeaderA]),

    repl_util:wait_until_new_leader(hd(ANodes -- [LeaderA]), LeaderA),
    InterimLeader = rpc:call(LeaderA, riak_core_cluster_mgr, get_leader, []),
    ?LOG_INFO("Interim leader: ~0p", [InterimLeader]),

    rt:heal(PInfo),

    %% There's no point in writing anything until the leaders converge, as we
    %% can drop writes in the middle of an election
    repl_util:wait_until_leader_converge(ANodes),

    ASecond = hd(ANodes -- [LeaderA]),

    ?LOG_INFO("Leader: ~0p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    ?LOG_INFO("Writing 2 more keys to ~0p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 1301, 1302, TestBucket, 2)),

    %% Verify data is replicated to Cluster "B"
    ?LOG_INFO("Reading 2 keys written to ~0p from ~0p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1301, 1302, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Completed network partition test").


%% @doc Bucket Sync Test
%%      Test Cycle:
%%        Make 3 buckets on Cluster "A":
%%          -No Replication
%%          -Real Time only
%%          -Full Sync only
%%        Disable real time replication and disconnect Cluster "A" and Cluster "B"
%%        Write 100 keys to real time only bucket.
%%        Reconnect and reenable real time and full sync between Cluster "A" and Cluster "B"
%%        Write 100 keys to the No Replication bucket
%%        Write 100 keys to the Full Sync only bucket
%%        Verify that Full sync didn't replicate
%%        Verify that real time bucket written to offline didn't replicate
%%        Verify that the No Replication bucket didn't replicate
%%        Restart full sync
%%        Verify full sync bucket replicated
%%        Verify that the Real time keys replicated
%%        Verify that the original real time keys did not replicate
%%        Verify that the No replication bucket didn't replicate.
bucket_sync_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    NoRepl = <<TestHash/binary, "-no_replication">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),

    log_to_nodes(ANodes ++ BNodes, "Starting bucket sync test"),

    replication:make_bucket(ANodes, NoRepl, [{repl, false}]),
    replication:make_bucket(ANodes, RealtimeOnly, [{repl, realtime}]),
    replication:make_bucket(ANodes, FullsyncOnly, [{repl, fullsync}]),

    %% Disable real time and disconnect Cluster "B" to prevent real time replication.
    ?LOG_INFO("Disconnect the 2 clusters"),
    repl_util:disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:disconnect_cluster(LeaderA, "B"),
    repl_util:wait_until_no_connection(LeaderA),
    rt:wait_until_ring_converged(ANodes),

    ?LOG_INFO("Write 100 keys to a real time only bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, RealtimeOnly, 2)),

    ?LOG_INFO("Reconnect Clusters A and B"),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    log_to_nodes(ANodes ++ BNodes, "Test fullsync and realtime independence"),

    ?LOG_INFO("write 100 keys to a {repl, false} bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, NoRepl, 2)),

    ?LOG_INFO("write 100 keys to a fullsync only bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, FullsyncOnly, 2)),

    ?LOG_INFO("Check the fullsync only bucket didn't replicate the writes"),
    Res6 = rt:systest_read(BFirst, 1, 100, FullsyncOnly, 2),
    ?assertEqual(100, length(Res6)),

    ?LOG_INFO("Check the realtime only bucket that was written to offline isn't replicated"),
    Res7 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res7)),

    ?LOG_INFO("Check the {repl, false} bucket didn't replicate"),
    Res8 = rt:systest_read(BFirst, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res8)),

    %% Do a fullsync, make sure that fullsync_only is replicated, but
    %% realtime_only and no_repl aren't
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    ?LOG_INFO("Check fullsync only bucket is now replicated"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 100, FullsyncOnly, 2)),

    ?LOG_INFO("Check realtime only bucket didn't replicate"),
    Res10 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res10)),

    ?LOG_INFO("Write 100 more keys into realtime only bucket on ~0p", [AFirst]),
    ?assertEqual([], repl_util:do_write(AFirst, 101, 200, RealtimeOnly, 2)),

    ?LOG_INFO("Check the realtime keys replicated"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, RealtimeOnly, 2)),

    ?LOG_INFO("Check the older keys in the realtime bucket did not replicate"),
    Res12 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res12)),

    ?LOG_INFO("Check {repl, false} bucket didn't replicate"),
    Res13 = rt:systest_read(BFirst, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res13)).


%% @doc Offline Queuing Test
%%      Test Cycle:
%%        Stop real time on Cluster "A"
%%        Write 100 keys to leader on Cluster "A"
%%        Restart real time on Cluster "A"
%%        Verify Keys replicated to Cluster "B" after real time was restarted
%%        Stop real time on Cluster "A"
%%        Verify that 100 keys are NOT on Cluster "A"
%%        Write 100 keys to Cluster "A"
%%        Verify that 100 keys were written to Cluster "A"
%%        Verify that 100 keys are NOT on Cluster "B"
%%        Re-enable real time on Cluster "A"
%%        Verify that 100 keys are available on Cluster "B"
offline_queueing_tests([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-offline_queueing_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing offline realtime queueing"),
    ?LOG_INFO("Testing offline realtime queueing"),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    ?LOG_INFO("Stopping realtime, queue will build"),
    repl_util:stop_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    ?LOG_INFO("Writing 100 keys"),
    ?assertEqual([], repl_util:do_write(LeaderA, 800, 900, TestBucket, 2)),

    ?LOG_INFO("Starting realtime"),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    ?LOG_INFO("Reading keys written while repl was stopped"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 800, 900, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Testing realtime migration on node shutdown"),
    ?LOG_INFO("Testing realtime migration on node shutdown"),
    Target = hd(ANodes -- [LeaderA]),

    ?LOG_INFO("Stopping realtime, queue will build"),
    repl_util:stop_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    ?LOG_INFO("Verifying 100 keys are missing from ~0p", [Target]),
    repl_util:read_from_cluster(Target, 901, 1000, TestBucket, 100),

    ?LOG_INFO("Writing 100 keys to ~0p", [Target]),
    ?assertEqual([], repl_util:do_write(Target, 901, 1000, TestBucket, 2)),

    ?LOG_INFO("Verifying 100 keys are read from ~0p", [Target]),
    repl_util:read_from_cluster(Target, 901, 1000, TestBucket, 0),

    ?LOG_INFO("Verifying 100 keys are missing from ~0p", [BFirst]),
    repl_util:read_from_cluster(BFirst, 901, 1000, TestBucket, 100),

    ?LOG_INFO("queue status: ~0p", [rpc:call(Target, riak_repl2_rtq, status, [])]),

    ?LOG_INFO("Stopping node ~0p", [Target]),

    rt:stop(Target),
    rt:wait_until_unpingable(Target),

    ?LOG_INFO("Starting realtime"),
    repl_util:start_realtime(LeaderA, "B"),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    ?LOG_INFO("Verifying 100 keys are now available on ~0p", [BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 901, 1000, TestBucket, 2)),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl).


%% @doc Protocol Buffer Write During Shutdown
%%      Test Cycle:
%%        Connect to Cluster "A" via PB
%%        Spawn background process to stop Cluster "A" nodes
%%        Write 10,000 keys to Cluster "A"
%%        Verify that there are and equal number of write failures on Cluster "A" and read failures on Cluster "B"
pb_write_during_shutdown([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-pb_write_shutdown_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing protocol buffer writes during shutdown"),

    LeaderA = rpc:call( AFirst, riak_core_cluster_mgr, get_leader, []),
    Target = hd( ANodes -- [LeaderA]),

    ConnInfo = proplists:get_value(Target, rt:connection_info([Target])),
    {IP, Port} = proplists:get_value(pb, ConnInfo),
    ?LOG_INFO("Connecting to pb socket ~0p:~b on ~0p", [IP, Port, Target]),
    PBSock = rt:pbc(Target),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
              timer:sleep(500),
              ?LOG_INFO("Stopping node ~0p again", [Target]),
              rt:stop(Target),
              ?LOG_INFO("Node stopped")
          end),

    ?LOG_INFO("Writing 10,000 keys"),
    WriteErrors =
        try
            pb_write(PBSock, 1000, 11000, TestBucket, 2)
        catch
      _:_ ->
          ?LOG_INFO("Shutdown timeout caught"),
          []
      end,
    ?LOG_INFO("Received ~b write failures", [length(WriteErrors)]),
    timer:sleep(3000),
    ?LOG_INFO("Checking number of read failures on secondary cluster"),
    ReadErrors = rt:systest_read(BFirst, 1000, 11000, TestBucket, 2),
    ?LOG_INFO("Received ~b read failures", [length(ReadErrors)]),

    %% Ensure node is down before we try to start it up again.
    ?LOG_INFO("pb_write_during_shutdown: Ensure node ~0p is down before restart", [Target]),
    ?assertEqual(ok, rt:wait_until_unpingable(Target)),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl),
    ReadErrors2 = rt:systest_read(Target, 1000, 11000, TestBucket, 2),
    ?LOG_INFO("Received ~b read failures on ~0p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            ?LOG_ERROR("Received more read errors on ~0p: ~b than write errors on ~0p: ~b",
            [BFirst, length(ReadErrors), Target, length(WriteErrors)]),
            FailedKeys = lists:foldl(
                fun({Key, _}, Acc) ->
                    case lists:keyfind(Key, 1, WriteErrors) of
                        false ->
                            [Key|Acc];
                        _ ->
                            Acc
                    end
                end, [], ReadErrors),
            ?LOG_INFO("Failed keys ~0p", [FailedKeys]),
            ?LOG_INFO("Validate number of read failures on secondary cluster"),
            StableReadErrorCount =
                get_read_errors_until_stable(
                    BFirst, TestBucket, 1000, 11000, length(ReadErrors)
                ),
            case length(WriteErrors) >= StableReadErrorCount of
                true ->
                    ?LOG_INFO(
                        "Discrepancy appears to be timing, checking again"),
                    ?assertMatch(
                        StableReadErrorCount,
                        length(
                            rt:systest_read(
                                BFirst, 1000, 11000, TestBucket, 2))
                        ),
                    ?LOG_INFO("Check complete");
                false ->
                    ?assert(false)
            end
    end.


get_read_errors_until_stable(Node, Bucket, Start, End, LastReading) ->
    timer:sleep(100),
    ReadErrors = rt:systest_read(Node, Start, End, Bucket, 2),
    case length(ReadErrors) of
        R when R < LastReading->
            ?LOG_INFO("Received ~b read failures - retry", [R]),
            get_read_errors_until_stable(
                Node, Bucket, Start, End, length(ReadErrors)
            );
        R ->
            ?LOG_INFO("Received ~b read failures", [R]),
            length(ReadErrors)
    end.


%% @doc HTTP Write during Shutdown
%%      Test Cycle:
%%        Connect to Cluster "A" via HTTP
%%        Spawn background process to stop Cluster "A" nodes
%%        Write 10,000 keys to Cluster "A"
%%        Verify that there are and equal number of write failures on Cluster "A" and read failures on Cluster "B"
http_write_during_shutdown([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-http_write_shutdown_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing http writes during shutdown"),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    Target = hd(ANodes -- [LeaderA]),

    ConnInfo = proplists:get_value(Target, rt:connection_info([Target])),
    {IP, Port} = proplists:get_value(http, ConnInfo),
    ?LOG_INFO("Connecting to http socket ~0p:~b on ~0p", [IP, Port, Target]),
    C = rt:httpc(Target),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
              timer:sleep(500),
              ?LOG_INFO("Stopping node ~0p again", [Target]),
              rt:stop(Target),
              ?LOG_INFO("Node stopped")
          end),

    ?LOG_INFO("Writing 10,000 keys"),
    WriteErrors =
        try
            http_write(C, 12000, 22000, TestBucket, 2)
        catch
            _:_ ->
                ?LOG_INFO("Shutdown timeout caught"),
                []
        end,
    ?LOG_INFO("got ~b write failures to ~0p", [length(WriteErrors), Target]),
    timer:sleep(3000),
    ?LOG_INFO("Checking number of read failures on secondary cluster node, ~0p", [BFirst]),
    [{_, {IP, Port2}},_] = rt:connection_info(BFirst),
    C2 = rhc:create("127.0.0.1", Port2, "riak", []),
    ReadErrors = http_read(C2, 12000, 22000, TestBucket, 2),
    ?LOG_INFO("Received ~b read failures from ~0p", [length(ReadErrors), BFirst]),

    %% Ensure node is down before we try to start it up again.
    ?LOG_INFO("HTTP: write_during_shutdown: Ensure node ~0p is down before restart", [Target]),
    ?assertEqual(ok, rt:wait_until_unpingable(Target)),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl),
    ReadErrors2 = http_read(C, 12000, 22000, TestBucket, 2),
    ?LOG_INFO("Received ~b read failures on ~0p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            ?LOG_ERROR("Received more read errors on ~0p: ~b than write errors on ~0p: ~b",
            [BFirst, length(ReadErrors), Target, length(WriteErrors)]),
            FailedKeys = lists:foldl(fun({Key, _}, Acc) ->
                case lists:keyfind(Key, 1, WriteErrors) of
                    false ->
                        [Key|Acc];
                    _ ->
                        Acc
                end
            end, [], ReadErrors),
        ?LOG_INFO("Failed keys ~0p", [FailedKeys]),
        ?assert(false)
    end.


client_iterate(_Sock, [], _Bucket, _W, Acc, _Fun, Parent) ->
    Parent ! {result, self(), Acc},
    Acc;


client_iterate(Sock, [N | NS], Bucket, W, Acc, Fun, Parent) ->
    NewAcc = try Fun(Sock, Bucket, N, W) of
        ok ->
            Acc;
        Other ->
            [{N, Other} | Acc]
    catch
        What:Why ->
            [{N, {What, Why}} | Acc]
    end,
    client_iterate(Sock, NS, Bucket, W, NewAcc, Fun, Parent).


http_write(Sock, Start, End, Bucket, W) ->
    F = fun(S, B, K, WVal) ->
            X = list_to_binary(integer_to_list(K)),
            Obj = riakc_obj:new(B, X, X),
            rhc:put(S, Obj, [{dw, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F, Parent) end) || K <- Partitions],
    collect_results(Workers, []).


pb_write(Sock, Start, End, Bucket, W) ->
    F = fun(S, B, K, WVal) ->
        Obj = riakc_obj:new(B, <<K:32/integer>>, <<K:32/integer>>),
        riakc_pb_socket:put(S, Obj, [{dw, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F, Parent) end) || K <- Partitions],
    collect_results(Workers, []).


http_read(Sock, Start, End, Bucket, R) ->
    F = fun(S, B, K, RVal) ->
        X = list_to_binary(integer_to_list(K)),
        case rhc:get(S, B, X, [{r, RVal}]) of
            {ok, _} ->
                ok;
            Error ->
                Error
        end
    end,
    client_iterate(Sock, lists:seq(Start, End), Bucket, R, [], F, self()).


partition_keys(Keys, PC) ->
    partition_keys(Keys, PC, lists:duplicate(PC, [])).


partition_keys([] , _, Acc) ->
    Acc;


partition_keys(Keys, PC, Acc) ->
    In = lists:sublist(Keys, PC),
    Rest = try lists:nthtail(PC, Keys)
        catch _:_ -> []
    end,
    NewAcc = lists:foldl(fun(K, [H|T]) ->
        T ++ [[K|H]]
        end, Acc, In),
    partition_keys(Rest, PC, NewAcc).


collect_results([], Acc) ->
    Acc;


collect_results(Workers, Acc) ->
    receive
        {result, Pid, Res} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Res ++ Acc);
        {'DOWN', _, _, Pid, _Reason} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Acc)
    end.
