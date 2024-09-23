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
-module(repl_consistent_object_filter).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Test to verify that replication properly filters consistent bucket
%% types. This is intended to be a temporary state of affairs so this
%% test should have a limited life span.
%%
%% Currently this test only exercises fullsync replication. The write
%% path for consistent objects bypasses the riak_kv postcommit hooks
%% that are the mechanism by which realtime replication works. As a
%% result, no attempt is ever made to replicate consistent objects.

%% @doc riak_test entry point
confirm() ->
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, BNodes} = make_clusters(),

    PBA = get_pb_pid(LeaderA),
    PBB = get_pb_pid(LeaderB),

    BucketType = <<"consistent_type">>,

    %% Create consistent bucket type on cluster A
    rt:create_and_activate_bucket_type(LeaderA,
                                       BucketType,
                                       [{consistent, true}, {n_val, 5}]),
    rt:wait_until_bucket_type_status(BucketType, active, ANodes),
    rt:wait_until_bucket_type_visible(ANodes, BucketType),

    %% Create consistent bucket type on cluster B
    rt:create_and_activate_bucket_type(LeaderB,
                                       BucketType,
                                       [{consistent, true}, {n_val, 5}]),
    rt:wait_until_bucket_type_status(BucketType, active, BNodes),
    rt:wait_until_bucket_type_visible(BNodes, BucketType),

    connect_clusters(LeaderA, LeaderB),

    %% Create two riak objects and execute consistent put of those
    %% objects
    Bucket = <<"unclebucket">>,
    Key1 = <<"Maizy">>,
    Key2 = <<"Miles">>,
    Bin1 = <<"Take this quarter, go downtown, and have a rat gnaw that thing off your face! Good day to you, madam.">>,
    Bin2 = <<"My Uncle was micro waving our socks and the dog threw up on the couch for an hour.">>,
    Obj1 = riakc_obj:new({BucketType, Bucket}, Key1, Bin1),
    Obj2 = riakc_obj:new({BucketType, Bucket}, Key2, Bin2),
    ?LOG_INFO("doing 2 consistent puts on A, bucket:~0p", [Bucket]),
    ok = riakc_pb_socket:put(PBA, Obj1),
    ok = riakc_pb_socket:put(PBA, Obj2),

    %% Enable fullsync and wait for it to complete
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    {Time, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),
    ?LOG_INFO("Fullsync completed in ~w seconds", [Time/1000/1000]),

    %% Attempt to read the objects from cluster B to verify they have
    %% not been replicated via realtime replication
    BReadRes3 = riakc_pb_socket:get(PBB, {BucketType, Bucket}, Key1),
    BReadRes4 = riakc_pb_socket:get(PBB, {BucketType, Bucket}, Key2),

    ?assertEqual({error, notfound}, BReadRes3),
    ?assertEqual({error, notfound}, BReadRes4),

    riakc_pb_socket:stop(PBA),
    riakc_pb_socket:stop(PBB),
    pass.

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    ?LOG_INFO("connect cluster A:~0p to B on port ~0p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

%% @doc Create two clusters of 1 node each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_clusters() ->
    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),
    NVal = 5,

    ?LOG_INFO("Deploy ~0p nodes", [NumNodes]),
    Conf = ensemble_util:fast_config(NVal) ++
        [
         {riak_repl,
          [
           %% turn off fullsync
           {fullsync_on_connect, false},
           {max_fssource_node, 2},
           {max_fssink_node, 2},
           {max_fssource_cluster, 5},
           {max_fssource_retries, 5}
          ]}
        ],

    Nodes = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),
    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    ensemble_util:wait_until_stable(AFirst, NVal),
    ensemble_util:wait_until_stable(BFirst, NVal),

    %% get the leader for the first cluster
    ?LOG_INFO("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),

    %% get the leader for the second cluster
    ?LOG_INFO("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    %% Name the clusters
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    ?assertEqual(true, rpc:call(AFirst, riak_ensemble_manager, enabled, [])),
    ensemble_util:wait_until_cluster(ANodes),
    ensemble_util:wait_for_membership(AFirst),
    ensemble_util:wait_until_stable(AFirst, NVal),

    ?assertEqual(true, rpc:call(BFirst, riak_ensemble_manager, enabled, [])),
    ensemble_util:wait_until_cluster(BNodes),
    ensemble_util:wait_for_membership(BFirst),
    ensemble_util:wait_until_stable(BFirst, NVal),

    LeaderA = repl_util:get_leader(AFirst),
    LeaderB = repl_util:get_leader(BFirst),

    {LeaderA, LeaderB, ANodes, BNodes}.

get_pb_pid(Leader) ->
    {ok, [{IP, PortA}] } = rpc:call(Leader, application, get_env, [riak_api, pb]),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PortA, []),
    Pid.
