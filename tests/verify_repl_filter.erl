%% -------------------------------------------------------------------
%%
%% Copyright (c) 2023 Workday, Inc.
%%
%% -------------------------------------------------------------------
-module(verify_repl_filter).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%
%% Test Objectives:
%% 
%% This test is intended to test filtering of replicated data during fullysync, such that
%% any data that is not intended to be replicated to a sink cluster (via the `repl` bucket property) 
%% is not listed using the fullsync keylisting strategy.  The idea is is to reduce comparisons
%% of data on disk when listing keys for partitions in source and sink clusters.  This is
%% especially important in Workday scenarious, given the relatively large amount of data written
%% into runtime storage classes.
%% 
%% Test Description:
%% 
%% This test creates two clusters, a ?NUM_SOURCE_NODES-node source cluster, and a
%% ?NUM_SINK_NODES-node sink cluster.  Each cluster is set up with two (multi-)
%% bitcask backeds, one that will correspond with a bucket type that is not replicated
%% to the sink cluster (from the source), and one that is.
%% 
%% Two bucket types are create: runtime and application, roughly corresponding to the
%% high level storage classes we use at Workday.  The runtime storage class is configured
%% to not replicate data from the source to sink cluster; wheres the appplication storage
%% class does so replicate.
%% 
%% This test instantiates a listening process, with a globally registered name, which collects
%% information from intercepts (see riak_repl_fullsync_helper_intercepts.erl) that are 
%% installed in each node in the source cluster.
%% 
%% These intercepts intercept the `is_replicated` function call, which is used in a vnode fold operation,
%% when funning a keylisting in fullsyncs.  The intercept will send a message back to the listenting
%% process, indicating whether a given BKey is to be replicated or not.
%% 
%% The test sets up the cluster, and then writes a collection of objects to:
%%   - the runtime bucket
%%   - the application bucket
%%   - the default bucket
%% 
%% The test records which objects have been written to which buckets.
%% 
%% The test then compares the list of Bkeys returned to the listener with the keys it wrote, to
%% verify that all and only the runtime objects have been skipped, and all and only the
%% application and default objects have been replicated.  Because the vnode folds result in
%% many duplicate objects, the lists sre uniquely sorted before comparison, for "set equality".
%% The test passes if these conditions hold.
%% 
%% In addition, we test that all replicated keys can be found in the sink cluster, and that no
%% skipped keys can be found in the sink.
%%

-define(BASE_CONFIG, [
    {riak_core, [
        {ring_creation_size, 8},
        %% turbo mode
        {vnode_inactivity_timeout, 1000},
        {vnode_management_timer, 100},
        {handoff_concurrency, 8},
        %% wd default
        {default_bucket_props, [{notfound_ok, false}]}
    ]},
    {riak_kv, [
        {anti_entropy, {off, []}}
    ]}
]).

-define(NUM_SOURCE_NODES, 1).
-define(NUM_SINK_NODES, 1).
-define(NUM_KEYS, 1000).
-define(SOURCE, "source").
-define(SINK, "sink").
-define(BACKEND_CONFIG, [
    {"storage_backend", "multi"},
    {"multi_backend.runtime.storage_backend", "bitcask"},
    {"multi_backend.runtime.bitcask.data_root", "$(platform_data_dir)/runtime"},
    {"multi_backend.application.storage_backend", "bitcask"},
    {"multi_backend.application.bitcask.data_root", "$(platform_data_dir)/bitcask"},
    {"multi_backend.default", "application"}
]).

-define(RUNTIME_BUCKET_TYPE, <<"runtime">>).
-define(RUNTIME_BUCKET_PROPS, [{backend, <<"runtime">>}, {repl, false}]).
-define(APPLICATION_BUCKET_TYPE, <<"application">>).
-define(APPLICATION_BUCKET_PROPS, [{backend, <<"application">>}]).
-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-record(state, {
    skipped_bkeys = [],
    replicated_bkeys = []
}).


-spec confirm() -> pass | fail.
confirm() ->
    case rt:get_backends() of
        bitcask ->
            ?LOG_INFO("Verified bitcask backend for this test"),
            ok = run_test(),
            ?LOG_INFO("Test ~p passed.", [?MODULE]),
            pass;
        SomethingElse ->
            ?LOG_ERROR("Unexpected backends: ~p", [SomethingElse]),
            fail
    end.

%% @private
run_test() ->
    
    {ok, Listener} = start_listener(),
    try
        {SourceNodes, SinkNodes} = setup_clusters(?BASE_CONFIG),

        ok = add_intercepts(SourceNodes),
        
        RuntimeBKeys = write_entries(SourceNodes, ?RUNTIME_BUCKET_TYPE, ?NUM_KEYS),
        ApplicationBKeys = write_entries(SourceNodes, ?APPLICATION_BUCKET_TYPE, ?NUM_KEYS),
        DefaultBKeys = write_entries(SourceNodes, ?DEFAULT_BUCKET_TYPE, ?NUM_KEYS),

        ok = run_fullsync(get_leader(SourceNodes)),

        State = get_state(Listener),
        ReplicatedBKeys = lists:usort(State#state.replicated_bkeys),
        SkippedBKeys = lists:usort(State#state.skipped_bkeys),

        ?assertEqual(
            ReplicatedBKeys, 
            lists:usort(ApplicationBKeys ++ DefaultBKeys), 
            "Expected application and default keys to be all and only replicated keys"
        ),
        ?assertEqual(
            SkippedBKeys, 
            lists:usort(RuntimeBKeys), 
            "Expected runtime keys to be all and only skipped keys"
        ),

        [SinkNode | _] = SinkNodes,
        verify_exist(SinkNode, ReplicatedBKeys),
        verify_dont_exist(SinkNode, SkippedBKeys),

        ?LOG_INFO("All replicated and skipped BKeys are as expected."),

        ok
    after
        stop_listener(Listener)
    end.

%% @private
add_intercepts(Nodes) ->
    Intercept = {{is_fullsync_replicated, 1}, count_is_fullsync_replicated},
    lists:foreach(
        fun(Node) ->
            ok = rt_intercept:add(Node, {riak_repl_fullsync_helper, [Intercept]})
        end,
        Nodes
    ).

%% @private
setup_clusters(Config) ->
    %%
    %% start the nodes
    %%
    ?LOG_INFO("Starting ~p nodes", [?NUM_SOURCE_NODES + ?NUM_SINK_NODES]),
    rt:set_conf(all, ?BACKEND_CONFIG),
    Nodes = rt:deploy_nodes(?NUM_SOURCE_NODES + ?NUM_SINK_NODES, Config),
    {SourceNodes, SinkNodes} = lists:split(?NUM_SOURCE_NODES, Nodes),
    [SourceNode | _] = SourceNodes,

    %%
    %% Join the source cluster
    %%
    rt:join_cluster(SourceNodes),
    rt:wait_for_cluster_service(SourceNodes, riak_kv),
    rt:wait_until_transfers_complete(SourceNodes),
    
    %%
    %% Join the sink cluster
    %%
    ?LOG_INFO("Building  sink cluster"),
    [SinkNode | _] = SinkNodes,
    rt:join_cluster(SinkNodes),
    rt:wait_for_cluster_service(SinkNodes, riak_kv),
    rt:wait_until_transfers_complete(SinkNodes),

    %%
    %% Create and activate new bucket types in source cluster
    %%
    ?LOG_INFO("Creating and activating new bucket types in source cluster..."),
    create_and_activate_bucket_types(SourceNodes, ?RUNTIME_BUCKET_TYPE, ?RUNTIME_BUCKET_PROPS),
    create_and_activate_bucket_types(SourceNodes, ?APPLICATION_BUCKET_TYPE, ?APPLICATION_BUCKET_PROPS),
    
    %%
    %% Create and activate new bucket types in sink cluster
    %%
    ?LOG_INFO("Creating and activating new bucket types in sink cluster..."),
    create_and_activate_bucket_types(SinkNodes, ?RUNTIME_BUCKET_TYPE, ?RUNTIME_BUCKET_PROPS),
    create_and_activate_bucket_types(SinkNodes, ?APPLICATION_BUCKET_TYPE, ?APPLICATION_BUCKET_PROPS),

    %%
    %% Name two clusters for replication
    %%
    ?LOG_INFO("Naming clusters"),
    repl_util:name_cluster(SourceNode, ?SOURCE),
    repl_util:name_cluster(SinkNode, ?SINK),
    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),
    ok = repl_util:wait_until_leader_converge(SourceNodes),
    ok = repl_util:wait_until_leader_converge(SinkNodes),
    
    %%
    %% Connect the clusters
    %%
    ?LOG_INFO("Connecting source and sink clusters"),
    SourceLeader = rpc:call(SourceNode, riak_core_cluster_mgr, get_leader, []),
    {ok, {_IP, SinkFirstPort}} = rpc:call(SinkNode, application, get_env, [riak_core, cluster_mgr]),
    repl_util:connect_cluster(SourceLeader, "127.0.0.1", SinkFirstPort),
    ?assertEqual(ok, repl_util:wait_for_connection(SourceLeader, ?SINK)),
    
    {SourceNodes, SinkNodes}.

%% @private
create_and_activate_bucket_types(Nodes, BucketType, BucketProperties) ->
    rt:create_and_activate_bucket_type(wd:random_element(Nodes), BucketType, BucketProperties),
    rt:wait_until_bucket_type_status(BucketType, active, Nodes),
    rt:wait_until_bucket_type_visible(Nodes, BucketType),
    rt:wait_until_bucket_props(Nodes, {BucketType, <<"anybucketnamewilldo">>}, BucketProperties).

%% @private
write_entries(Cluster, BucketType, NumEntries) ->
    [Node | _Rest] = Cluster,
    NodeClient = rt:pbc(Node),
    try
        BKeys = [try_put(Node, NodeClient, BucketType,  integer_to_binary(I)) || I <- lists:seq(1, NumEntries)],
        ?LOG_INFO("Wrote ~p keys under BucketType ~p", [NumEntries, BucketType]),
        BKeys
    after
        riakc_pb_socket:stop(NodeClient)
    end.

%% @private
try_put(Node, Client, BucketType, Key) ->
    BucketName = atom_to_binary(?MODULE),
    Bucket = {BucketType, BucketName},
    BKey = {Bucket, Key},
    Value = wd:generate_value(0),
    RObj = riakc_obj:new(
        Bucket, Key, Value, "application/binary"
    ),
    try
        case riakc_pb_socket:put(Client, RObj, [{dw, 2}]) of
            ok ->
                BKey;
            Error ->
                ?LOG_ERROR("Failed to write object to node ~p with error ~p", [Node, Error]),
                undefined
        end
    catch
        _:E ->
            ?LOG_WARNING("Failed to write object to node ~p with error ~p", [Node, E]),
            error(E)
    end.

%% @private
verify_exist(Node, BKeys) ->
    NodeClient = rt:pbc(Node),
    try
        lists:foreach(
            fun(BKey) ->
                ok = try_get(Node, NodeClient, BKey)
            end,
            BKeys
        )
    after
        riakc_pb_socket:stop(NodeClient)
    end.

%% @private
verify_dont_exist(Node, BKeys) ->
    NodeClient = rt:pbc(Node),
    try
        lists:foreach(
            fun(BKey) ->
                {error, notfound} = try_get(Node, NodeClient, BKey)
            end,
            BKeys
        )
    after
        riakc_pb_socket:stop(NodeClient)
    end.

%% @private
try_get(Node, Client, {Bucket, Key} = BKey) ->
    try
        case riakc_pb_socket:get(Client, Bucket, Key, [{r, 1}]) of
            {ok, _RObj} ->
                ok;
            Error ->
                Error
        end
    catch
        _:E ->
            ?LOG_WARNING("Failed to read BKey ~p from node ~p with exception ~p", [BKey, Node, E]),
            error(E)
    end.


%% @private
get_leader([Node | _]) ->
    repl_util:get_leader(Node).

%% @private
run_fullsync(SrcLeader) ->
    repl_util:enable_fullsync(SrcLeader, ?SINK),
    rt:wait_until(
        fun() ->
            ?LOG_INFO("Waiting for fullsync to complete using source leader ~p", [SrcLeader]),
            fullsyncs_completed_status(SrcLeader) > 0
        end
    ),
    ?LOG_INFO("Fullsync complete using source leader ~p", [SrcLeader]),
    ok.

%% @private
coord_status(Node) ->
    StatusResult = rpc:block_call(
        Node, riak_repl2_fscoordinator, status, []),
    ?LOG_DEBUG("COORD STATUS ~p ~p", [Node, StatusResult]),
    StatusResult.

%% @private
fullsyncs_completed_status(Node) ->
    [{_, StatusProps}] = coord_status(Node),
    proplists:get_value(fullsyncs_completed, StatusProps).

%%
%% listenser proc
%% 

%% @private
start_listener() ->
    Pid = spawn_opt(fun loop/0, [link]),
    yes = global:register_name(?MODULE, Pid),
    {ok, Pid}.

%% @private
stop_listener(Pid) ->
    Pid ! halt.

%% @private
get_state(Pid) ->
    Ref = erlang:make_ref(),
    Pid ! {get_state, Ref, self()},
    receive
        {Ref, State} ->
            State
    end.

%% @private
loop() ->
    loop(#state{}).

%% @private
loop(State) ->
    receive
        halt ->
            ok;
        {is_fullsync_replicated, {Pid, Ref}, BKey, IsReplicated} ->
            case IsReplicated of
                true ->
                    Pid ! {Ref, ok},
                    loop(State#state{replicated_bkeys = [BKey | State#state.replicated_bkeys]});
                _ ->
                    Pid ! {Ref, ok},
                    loop(State#state{skipped_bkeys = [BKey | State#state.skipped_bkeys]})
            end;
        {get_state, Ref, Pid} ->
            Pid ! {Ref, State},
            loop(State)
    end.
