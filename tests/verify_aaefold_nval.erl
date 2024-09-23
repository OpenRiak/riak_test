%% -------------------------------------------------------------------
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
%% @doc Verification of AAE fold based on n_val (with cached trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store
-module(verify_aaefold_nval).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(REBUILD_TICK, 30 * 1000).
-define(CFG_NOREBUILD(NVal),
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{n_val, NVal}]}
          ]}]
       ).
-define(CFG_REBUILD(NVal),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 0},
           {tictacaae_rebuilddelay, 60},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, ?REBUILD_TICK} % Check for rebuilds!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{n_val, NVal}]}
          ]}]
       ).
-define(NUM_NODES, 3).
-define(NUM_KEYS_NVAL3_PERNODE, 8000).
-define(NUM_KEYS_NVAL2_PERNODE, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(NVAL_TYPE, <<"nval2_type">>).
-define(NVAL_BUCKET, <<"nval2_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(?N_VAL)),
    ok = verify_aae_fold(Nodes1),
    rt:clean_cluster(Nodes1),

    [Node] = rt:build_cluster(1, ?CFG_NOREBUILD(1)),
    ok = verify_aae_rebuildfold(Node),

    rt:clean_cluster([Node]),

    Nodes2 = rt:build_cluster(?NUM_NODES, ?CFG_REBUILD(?N_VAL)),
    ?LOG_INFO("Sleeping for twice rebuild tick - testing with rebuilds ongoing"),
    timer:sleep(2 * ?REBUILD_TICK),
    ok = verify_aae_fold(Nodes2),
    pass.


verify_aae_fold(Nodes) ->

    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CT} = riak:client_connect(lists:last(Nodes)),

    ?LOG_INFO("Fold for empty root"),
    {ok, RH0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),
    NoDirtyBranches = aae_exchange:compare_roots(RH0, RT0),
    ?LOG_INFO("NoDirtyBranches ~w", [NoDirtyBranches]),

    ?LOG_INFO("Commencing object load"),
    KeyLoadFun =
        fun(Node, KeyCount) ->
            KVs = test_data(KeyCount + 1,
                                KeyCount + ?NUM_KEYS_NVAL3_PERNODE,
                                list_to_binary("U1")),
            ok = write_data(Node, KVs),
            KeyCount + ?NUM_KEYS_NVAL3_PERNODE
        end,

    lists:foldl(KeyLoadFun, 1, Nodes),
    ?LOG_INFO("Loaded ~w objects", [?NUM_KEYS_NVAL3_PERNODE * length(Nodes)]),
    wait_until_root_stable(CH),
    wait_until_root_stable(CT),

    ?LOG_INFO("Fold for busy root"),
    {ok, RH1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),

    ?assertMatch(true, RH1 == RT1),
    ?assertMatch(true, RH0 == RT0),
    ?assertMatch(false, RH0 == RH1),

    ?LOG_INFO("Make ~w changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),
    wait_until_root_stable(CH),

    {ok, RH2} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches2 = aae_exchange:compare_roots(RH1, RH2),

    ?LOG_INFO("Found ~w branch deltas", [length(DirtyBranches2)]),
    ?assertMatch(true, length(DirtyBranches2) > 0),
    ?assertMatch(true, length(DirtyBranches2) =< ?DELTA_COUNT),

    {ok, BH2} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches2}, CH),

    ?LOG_INFO("Make ~w changes to same keys", [?DELTA_COUNT]),
    Changes3 = test_data(1, ?DELTA_COUNT, list_to_binary("U3")),
    ok = write_data(hd(Nodes), Changes3),
    wait_until_root_stable(CH),

    {ok, RH3} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches3 = aae_exchange:compare_roots(RH2, RH3),

    ?LOG_INFO("Found ~w branch deltas", [length(DirtyBranches3)]),
    ?assertMatch(true, DirtyBranches2 == DirtyBranches3),

    {ok, BH3} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),

    DirtySegments1 = aae_exchange:compare_branches(BH2, BH3),
    ?LOG_INFO("Found ~w mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(true, length(DirtySegments1) > 0),
    ?assertMatch(true, length(DirtySegments1) =< ?DELTA_COUNT),

    {ok, KCL1} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),

    ?LOG_INFO("Found ~w mismatched keys", [length(KCL1)]),

    ?assertMatch(true, length(KCL1) >= ?DELTA_COUNT),
    MappedKCL1 = lists:map(fun({B, K, VC}) -> {{B, K}, VC} end, KCL1),

    ?LOG_INFO("Checking all mismatched keys in result"),
    MatchFun =
        fun(I) ->
            K = to_key(I),
            InFetchClocks = lists:keyfind({?BUCKET, K}, 1, MappedKCL1),
            ?assertMatch(true, {?BUCKET, K} == element(1, InFetchClocks))
        end,
    lists:foreach(MatchFun, lists:seq(1, ?DELTA_COUNT)),

    ?LOG_INFO("Create bucket type with nval of 2"),
    rt:create_and_activate_bucket_type(hd(Nodes), ?NVAL_TYPE, [{n_val, 2}]),
    rt:wait_until_bucket_type_status(?NVAL_TYPE, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {?NVAL_TYPE, ?NVAL_BUCKET}, [{n_val, 2}]),

    ?LOG_INFO("Stop and start nodes to notice new preflists"),
    lists:foreach(fun(N) -> rt:stop_and_wait(N) end, Nodes),
    lists:foreach(fun(N) -> rt:start_and_wait(N) end, Nodes),
    rt:wait_until_all_members(Nodes),
    rt:wait_for_cluster_service(Nodes, riak_kv),

    ?LOG_INFO("Check fetch_clocks for n_val 3"),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?LOG_INFO("Check fetch root for n_val 2 is empty root"),
    {ok, RH0N2} = riak_client:aae_fold({merge_root_nval, 2}, CH),
    ?assertMatch(KCL1, KCL2),
    ?assertMatch(RH0, RH0N2),
    {ok, RB0N2} =
        riak_client:aae_fold({merge_branch_nval, 2, lists:seq(1, 128)}, CH),

    ?LOG_INFO("Write values to nval=2 bucket"),
    KVs =
        test_data(1,
                    ?NUM_NODES * ?NUM_KEYS_NVAL2_PERNODE,
                    list_to_binary("NV2")),
    ok = write_data({?NVAL_TYPE, ?NVAL_BUCKET}, hd(Nodes), KVs, []),
    ?LOG_INFO("Check fetch root for n_val 2 is not empty"),
    {ok, RH1N2} = riak_client:aae_fold({merge_root_nval, 2}, CH),
    ?assertMatch(false, RH0N2 == RH1N2),

    ?LOG_INFO("Check fetch_clocks for n_val=3 unchanged"),
    {ok, KCL3} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?assertMatch(KCL2, KCL3),
    ?LOG_INFO("Check root for n_val=3 unchanged"),
    {ok, RH4} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    ?assertMatch(RH3, RH4),

    ?LOG_INFO("Fetch clocks for nval=2 bucket"),
    {ok, RB1N2} =
        riak_client:aae_fold({merge_branch_nval, 2, lists:seq(1, 64)}, CH),
    DirtySegmentsN2 = aae_exchange:compare_branches(RB1N2, RB0N2),
    ?LOG_INFO("Dirty segments ~w", [length(DirtySegmentsN2)]),
    ?LOG_INFO("Confirm only nval=2 keys returned from fetch_clocks nval2"),
    {ok, KCL0N2} =
        riak_client:aae_fold({fetch_clocks_nval,
                                    2,
                                    lists:sublist(DirtySegmentsN2, 32)},
                                CH),
    lists:foreach(fun({B, _K, _C}) ->
                        ?assertMatch({?NVAL_TYPE, ?NVAL_BUCKET}, B)
                    end,
                    KCL0N2),
    ?LOG_INFO("Fetched ~w keys from 32 dirty segments", [length(KCL0N2)]),
    ?assertMatch(true, length(KCL0N2) >= 32),
    {ok, KCL1N2} =
        riak_client:aae_fold(
            {fetch_clocks_nval, 2, lists:sublist(DirtySegmentsN2, 32)},
            CT),
    ?assertMatch(true, lists:sort(KCL0N2) == lists:sort(KCL1N2)),



    ?LOG_INFO("No nval=2 keys from fetch_clocks when searching for nval=3"),
    {ok, KCL0N3} =
        riak_client:aae_fold(
            {fetch_clocks_nval, 3, lists:sublist(DirtySegmentsN2, 10)},
            CH),
    lists:foreach(
        fun({B, _K, _C}) -> ?assertMatch(?BUCKET, B) end, KCL0N3),

    ?LOG_INFO("Stopping a node - query results should be unchanged"),
    rt:stop_and_wait(hd(tl(Nodes))),
    {ok, BH4} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),
    ?assertMatch(true, BH3 == BH4),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)),

    % Need to re-start or clean will fail
    rt:start_and_wait(hd(tl(Nodes))),

    VnodeCount = get_vnode_count(hd(Nodes)),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    
    rt_redbug:set_tracing_applied(true),
    ?LOG_INFO("Setup redbug tracing for force rebuild test"),
    {ok, CWD} = file:get_cwd(),
    FnameBase = "rebug_aaefold",
    FileBase = filename:join([CWD, FnameBase]),
    ApproxTrcFile = FileBase ++ "Approx",
    file:delete(ApproxTrcFile),

    TraceFun  = "aae_controller:aae_fetchclocks/5",
    enable_tree_rebuilds(hd(Nodes)),
    VnodeCount = get_vnode_count(hd(Nodes)),

    redbug_start(TraceFun, ApproxTrcFile, hd(Nodes)),

    ?LOG_INFO("Testing that each vnode repaired no more than once"),

    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),

    stopped = redbug:stop(hd(Nodes)),

    RebuildCount = trace_count(TraceFun, ApproxTrcFile),
    ?assert(RebuildCount > 0),
    ?assert(RebuildCount =< VnodeCount),

    file:delete(ApproxTrcFile)
    .

verify_aae_rebuildfold(Node) ->
    ?LOG_INFO("Loading nval 1 node with 10000 keys"),
    KVs = test_data(1, 10000, list_to_binary("U1")),
    ok = write_data(Node, KVs),

    rt_redbug:set_tracing_applied(true),
    ?LOG_INFO("Setup redbug tracing for force rebuild test"),
    {ok, CWD} = file:get_cwd(),
    FnameBase = "rebug_aaefold",
    FileBase = filename:join([CWD, FnameBase]),
    PreTrcFile = FileBase ++ "Pre",
    RebuildTrcFile = FileBase ++ "Rebuild",
    PostTrcFile = FileBase ++ "Post",
    RerunTrcFile = FileBase ++ "Rerun",
    file:delete(PreTrcFile),
    file:delete(RebuildTrcFile),
    file:delete(PostTrcFile),
    file:delete(RerunTrcFile),
    TraceFun  = "aae_controller:aae_fetchclocks/5",

    {ok, CH} = riak:client_connect(Node),

    redbug_start(TraceFun, PreTrcFile, Node),

    {ok, _KCL} =
        riak_client:aae_fold({fetch_clocks_nval, 1, lists:seq(1, 1000)}, CH),
    SegCount = fold_until_segments_hit_key(CH, 500),
    TestSegs = lists:seq(1, SegCount),

    stopped = redbug:stop(Node),

    enable_tree_rebuilds(Node),

    ?assertMatch(0, trace_count(TraceFun, PreTrcFile)),
    file:delete(PreTrcFile),
    
    VnodeCount = get_vnode_count(Node),
    redbug_start(TraceFun, RebuildTrcFile, Node),
    ?LOG_INFO("Testing each vnode repaired exactly once"),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    stopped = redbug:stop(Node),
    ?assertMatch(VnodeCount, trace_count(TraceFun, RebuildTrcFile)),
    file:delete(RebuildTrcFile),

    disable_tree_rebuilds(Node),

    redbug_start(TraceFun, PostTrcFile, Node),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    stopped = redbug:stop(Node),
    ?assertMatch(0, trace_count(TraceFun, PostTrcFile)),
    file:delete(PostTrcFile),

    enable_tree_rebuilds(Node),

    redbug_start(TraceFun, RerunTrcFile, Node),
    ?LOG_INFO("Testing each vnode repaired exactly once - again"),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    {ok, KCLN} = riak_client:aae_fold({fetch_clocks_nval, 1, TestSegs}, CH),
    stopped = redbug:stop(Node),
    ?assertMatch(VnodeCount, trace_count(TraceFun, RerunTrcFile)),
    file:delete(RerunTrcFile),

    ok.


enable_tree_rebuilds(Node) ->
    rpc:call(Node, riak_kv_ttaaefs_manager, trigger_tree_repairs, []).

disable_tree_rebuilds(Node) ->
    rpc:call(Node, riak_kv_ttaaefs_manager, diable_tree_repairs, []).

fold_until_segments_hit_key(CH, N) ->
    {ok, KCL} =
        riak_client:aae_fold({fetch_clocks_nval, 1, lists:seq(1, N)}, CH),
    case length(KCL) of
        L when L > 0 ->
            ?LOG_INFO("Testing ~w segments hit ~w keys", [N, L]),
            N;
        _ ->
            fold_until_segments_hit_key(CH, N + 100)
    end.

get_vnode_count(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    length(rpc:call(Node, riak_core_ring, my_indices, [R])).

trace_count(TraceFun, File) ->
    ?LOG_INFO("checking ~p", [File]),
    case file:read_file(File) of
        {ok, FileData} ->
            count_matches(re:run(FileData, TraceFun, [global]));
        {error, enoent} ->
            0
    end.

count_matches(nomatch) ->
    0;
count_matches({match, Matches}) ->
    length(Matches).
    

redbug_start(TraceFun, TrcFile, Node) ->
    timer:sleep(1000), % redbug doesn't always appear to immediately stop
    ?LOG_INFO("TracingFun ~s on ~w", [TraceFun, Node]),
    TraceOpts =
        maps:merge(
            maps:from_list(
                [{target, Node}, {arity, true}, {print_file, TrcFile}]),
                rt_redbug:default_trace_options()
            ),
    Start = redbug:start(TraceFun, TraceOpts),
    ?LOG_INFO("Redbug start message ~w", [Start]).
    
to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(?BUCKET, Node, KVs, []).

write_data(Bucket, Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, Bucket, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(Bucket, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    timer:sleep(5000),
    ok.

wait_until_root_stable(Client) ->
    {ok, RH0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, Client),
    timer:sleep(2000),
    {ok, RH1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, Client),
    case aae_exchange:compare_roots(RH0, RH1) of
        [] ->
            ?LOG_INFO("Root appears stable matched");
        [L] ->
            Pre = L * 4,
            <<_B0:Pre/binary, V0:32/integer, _Post0/binary>> = RH0,
            <<_B1:Pre/binary, V1:32/integer, _Post1/binary>> = RH1,
            ?LOG_INFO("Root not stable: branch ~w compares ~w with ~w",
                        [L, V0, V1]),
            wait_until_root_stable(Client)
    end.
