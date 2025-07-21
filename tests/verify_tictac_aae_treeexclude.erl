%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Martin Sumner.
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
%% @doc Verification of Active Anti Entropy -  Tree Exclude.
%% Confirm that entropy issues are/are-not detected and resolved as expected
%% depending on the use of the aae_tree_exclude bucket property
-module(verify_tictac_aae_treeexclude).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(CFG_NOREBUILD(PrimaryOnly, InitialSkip, MaxResults, ExTick, KR),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, ExTick * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {tictacaae_primaryonly, PrimaryOnly},
           {tictacaae_stepinitialtick, InitialSkip},
           {tictacaae_maxresults, MaxResults},
           {tictacaae_repairloops, 4},
           {tictacaae_enablekeyrange, KR}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?RING_SIZE}
          ]}]
       ).
-define(CFG_REBUILD(BlockTimeout),
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
           {tictacaae_rebuildtick, 15 * 1000}, % Check for rebuilds!
           {max_aae_queue_time, 0},
           {tictacaae_stepinitialtick, false},
           {log_readrepair, true},
           {tictacaae_enablekeyrange, true},
           {tictacaae_rebuild_blocktime, BlockTimeout}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(RING_SIZE, 16).
-define(NUM_KEYS, 16000).
-define(ROGUE_KEYS, 2000).
-define(N_VAL, 3).
-define(STD_TYPE1, <<"BT1">>).
-define(STD_TYPE2, <<"BT2">>).
-define(ST_BUCKET1, <<"Bucket1">>).
-define(ST_BUCKET2, <<"Bucket2">>).
-define(EXCL_TYPE, <<"ExBT">>).
-define(EXCL_BUCKET, <<"ExBT">>).
-define(TYPED_BUCKET_NAME, <<"TestBucket">>).

confirm() ->

    Nodes1 =
        rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(true, false, 128, 15, false)
    ),
    ok = verify_aae(Nodes1),

    rt:clean_cluster(Nodes1),
    Nodes2 = rt:build_cluster(?NUM_NODES, ?CFG_REBUILD(1000)),
    ok = verify_aae(Nodes2),

    pass.


verify_aae(Nodes) ->
    ?LOG_INFO("Tictac AAE tests with aae_tree_exclude"),
    Node = hd(Nodes),
    PB = rt:pbc(Node),

    ?LOG_INFO("Setting bucket types"),
    ?LOG_INFO(
        "As an side - checking bucket properties enabled as expected"
        "For sync_on_write and node_confirms"
    ),
    rt:create_activate_and_wait_for_bucket_type(
        Nodes, ?STD_TYPE1, [{sync_on_write, one}, {node_confirms, 0}]
    ),
    rt:create_activate_and_wait_for_bucket_type(
        Nodes, ?STD_TYPE2, [{sync_on_write, backend}, {node_confirms, 1}]
    ),
    rt:create_activate_and_wait_for_bucket_type(
        Nodes, ?EXCL_TYPE, [{aae_tree_exclude, true}]
    ),
    ?LOG_INFO("Setting properties on untyped buckets"),
    rt:pbc_set_bucket_prop(
        PB, ?ST_BUCKET1, [{sync_on_write, one}, {node_confirms, 0}]
    ),
    rt:pbc_set_bucket_prop(
        PB, ?ST_BUCKET2, [{sync_on_write, backend}, {node_confirms, 1}]
    ),
    rt:pbc_set_bucket_prop(
        PB, ?EXCL_BUCKET, [{sync_on_write, all}, {aae_tree_exclude, true}]
    ),
    rt:wait_until_bucket_props(Nodes, ?EXCL_BUCKET, [{aae_tree_exclude, true}]),

    B1Props =
        erpc:call(
            Node,
            riak_core_bucket,
            get_bucket,
            [{?STD_TYPE1, ?TYPED_BUCKET_NAME}]
    ),
    B2Props =
        erpc:call(
            Node,
            riak_core_bucket,
            get_bucket,
            [{?STD_TYPE2, ?TYPED_BUCKET_NAME}]
    ),
    B4Props =
        erpc:call(Node, riak_core_bucket, get_bucket, [?ST_BUCKET1]
    ),
    B5Props =
        erpc:call(Node, riak_core_bucket, get_bucket, [?ST_BUCKET2]
    ),
    B6Props =
        erpc:call(Node, riak_core_bucket, get_bucket, [?EXCL_BUCKET]
    ),
    ?assert(lists:member({sync_on_write, one}, B1Props)),
    ?assert(lists:member({sync_on_write, backend}, B2Props)),
    ?assert(lists:member({sync_on_write, one}, B4Props)),
    ?assert(lists:member({sync_on_write, backend}, B5Props)),
    ?assert(lists:member({sync_on_write, all}, B6Props)),
    ?assert(lists:member({node_confirms, 0}, B1Props)),
    ?assert(lists:member({node_confirms, 1}, B2Props)),
    ?assert(lists:member({node_confirms, 0}, B4Props)),
    ?assert(lists:member({node_confirms, 1}, B5Props)),

    ?LOG_INFO("Don't test with sync = all (or even sync = one).  Very slow!"),
    erpc:call(
        Node,
        riak_core_bucket_type,
        update,
        [?STD_TYPE1, [{sync_on_write, backend}]]
    ),
    rt:pbc_set_bucket_prop(
        PB, ?ST_BUCKET1, [{sync_on_write, backend}]
    ),
    rt:pbc_set_bucket_prop(
        PB, ?EXCL_BUCKET, [{sync_on_write, backend}]
    ),

    ?LOG_INFO("Loading initial data ~w keys per bucket", [?NUM_KEYS]),
    KeyList = test_KVs(1, ?NUM_KEYS),
    write_data(Node, KeyList, [], {?STD_TYPE1, ?TYPED_BUCKET_NAME}),
    write_data(Node, KeyList, [], {?STD_TYPE2, ?TYPED_BUCKET_NAME}),
    write_data(Node, KeyList, [], {?EXCL_TYPE, ?TYPED_BUCKET_NAME}),
    write_data(Node, KeyList, [], ?ST_BUCKET1),
    write_data(Node, KeyList, [], ?ST_BUCKET2),
    write_data(Node, KeyList, [], ?EXCL_BUCKET),

    ?LOG_INFO("Loading rogue data ~w keys per bucket", [?ROGUE_KEYS]),
    RogueKeyList = test_KVs(?NUM_KEYS + 1, ?NUM_KEYS + ?ROGUE_KEYS),
    write_data(Node, RogueKeyList, [{n_val, 1}], {?STD_TYPE1, ?TYPED_BUCKET_NAME}),
    write_data(Node, RogueKeyList, [{n_val, 1}], {?STD_TYPE2, ?TYPED_BUCKET_NAME}),
    write_data(Node, RogueKeyList, [{n_val, 1}], {?EXCL_TYPE, ?TYPED_BUCKET_NAME}),
    write_data(Node, RogueKeyList, [{n_val, 1}], ?ST_BUCKET1),
    write_data(Node, RogueKeyList, [{n_val, 1}], ?ST_BUCKET2),
    write_data(Node, RogueKeyList, [{n_val, 1}], ?EXCL_BUCKET),    

    MaxTime = rt_config:get(rt_max_wait_time),

    ?LOG_INFO("Verify ~w non-rogue keys for each bucket", [?NUM_KEYS]),
    ok = verify_data(Node, KeyList, {?STD_TYPE1, ?TYPED_BUCKET_NAME}, MaxTime),
    ok = verify_data(Node, KeyList, {?STD_TYPE2, ?TYPED_BUCKET_NAME}, MaxTime),
    ok = verify_data(Node, KeyList, {?EXCL_TYPE, ?TYPED_BUCKET_NAME}, MaxTime),
    ok = verify_data(Node, KeyList, ?ST_BUCKET1, MaxTime),
    ok = verify_data(Node, KeyList, ?ST_BUCKET2, MaxTime),
    ok = verify_data(Node, KeyList, ?EXCL_BUCKET, MaxTime),

    ?LOG_INFO("Verify ~w rogue keys for each std bucket", [?ROGUE_KEYS]),
    ok = verify_data(Node, RogueKeyList, {?STD_TYPE1, ?TYPED_BUCKET_NAME}, MaxTime),
    ok = verify_data(Node, RogueKeyList, {?STD_TYPE2, ?TYPED_BUCKET_NAME}, MaxTime),
    ok = verify_data(Node, RogueKeyList, ?ST_BUCKET1, MaxTime),
    ok = verify_data(Node, RogueKeyList, ?ST_BUCKET2, MaxTime),

    NoFixMaxTime = 10000,
    ?LOG_INFO(
        "If not for aae_tree_exclude exclude buckets would be fixed with "
        "other buckets - wait a short time to confirm not fixed"
    ),
    aae_failed_to_fix_data =
        verify_data(
            Node, RogueKeyList, {?EXCL_TYPE, ?TYPED_BUCKET_NAME}, NoFixMaxTime
        ),
    aae_failed_to_fix_data =
        verify_data(
            Node, RogueKeyList, ?EXCL_BUCKET, NoFixMaxTime
        ),

    ?LOG_INFO(
        "Reset all the key filters - just check they all return true"
    ),
    {?RING_SIZE, 0} = erpc:call(Node, riak_kv_util, reset_aae_key_filter, []),

    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_KVs(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

write_data(Node, KVs, Opts, Bucket) ->
    ?LOG_INFO("Data load to Bucket ~0p", [Bucket]),
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
    ok.

verify_data(Node, KeyValues, Bucket, MaxTime) ->
    ?LOG_INFO("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
        fun() ->
            Matches =
                [
                    verify_replicas(Node, Bucket, K, V, ?N_VAL)
                       || {K, V} <- KeyValues
                ],
            CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
            NumGood = lists:foldl(CountTrues, 0, Matches),
            Num = length(KeyValues),
            case Num == NumGood of
                true -> true;
                _ ->
                    ?LOG_INFO(
                        "Data not yet correct: ~b mismatches",
                        [Num-NumGood]
                    ),
                    false
            end
        end,
    Delay = 2000, % every two seconds until max time.
    Retry = MaxTime div Delay,
    R =
        case rt:wait_until(CheckFun, Retry, Delay) of
            ok ->
                ?LOG_INFO("Data is now correct. Yay!");
            _ ->
                ?LOG_ERROR("AAE failed to fix data"),
                aae_failed_to_fix_data
        end,
    riakc_pb_socket:stop(PB),
    R.

merge_values(O) ->
    Vals = riak_object:get_values(O),
    lists:foldl(
        fun(NV, V) ->
                case size(NV) > size(V) of
                    true -> NV;
                    _ -> V
                end
        end,
        <<>>,
        Vals
    ).

verify_replicas(Node, B, K, V, N) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
               || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.