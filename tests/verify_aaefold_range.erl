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
%% @doc Verification of AAE fold based on range (dynamic fold-based trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store
-module(verify_aaefold_range).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(CFG_NOREBUILD,
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
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

-define(NUM_KEYS, 12000). % must be divisible by node count
-define(BUCKET, <<"test_bucket">>).
-define(DELTA_COUNT, 10).

confirm() ->
    rt:set_advanced_conf(all, ?CFG_NOREBUILD),
    Nodes0 = rt:build_cluster([previous, previous, current, current]),
    ok = verify_aae_fold(Nodes0),
    pass.


verify_aae_fold(Nodes) ->

    CountPerNode = ?NUM_KEYS div length(Nodes),
    ?assertMatch(?NUM_KEYS, CountPerNode * length(Nodes)),

    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CT} = riak:client_connect(lists:last(Nodes)),

    ?LOG_INFO("Fold for empty tree range"),
    TreeQuery = {merge_tree_range, ?BUCKET, all, small, all, all, pre_hash},
    {ok, RH0} = riak_client:aae_fold(TreeQuery, CH),
    {ok, RT0} = riak_client:aae_fold(TreeQuery, CT),

    ?LOG_INFO("Commencing object load"),
    KeyLoadFun =
        fun(Node, KeyCount) ->
            KVs = test_data(
                KeyCount + 1,
                KeyCount + CountPerNode,
                list_to_binary("U1")),
            ok = write_data(Node, KVs),
            KeyCount + CountPerNode
        end,

    lists:foldl(KeyLoadFun, 0, Nodes),
    ?LOG_INFO("Loaded ~b objects", [?NUM_KEYS]),

    ?LOG_INFO("Fold for busy tree"),
    {ok, RH1} = riak_client:aae_fold(TreeQuery, CH),

    ?LOG_INFO("Force use of old trees on comparator"),
    ok =
        erpc:call(
            lists:last(Nodes),
            application,
            set_env,
            [riak_kv, legacyformat_tictacaae_tree, true]
        ),
    {ok, RT1} = riak_client:aae_fold(TreeQuery, CT),

    ?assertMatch(false, RH0 == RH1),
    ?assertMatch(true, [] == aae_exchange:compare_trees(RH0, RT0)),
    ?assertMatch(true, [] == aae_exchange:compare_trees(RH1, RT1)),

    ?LOG_INFO("Make ~b changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),

    {ok, RH2} = riak_client:aae_fold(TreeQuery, CH),
    DirtySegments1 = aae_exchange:compare_trees(RH1, RH2),

    ?LOG_INFO("Found ~b mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(true, length(DirtySegments1) > 0),
    ?assertMatch(true, length(DirtySegments1) =< ?DELTA_COUNT),

    FetchClocksQuery =
        {fetch_clocks_range,
            ?BUCKET, all,
            {segments, DirtySegments1, small},
            all},

    {ok, KCL1} = riak_client:aae_fold(FetchClocksQuery, CH),

    ?LOG_INFO("Found ~b mismatched keys", [length(KCL1)]),

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

    ?LOG_INFO("Activate bucket type and load objects"),
    rt:create_and_activate_bucket_type(
        hd(Nodes), <<"nval4">>, [{n_val, 4}, {allow_mult, false}]
    ),

    Nv4B = {<<"nval4">>, <<"test_typed_buckets">>},
    timer:sleep(1000),

    KeyLoadTypeBFun =
        fun(Node, KeyCount) ->
            KVs =
                test_data(
                    KeyCount + 1,
                    KeyCount + CountPerNode,
                    list_to_binary("U1")
                ),
            ok = write_data(Node, KVs, [], Nv4B),
            KeyCount + CountPerNode
        end,
    lists:foldl(KeyLoadTypeBFun, 0, Nodes),
    TypedBucketObjectCount = ?NUM_KEYS,
    ?LOG_INFO("Loaded ~b objects", [TypedBucketObjectCount]),
    timer:sleep(1000),

    ObjectStatsTypedBucketQuery = {object_stats, Nv4B, all, all},
    {ok, ObjStatsTypedBucket0} =
        riak_client:aae_fold(ObjectStatsTypedBucketQuery, CH),
    ?LOG_INFO("Object Stats ~0p", [ObjStatsTypedBucket0]),
    {total_count, TCBT0} = hd(ObjStatsTypedBucket0),
    ?assertMatch(TCBT0, TypedBucketObjectCount),

    ?LOG_INFO("Stopping a node - query results should be unchanged"),
    rt:stop_and_wait(hd(tl(Nodes))),

    {ok, KCL2} = riak_client:aae_fold(FetchClocksQuery, CH),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)).


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    write_data(Node, KVs, Opts, ?BUCKET).

write_data(Node, KVs, Opts, Bucket) ->
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

