%% -------------------------------------------------------------------
%%
%% Copyright (c) 2024 Workday, Inc.
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
-module(verify_clone).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(STAT_DELAY, 8000).

-define(BROKEN_BUCKET_TYPE, <<"some_non_existent">>).
-define(BUCKET_TYPE_A, <<"bucket_type_a">>).
-define(BUCKET_TYPE_B, <<"bucket_type_b">>).
-define(BUCKET_A, <<"clone_bucket_a">>).
-define(BUCKET_B, <<"clone_bucket_b">>).
-define(SRC_BUCKET, {?BUCKET_TYPE_A,   ?BUCKET_A}).
-define(DST_BUCKET, {?BUCKET_TYPE_B,   ?BUCKET_B}).

-define(HAPPY_COPY_SRC_KEY, <<"happy_copy_src_key">>).
-define(HAPPY_COPY_DST_KEY, <<"happy_copy_dest_key">>).
-define(HAPPY_COPY_VALUE, <<"happy_copy_value">>).
-define(HAPPY_MOVE_SRC_KEY, <<"happy_move_src_key">>).
-define(HAPPY_MOVE_DST_KEY, <<"happy_move_dest_key">>).
-define(HAPPY_MOVE_VALUE, <<"happy_move_value">>).
-define(EXISTING_KEY, <<"key_exists_in_both_buckets">>).
-define(EXISTING_VALUE, <<"existing value">>).
-define(VCLOCK_SRC_KEY, <<"src_key_vclock">>).
-define(VCLOCK_DST_KEY, <<"dst_key_vclock">>).
-define(VCLOCK_VALUE, <<"value_vclock">>).
-define(VCLOCK, <<"some_vclock">>).

confirm() ->
    [[NodeA1, NodeA2, NodeA3], [NodeB1]] = rt:build_clusters([3, 1]),

    rt:create_and_activate_bucket_type(NodeA3, ?BUCKET_TYPE_A, []),
    rt:create_and_activate_bucket_type(NodeA2, ?BUCKET_TYPE_B, []),
    rt:create_and_activate_bucket_type(NodeB1, ?BUCKET_TYPE_A, []),
    rt:create_and_activate_bucket_type(NodeA1, ?BROKEN_BUCKET_TYPE, []),

    rt:wait_until_bucket_type_status(?BUCKET_TYPE_A, active, NodeA1),
    rt:wait_until_bucket_type_status(?BUCKET_TYPE_B, active, NodeA1),
    rt:wait_until_bucket_type_status(?BROKEN_BUCKET_TYPE, active, NodeA1),

    PbcNodeA1 = rt:pbc(NodeA1),
    ok = riakc_pb_socket:put(
        PbcNodeA1,
        riakc_obj:new(
            ?SRC_BUCKET,
            ?HAPPY_COPY_SRC_KEY,
            ?HAPPY_COPY_VALUE
        )
    ),
    ok = riakc_pb_socket:put(
        PbcNodeA1,
        riakc_obj:new(
            ?SRC_BUCKET,
            ?HAPPY_MOVE_SRC_KEY,
            ?HAPPY_MOVE_VALUE
        )
    ),
    ok = riakc_pb_socket:put(
        PbcNodeA1,
        riakc_obj:new(
            ?SRC_BUCKET,
            ?EXISTING_KEY,
            ?EXISTING_VALUE
        )
    ),
    ok = riakc_pb_socket:put(
        PbcNodeA1,
        riakc_obj:new(
            ?DST_BUCKET,
            ?EXISTING_KEY,
            ?EXISTING_VALUE
        )
    ),
    ok = riakc_pb_socket:put(
        PbcNodeA1,
        riakc_obj:new(
            ?SRC_BUCKET,
            ?VCLOCK_SRC_KEY,
            ?VCLOCK_VALUE,
            riak_object:encode_vclock(?VCLOCK)
        )
    ),

    test_destination_not_empty_error(PbcNodeA1),
    test_name_unchanged_error(PbcNodeA1),
    test_successful_copy(PbcNodeA1, NodeA1),
    test_successful_move(PbcNodeA1, NodeA1),
    test_cannot_clone_with_mismatched_vclock(PbcNodeA1),
    test_copy_with_vclocks(PbcNodeA1),
    test_key_generation(PbcNodeA1),
    test_return_body(PbcNodeA1),
    test_operation_details(PbcNodeA1),
    test_copy_move_timeout(PbcNodeA1),
    test_move_delete_failure(PbcNodeA1),
    pass.

test_destination_not_empty_error(Pbc) ->
    ?LOG_INFO("Verify cannot copy/move when destination exists ..."),
    ?assertMatch(
        {error, destination_not_empty},
        riakc_pb_socket:copy(
            Pbc,
            ?SRC_BUCKET,
            ?EXISTING_KEY,
            ?DST_BUCKET,
            ?EXISTING_KEY
        )
    ),
    ?assertMatch(
        {error, destination_not_empty},
        riakc_pb_socket:move(
            Pbc,
            ?SRC_BUCKET,
            ?EXISTING_KEY,
            ?DST_BUCKET,
            ?EXISTING_KEY
        )
    ),
    pass.

test_name_unchanged_error(Pbc) ->
    ?LOG_INFO("Verify cannot copy/move to the same name ..."),

    ?assertMatch(
        {error, name_unchanged},
        riakc_pb_socket:copy(
            Pbc, ?SRC_BUCKET, ?EXISTING_KEY, ?SRC_BUCKET, ?EXISTING_KEY)),

    ?assertMatch(
        {error, name_unchanged},
        riakc_pb_socket:copy(
            Pbc, ?DST_BUCKET, ?EXISTING_KEY, ?DST_BUCKET, ?EXISTING_KEY)),
    pass.

test_successful_copy(Pbc, Node) ->
    ?LOG_INFO("Verify copy ..."),
    ?assertMatch(
        {error, notfound},
        riakc_pb_socket:get(Pbc, ?DST_BUCKET, ?HAPPY_COPY_DST_KEY)
    ),
    CopyStat = get_stat_value(Node, node_pb_copy_requests_total),
    CopyRes = riakc_pb_socket:copy(
        Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY,
        ?DST_BUCKET, ?HAPPY_COPY_DST_KEY),
    ?assertMatch({ok, _}, CopyRes),
    {_, ResObj} = CopyRes,
    ResMD = riakc_obj:get_metadata(ResObj),
    ResVC = riakc_obj:vclock(ResObj),
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(ResObj)),
    ?assertMatch(?HAPPY_COPY_DST_KEY, riakc_obj:key(ResObj)),
    ?assertMatch(<<>>, riakc_obj:get_value(ResObj)),
    ?assertEqual((CopyStat + 1),
        get_stat_value(Node, node_pb_copy_requests_total)),

    SrcRes = riakc_pb_socket:get(Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY),
    ?assertMatch({ok, _}, SrcRes),
    {ok, SrcObj} = SrcRes,
    SrcMD = riakc_obj:get_metadata(SrcObj),
    SrcVC = riakc_obj:vclock(SrcObj),
    ?assertMatch(?SRC_BUCKET, riakc_obj:bucket(SrcObj)),
    ?assertMatch(?HAPPY_COPY_SRC_KEY, riakc_obj:key(SrcObj)),
    ?assertMatch(?HAPPY_COPY_VALUE, riakc_obj:get_value(SrcObj)),
    ?assertNotEqual(ResMD, SrcMD),
    ?assertNotEqual(ResVC, SrcVC),

    DstRes = riakc_pb_socket:get(Pbc, ?DST_BUCKET, ?HAPPY_COPY_DST_KEY),
    ?assertMatch({ok, _}, DstRes),
    {ok, DstObj} = DstRes,
    DstMD = riakc_obj:get_metadata(DstObj),
    DstVC = riakc_obj:vclock(DstObj),
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(DstObj)),
    ?assertMatch(?HAPPY_COPY_DST_KEY, riakc_obj:key(DstObj)),
    ?assertMatch(?HAPPY_COPY_VALUE, riakc_obj:get_value(DstObj)),
    ?assertEqual(ResMD, DstMD),
    ?assertEqual(ResVC, DstVC),
    pass.

test_successful_move(Pbc, Node) ->
    ?LOG_INFO("Verify move ..."),
    ?assertMatch(
        {error, notfound},
        riakc_pb_socket:get(Pbc, ?DST_BUCKET, ?HAPPY_MOVE_DST_KEY)
    ),
    MoveStat = get_stat_value(Node, node_pb_move_requests_total),
    ?assertMatch({ok, _}, riakc_pb_socket:move(
        Pbc,
        ?SRC_BUCKET,
        ?HAPPY_MOVE_SRC_KEY,
        ?DST_BUCKET,
        ?HAPPY_MOVE_DST_KEY,
        #{ r => 1, pr => 1 }
    )),
    ?assertEqual((MoveStat + 1),
        get_stat_value(Node, node_pb_move_requests_total)),

    ?assertMatch(
        {error, notfound},
        riakc_pb_socket:get(Pbc, ?SRC_BUCKET, ?HAPPY_MOVE_SRC_KEY)
    ),

    GetB = riakc_pb_socket:get(Pbc, ?DST_BUCKET, ?HAPPY_MOVE_DST_KEY),
    ?assertMatch({ok, _}, GetB),
    {ok, R_B} = GetB,
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(R_B)),
    ?assertMatch(?HAPPY_MOVE_DST_KEY, riakc_obj:key(R_B)),
    ?assertMatch(?HAPPY_MOVE_VALUE, riakc_obj:get_value(R_B)),

    pass.

test_cannot_clone_with_mismatched_vclock(Pbc) ->
    ?LOG_INFO("Verify cannot clone with mismatched vclock ..."),
    ?assertMatch(
        {error, src_out_of_date},
        riakc_pb_socket:copy(
            Pbc,
            ?SRC_BUCKET,
            ?VCLOCK_SRC_KEY,
            riak_object:encode_vclock(<<"some_fake_vclock">>),
            ?DST_BUCKET,
            ?VCLOCK_DST_KEY,
            #{}
        )
    ),
    ?assertMatch(
        {error, src_out_of_date},
        riakc_pb_socket:move(
            Pbc,
            ?SRC_BUCKET,
            ?VCLOCK_SRC_KEY,
            riak_object:encode_vclock(<<"some_fake_vclock">>),
            ?DST_BUCKET,
            ?VCLOCK_DST_KEY,
            #{}
        )
    ),
    pass.

test_copy_with_vclocks(Pbc) ->
    ?LOG_INFO("Verify copy with vclock ..."),
    GetR = riakc_pb_socket:get(Pbc, ?SRC_BUCKET, ?VCLOCK_SRC_KEY),
    ?assertMatch({ok, _}, GetR),
    {ok, RObj} = GetR,
    VClock = riakc_obj:vclock(RObj),
    ?assertMatch({ok, _}, riakc_pb_socket:copy(
        Pbc,
        ?SRC_BUCKET,
        ?VCLOCK_SRC_KEY,
        VClock,
        ?DST_BUCKET,
        ?VCLOCK_DST_KEY,
        #{}
    )),
    ?assertMatch(
        {ok, _},
        riakc_pb_socket:get(Pbc, ?DST_BUCKET, ?VCLOCK_DST_KEY)
    ),
    pass.

test_key_generation(Pbc) ->
    ?LOG_INFO("Verify key generation ..."),

    Res = riakc_pb_socket:copy(
        Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY,
        ?DST_BUCKET, undefined ),
    ?assertMatch({ok, _Obj}, Res),
    {_, RObj} = Res,
    Key = riakc_obj:key(RObj),
    ?assertMatch({ok, _},
        riakc_pb_socket:get(Pbc, ?DST_BUCKET, Key)),
    ?LOG_INFO("Generated key: ~0p", [Key]),
    pass.

test_return_body(Pbc) ->
    ?LOG_INFO("Verify copy/move 'return_body' option ..."),
    TestOpts = #{return_body => true, timeout => 123456},

    CopyRes = riakc_pb_socket:copy(
        Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY,
        ?DST_BUCKET, undefined, TestOpts),
    ?assertMatch({ok, _}, CopyRes),
    {_, CopyRObj} = CopyRes,
    CopyKey = riakc_obj:key(CopyRObj),
    CopyGet = riakc_pb_socket:get(Pbc, ?DST_BUCKET, CopyKey),
    ?assertMatch({ok, _}, CopyGet),
    {ok, CopyGObj} = CopyGet,
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(CopyRObj)),
    ?assertMatch(CopyKey, riakc_obj:key(CopyRObj)),
    ?assertEqual(
        riakc_obj:get_value(CopyGObj), riakc_obj:get_value(CopyRObj)),
    ?assertEqual(
        riakc_obj:vclock(CopyGObj), riakc_obj:vclock(CopyRObj)),

    %% Use the record we created with Copy
    MoveRes = riakc_pb_socket:move(
        Pbc, ?DST_BUCKET, CopyKey,
        ?DST_BUCKET, undefined, TestOpts),
    ?assertMatch({ok, _}, MoveRes),
    {_, MoveRObj} = MoveRes,
    MoveKey = riakc_obj:key(MoveRObj),
    MoveGet = riakc_pb_socket:get(Pbc, ?DST_BUCKET, MoveKey),
    ?assertMatch({ok, _}, MoveGet),
    {ok, MoveGObj} = MoveGet,
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(MoveRObj)),
    ?assertMatch(MoveKey, riakc_obj:key(MoveRObj)),
    ?assertEqual(
        riakc_obj:get_value(MoveGObj), riakc_obj:get_value(MoveRObj)),
    ?assertEqual(
        riakc_obj:vclock(MoveGObj), riakc_obj:vclock(MoveRObj)),
    pass.

test_operation_details(Pbc) ->
    ?LOG_INFO("Verify requested details ..."),

    CopyOpts = #{details => [timing]},
    CopyRes = riakc_pb_socket:copy(
        Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY,
        ?DST_BUCKET, undefined, CopyOpts),
    ?assertMatch({ok, CObj, [_|_]} when erlang:is_tuple(CObj), CopyRes),
    {_, _CObj, CopyDetails} = CopyRes,
    ?LOG_INFO("Copy details with ~0p:~n    ~p", [CopyOpts, CopyDetails]),

    MoveOpts = #{details => [true]},
    MoveRes = riakc_pb_socket:move(
        Pbc, ?DST_BUCKET, ?HAPPY_MOVE_DST_KEY,
        ?DST_BUCKET, undefined, MoveOpts),
    ?assertMatch({ok, MObj, [_|_]} when erlang:is_tuple(MObj), MoveRes),
    {_, _MObj, MoveDetails} = MoveRes,
    ?LOG_INFO("Move details with ~0p:~n    ~p", [MoveOpts, MoveDetails]),

    FailOpts = #{details => [true]},
    FailRes = riakc_pb_socket:move(
        Pbc, ?DST_BUCKET, ?HAPPY_MOVE_DST_KEY,
        ?DST_BUCKET, undefined, FailOpts),
    ?assertMatch({error, _, [_|_]}, FailRes),
    {_, FailErr, FailDetails} = FailRes,
    ?LOG_INFO("Fail {error, ~0p, Details} with ~0p:~n    ~p",
        [FailErr, FailOpts, FailDetails]),

    NoneOpts = #{details => [bogus]},
    NoneRes = riakc_pb_socket:move(
        Pbc, ?DST_BUCKET, ?HAPPY_MOVE_DST_KEY,
        ?DST_BUCKET, undefined, NoneOpts),
    ?assertMatch({error, _}, NoneRes),
    {_, NoneErr} = NoneRes,
    ?LOG_INFO("No {error, ~0p} details with ~0p", [NoneErr, NoneOpts]),
    pass.

test_copy_move_timeout(Pbc) ->
    ?LOG_INFO("Verify copy/move timeouts ..."),
    %% Give Get/Put plenty of time so we can [try to] time out in the
    %% encompassing operation. The two Gets (verification that the destination
    %% key doesn't exist followed by fetching the source record) execute VERY
    %% quickly, often under 1ms in a local rtdev harness, so we load up the
    %% options to slow them down as much as we can. The 'recv_timeout' option
    %% will make it through to the Get/Put FSMs so they won't time out there;
    %% we want the timeout to occur at an outer layer.
    TestOpts = #{
        r => all, pr => all, details => [true],
        recv_timeout => 30000
    },
    %% Even a 1ms timeout may get to invoking Put, so we'll try both 0ms and
    %% 1ms. The result should look pretty much the same, but we can tell from
    %% the Details whether Put was invoked and timed out.

    Opts0 = TestOpts#{timeout => 0},
    %% This should time out in 'clone_get'
    Res0 = riakc_pb_socket:copy(
        Pbc, ?DST_BUCKET, ?HAPPY_COPY_DST_KEY,
        ?DST_BUCKET, undefined, Opts0),
    ?assertMatch({error, timeout, [_|_]}, Res0),
    {_, _, Details0} = Res0,
    ?LOG_INFO("Timeout details with ~0p:~n    ~p", [Opts0, Details0]),

    Opts1 = TestOpts#{timeout => 1},
    %% This MAY make it to timing out in 'clone_srcobj'
    Res1 = riakc_pb_socket:copy(
        Pbc, ?DST_BUCKET, ?HAPPY_COPY_DST_KEY,
        ?DST_BUCKET, undefined, Opts1),
    ?assertMatch({error, timeout, [_|_]}, Res0),
    {_, _, Details1} = Res1,
    ?LOG_INFO("Timeout details with ~0p:~n    ~p", [Opts1, Details1]),

    %% Test 'infinity' timeout just to make sure nothing's trying to do
    %% integer arithmetic on it without checking first.
    InfOpts = #{timeout => infinity},
    ?LOG_INFO("Testing with ~0p ...", [InfOpts]),
    InfRes = riakc_pb_socket:move(
        Pbc, ?DST_BUCKET, ?HAPPY_COPY_DST_KEY,
        ?DST_BUCKET, undefined, InfOpts),
    ?assertMatch({ok, _}, InfRes),
    {_, InfObj} = InfRes,
    %% Confirm it's a riakc_obj
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(InfObj)),
    pass.

%% Do this last since it tries to blow away one of our always-present records.
test_move_delete_failure(Pbc) ->
    ?LOG_INFO("Verify partial success on move delete failure ..."),
    %% This option is only consumed by delete, so give it an impossible
    %% value to get through the get/put successfully and generate an
    %% error at the delete phase.
    TestOpts = #{rw => 4},

    MoveRes = riakc_pb_socket:move(
        Pbc, ?SRC_BUCKET, ?HAPPY_COPY_SRC_KEY,
        ?DST_BUCKET, undefined, TestOpts),
    ?assertMatch({ok, _, _}, MoveRes),
    {_, MoveRObj, DelFail} = MoveRes,
    %% Confirm it's a riakc_obj
    ?assertMatch(?DST_BUCKET, riakc_obj:bucket(MoveRObj)),
    ?LOG_INFO("Delete failure reason: ~0p", [DelFail]),
    pass.


get_stat_value(Node, StatKey) ->
    rt:get_stat(rt:get_stats(Node, ?STAT_DELAY), StatKey).
