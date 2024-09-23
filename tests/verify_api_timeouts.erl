%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
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
-module(verify_api_timeouts).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"listkeys_bucket">>).
-define(NUM_BUCKETS, 1200).
-define(NUM_KEYS, 1000).

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    [Node] = rt:build_cluster(1),
    rt:wait_until_pingable(Node),

    HC = rt:httpc(Node),
    ?LOG_INFO("setting up initial data and loading remote code"),
    rt:httpc_write(HC, <<"foo">>, <<"bar">>, <<"foobarbaz\n">>),
    rt:httpc_write(HC, <<"foo">>, <<"bar2">>, <<"foobarbaz2\n">>),

    put_keys(Node, ?BUCKET, ?NUM_KEYS),
    put_buckets(Node, ?NUM_BUCKETS),
    timer:sleep(2000),

    rt_intercept:add(Node, {riak_kv_get_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_put_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_vnode,
                            [{{handle_coverage,4}, slow_handle_coverage}]}),


    ?LOG_INFO("testing HTTP API"),

    ?LOG_INFO("testing GET timeout"),
    {error, Tup1} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup1),

    ?LOG_INFO("testing PUT timeout"),
    {error, Tup2} = rhc:put(HC, riakc_obj:new(<<"foo">>, <<"bar">>,
                                              <<"getgetgetgetget\n">>),
                            [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup2),

    ?LOG_INFO("testing DELETE timeout"),
    {error, Tup3} = rhc:delete(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup3),

    ?LOG_INFO("testing invalid timeout value"),
    {error, Tup4} = bad_timeout_fun_http(HC),
    ?assertMatch({ok, "400", _,
                  <<"Bad timeout value \"asdasdasd\"\n">>},
                 Tup4),

    ?LOG_INFO("testing GET still works before long timeout"),
    {ok, O} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 4000}]),

    %% either of these are potentially valid.
    ok =
        case riakc_obj:get_values(O) of
            [<<"foobarbaz\n">>] ->
                ?LOG_INFO("Original Value"),
                ok;
            [<<"getgetgetgetget\n">>] ->
                ?LOG_INFO("New Value"),
                ok;
            [_A, _B] = L ->
                ?assertEqual([<<"foobarbaz\n">>,<<"getgetgetgetget\n">>],
                            lists:sort(L)),
                ?LOG_INFO("Both Values"),
                ok
        end,

    PC = rt:pbc(Node),

    ?LOG_INFO("testing PBC API"),

    BOOM = {error, <<"timeout">>},

    ?LOG_INFO("testing GET timeout"),
    PGET = riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, [{timeout, 100}]),
    ?assertEqual(BOOM, PGET),

    ?LOG_INFO("testing PUT timeout"),
    PPUT =
        riakc_pb_socket:put(
            PC,
            riakc_obj:new(<<"foo">>, <<"bar2">>, <<"get2get2get2get2get\n">>),
            [{timeout, 100}]
        ),
    ?assertEqual(BOOM, PPUT),

    ?LOG_INFO("testing DELETE timeout"),
    PDEL = riakc_pb_socket:delete(PC, <<"foo">>, <<"bar2">>,
                                  [{timeout, 100}]),
    ?assertEqual(BOOM, PDEL),

    ?LOG_INFO("testing invalid timeout value"),
    ?assertError(badarg, bad_timeout_fun_pb(PC)),

    ?LOG_INFO("testing GET still works before long timeout"),
    {ok, O2} =
        riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, [{timeout, 4000}]),

    %% either of these are potentially valid.
    ok =
        case riakc_obj:get_values(O2) of
            [<<"get2get2get2get2get\n">>] ->
                ?LOG_INFO("New Value"),
                ok;
            [<<"foobarbaz2\n">>] ->
                ?LOG_INFO("Original Value"),
                ok;
            [_A2, _B2] = L2 ->
                ?assertEqual([<<"foobarbaz2\n">>, <<"get2get2get2get2get\n">>],
                            lists:sort(L2)),
                ?LOG_INFO("Both Values"),
                ok
        end,


    Long = 1000000,
    Short = 1000,

    ?LOG_INFO("Checking List timeouts"),

    ?LOG_INFO("Checking PBC"),
    Pid = rt:pbc(Node),
    ?LOG_INFO("Checking keys timeout"),
    ?assertMatch({error, <<"timeout">>},
                 riakc_pb_socket:list_keys(Pid, ?BUCKET, Short)),
    ?LOG_INFO("Checking keys w/ long timeout"),
    ?assertMatch({ok, _},
                 riakc_pb_socket:list_keys(Pid, ?BUCKET, Long)),
    ?LOG_INFO("Checking stream keys timeout"),
    {ok, ReqId0} = riakc_pb_socket:stream_list_keys(Pid, ?BUCKET, Short),
    wait_for_error(ReqId0),
    ?LOG_INFO("Checking stream keys works w/ long timeout"),
    {ok, ReqId8} = riakc_pb_socket:stream_list_keys(Pid, ?BUCKET, Long),
    wait_for_end(ReqId8),

    ?LOG_INFO("Checking buckets timeout"),
    ?assertMatch({error, <<"timeout">>},
                 riakc_pb_socket:list_buckets(Pid, Short)),
    ?LOG_INFO("Checking buckets w/ long timeout"),
    ?assertMatch({ok, _},
                 riakc_pb_socket:list_buckets(Pid, Long)),
    ?LOG_INFO("Checking stream buckets timeout"),
    {ok, ReqId1} = riakc_pb_socket:stream_list_buckets(Pid, Short),
    wait_for_error(ReqId1),
    ?LOG_INFO("Checking stream buckets works w/ long timeout"),
    {ok, ReqId7} = riakc_pb_socket:stream_list_buckets(Pid, Long),
    wait_for_end(ReqId7),


    ?LOG_INFO("Checking HTTP"),
    LHC = rt:httpc(Node),
    ?LOG_INFO("Checking keys timeout"),
    ?assertMatch({error, <<"timeout">>},
                 rhc:list_keys(LHC, ?BUCKET, Short)),
    ?LOG_INFO("Checking keys w/ long timeout"),
    ?assertMatch({ok, _},
                 rhc:list_keys(LHC, ?BUCKET, Long)),
    ?LOG_INFO("Checking stream keys timeout"),
    {ok, ReqId2} = rhc:stream_list_keys(LHC, ?BUCKET, Short),
    wait_for_error(ReqId2),
    ?LOG_INFO("Checking stream keys works w/ long timeout"),
    {ok, ReqId4} = rhc:stream_list_keys(LHC, ?BUCKET, Long),
    wait_for_end(ReqId4),

    ?LOG_INFO("Checking buckets timeout"),
    ?assertMatch({error, <<"timeout">>},
                 rhc:list_buckets(LHC, Short)),
    ?LOG_INFO("Checking buckets w/ long timeout"),
    ?assertMatch({ok, _},
                 rhc:list_buckets(LHC, Long)),
    ?LOG_INFO("Checking stream buckets timeout"),
    {ok, ReqId3} = rhc:stream_list_buckets(LHC, Short),
    wait_for_error(ReqId3),
    ?LOG_INFO("Checking stream buckets works w/ long timeout"),
    {ok, ReqId5} = rhc:stream_list_buckets(LHC, Long),
    wait_for_end(ReqId5),

    pass.

-dialyzer({nowarn_function, bad_timeout_fun_pb/1}).
-dialyzer({nowarn_function, bad_timeout_fun_http/1}).

bad_timeout_fun_pb(PC) ->
    riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, [{timeout, asdasdasd}]).

bad_timeout_fun_http(HC) ->
    rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, asdasdasd}]).

wait_for_error(ReqId) ->
    receive
        {ReqId, done} ->
            ?LOG_ERROR("stream incorrectly finished"),
            error(stream_finished);
        {ReqId, {error, <<"timeout">>}} ->
            ?LOG_INFO("stream correctly timed out"),
            ok;
        {ReqId, {_Key, _Vals}} ->
            %% the line below is spammy but nice for debugging
            %%{ReqId, {Key, Vals}} ->
            %%?LOG_INFO("Got some values ~0p, ~0p", [Key, Vals]),
            wait_for_error(ReqId);
        {ReqId, Other} ->
            error({unexpected_message, Other})
    after 10000 ->
            error(error_stream_recv_timed_out)
    end.

wait_for_end(ReqId) ->
    receive
        {ReqId, done} ->
            ?LOG_INFO("stream correctly finished"),
            ok;
        {ReqId, {error, <<"timeout">>}} ->
            ?LOG_ERROR("stream incorrectly timed out"),
            error(stream_timed_out);
       {ReqId, {_Key, _Vals}} ->
            %% the line below is spammy but nice for debugging
            %%{ReqId, {Key, Vals}} ->
            %%?LOG_INFO("Got some values ~0p, ~0p", [Key, Vals]),
            wait_for_end(ReqId);
        {ReqId, Other} ->
            error({unexpected_message, Other})
    after 10000 ->
            error(error_stream_recv_timed_out)
    end.


put_buckets(Node, Num) ->
    Pid = rt:pbc(Node),
    Buckets = [list_to_binary(["", integer_to_list(Ki)])
               || Ki <- lists:seq(0, Num - 1)],
    {Key, Val} = {<<"test_key">>, <<"test_value">>},
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val))
     || Bucket <- Buckets],
    riakc_pb_socket:stop(Pid).


put_keys(Node, Bucket, Num) ->
    Pid = rt:pbc(Node),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)],
    riakc_pb_socket:stop(Pid).
