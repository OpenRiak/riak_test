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
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Basho Technologies
%%% @doc
%%% riak_test for riak_dt counter convergence,
%%% @end
-module(verify_counter_converge).
-behavior(riak_test).

-export([confirm/0]).

%% Called by verify_... tests
-export([set_allow_mult_true/1, set_allow_mult_true/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"test-counters">>).

confirm() ->
    Key = <<"a">>,

    [N1, N2, N3, N4]=Nodes = rt:build_cluster(4),
    [C1, C2, C3, C4]=Clients =  [ rt:httpc(N) || N <- Nodes ],

    set_allow_mult_true(Nodes),

    increment_counter(C1, Key),
    increment_counter(C2, Key, 10),

    [?assertEqual(11, get_counter(C, Key)) || C <- Clients],

    decrement_counter(C3, Key),
    decrement_counter(C4, Key, 2),

    [?assertEqual(8, get_counter(C, Key)) || C <- Clients],

    ?LOG_INFO("Partition cluster in two."),

    PartInfo = rt:partition([N1, N2], [N3, N4]),

    ?LOG_INFO("Increment counter on partitioned cluster"),

    %% increment one side
    increment_counter(C1, Key, 5),

    C1Incr = get_counter(C1, Key),
    C2Incr = get_counter(C2, Key),
    ?LOG_INFO("Validate counter after increment applied"),
    ?LOG_INFO(
        "Initial value on increment side ~0p ~0p",
        [C1Incr, C2Incr]),

    case {C1Incr, C2Incr} of
        {13, 13} ->
            ok;
        _ ->
            SW = os:timestamp(),
            rt:wait_until(fun() -> 13 == get_counter(C1, Key) end),
            rt:wait_until(fun() -> 13 == get_counter(C2, Key) end),
            ?LOG_WARNING(
                "Maybe waited for increment ~b ms",
                [timer:now_diff(os:timestamp(), SW) div 1000]),
            ?LOG_WARNING(
                "Maybe cluster unstable after partition?")
    end,

    %% check value on one side is different from other
    ?assertEqual(13, get_counter(C1, Key)),
    ?assertEqual(13, get_counter(C2, Key)),

    ?assertEqual(8, get_counter(C3, Key)),
    ?assertEqual(8, get_counter(C4, Key)),

    %% decrement other side
    decrement_counter(C3, Key, 2),

    %% verify values differ
    [?assertEqual(13, get_counter(C, Key)) || C <- [C1, C2]],
    [?assertEqual(6, get_counter(C, Key)) || C <- [C3, C4]],

    %% heal
    ?LOG_INFO("Heal and check merged values"),
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv),

    %% verify all nodes agree
    [?assertEqual(ok, rt:wait_until(fun() ->
                                            11 == get_counter(HP, Key)
                                    end)) ||  HP <- Clients ],

    pass.

set_allow_mult_true(Nodes) ->
    set_allow_mult_true(Nodes, ?BUCKET).

set_allow_mult_true(Nodes, Bucket) ->
    %% Counters REQUIRE allow_mult=true
    N1 = hd(Nodes),
    AllowMult = [{allow_mult, true}],
    ?LOG_INFO("Setting bucket properties ~0p for bucket ~0p on node ~0p",
               [AllowMult, Bucket, N1]),
    rpc:call(N1, riak_core_bucket, set_bucket, [Bucket, AllowMult]),
    rt:wait_until_ring_converged(Nodes).

%% Counter API
get_counter(Client, Key) ->
    rt:wait_until(not_404(Client, Key)),
    {ok, Val} = rhc:counter_val(Client, ?BUCKET, Key),
    Val.

%% returns a fun for rt:wait_until/1. It would be great of
%% rt:wait_until had a flavour that returned a value.
not_404(Client, Key) ->
    fun() ->
            Res = rhc:counter_val(Client, ?BUCKET, Key),
            case Res of
                %% NOTE: only 404, any other error is unexpected to
                %% resolve itself
                {error, {ok,"404", _Headers, <<"not found\n">>}} ->
                    ?LOG_INFO("Key ~0p not found on ~0p", [Key, Client]),
                    false;
                _ ->
                    true
            end
    end.

increment_counter(Client, Key) ->
    increment_counter(Client, Key, 1).

increment_counter(Client, Key, Amt) ->
    update_counter(Client, Key, Amt).

decrement_counter(Client, Key) ->
    decrement_counter(Client, Key, 1).

decrement_counter(Client, Key, Amt) ->
    update_counter(Client, Key, -Amt).

update_counter(Client, Key, Amt) ->
    case rhc:counter_incr(Client, ?BUCKET, Key, Amt) of
        ok ->
            ok;
        {error,req_timedout} ->
            ?LOG_WARNING("Increment timed out"),
            ?LOG_WARNING("Assuming increment has not been made"),
            ok = rhc:counter_incr(Client, ?BUCKET, Key, Amt)
    end.
