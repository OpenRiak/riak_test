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
%% 
%% A single node test, that exercises the API, and allows for profiling
%% of that API activity

-module(general_counter_perf).
-export([confirm/0, confirm_pb/2]).
-export([test_loop/6]).

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 32).
-define(BUCKET_TYPE, <<"counters">>).
-define(TEST_BUCKET, {?BUCKET_TYPE, <<"TestBucket">>}).
-define(COUNTER_COUNT, 20000).
-define(UPDATE_COUNT, 20000).
-define(CLIENT_COUNT, 100).
-define(LOG_EVERY, 1000).
-define(PROFILE_TEST, false).

-define(CONF,
        [
            {riak_kv,
                [
                    {anti_entropy, {off, []}},
                    {delete_mode, keep},
                    {tictacaae_active, active},
                    {tictacaae_parallelstore, leveled_ko},
                    {tictacaae_storeheads, true},
                    {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
                    {tictacaae_suspend, true},
                    {direct_stats, false}
                ]
            },
            {leveled,
                [
                    {compaction_runs_perday, 24},
                    {journal_objectcount, 20000},
                    {compression_method, zstd}
                ]
            },
            {eleveldb,
                [
                    {compression, lz4}
                ]
            },
            {riak_dt,
                [
                    {binary_compression, false}
                ]
            },
            {riak_core,
                [
                    {ring_creation_size, ?DEFAULT_RING_SIZE}
                ]
            }
        ]
       ).

confirm() ->
    [Node] = rt:build_cluster(1, ?CONF),
    rt:wait_for_service(Node, riak_kv),
    confirm_pb(Node, ?PROFILE_TEST).

confirm_pb(Node, Profile) ->

    rt:create_and_activate_bucket_type(
        Node,
        ?BUCKET_TYPE,
        [{datatype, counter}, {allow_mult, true}]
    ),

    Profiler =
        case Profile of
            true ->
                general_api_perf:spawn_profile_fun(Node);
            false ->
                ok
        end,

    SW = os:timestamp(),
    ReturnPid = self(),
    lists:foreach(
        fun(C) ->
            spawn(
                ?MODULE,
                test_loop,
                [C, ReturnPid, ?COUNTER_COUNT, 0, ?UPDATE_COUNT, {0, 0}]
            )
        end,
        lists:map(fun(_I) -> rt:pbc(Node) end, lists:seq(1, ?CLIENT_COUNT))
    ),
    
    receive_loop(?CLIENT_COUNT),

    TestTime = timer:now_diff(os:timestamp(), SW) div 1000,
    ?LOG_INFO(
        "Test with ~w updates across ~w counters in ~w ms updates persec ~w" ,
        [
            ?UPDATE_COUNT * ?CLIENT_COUNT,
            ?COUNTER_COUNT,
            TestTime,
            trunc((1000 * ?UPDATE_COUNT * ?CLIENT_COUNT) / TestTime)
        ]
    ),
    case Profile of
        true ->
            Profiler ! complete;
        false ->
            ok
    end,
    pass.

receive_loop(0) ->
    ok;
receive_loop(C) when C > 0 ->
    receive
        complete ->
            receive_loop(C - 1)
    end.

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_loop(Client, TestPid, _CC, UC, UC, _TS) ->
    riakc_pb_socket:stop(Client),
    TestPid ! complete;
test_loop(Client, TestPid, CC, C, UC, {TS, MaxTS}) ->
    Key = to_key(rand:uniform(CC)),
    C1 = riakc_counter:increment(rand:uniform(16), riakc_counter:new()),
    {TS0, ok} =
        timer:tc(
            fun() ->
                riakc_pb_socket:update_type(
                    Client,
                    ?TEST_BUCKET,
                    Key,
                    riakc_counter:to_op(C1)
                )
            end
        ),
    UpdTSAcc =
        case C rem ?LOG_EVERY of
            0 when C > 0 ->
                ?LOG_INFO(
                    "~w updates up to accumulated total of ~w took "
                    "mean_micros=~w with max_micros=~w",
                    [
                        ?LOG_EVERY,
                        C,
                        (TS + TS0) div ?LOG_EVERY,
                        max(TS0, MaxTS)
                    ]
                ),
                {0, 0};
            _ ->
                {TS + TS0, max(MaxTS, TS0)}
        end,
    test_loop(Client, TestPid, CC, C + 1, UC, UpdTSAcc).


    