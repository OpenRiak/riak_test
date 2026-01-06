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

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(BUCKET_TYPE, <<"counters">>).
-define(TEST_BUCKET, {?BUCKET_TYPE, <<"TestBucket">>}).
-define(COUNTER_COUNT, 10000).
-define(UPDATE_COUNT, 250000).


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
                    {direct_stats, true}
                ]
            },
            {leveled,
                [
                    {compaction_runs_perday, 48},
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
    confirm_pb(Node, true).

confirm_pb(Node, Profile) ->

    rt:create_and_activate_bucket_type(
        Node,
        ?BUCKET_TYPE,
        [{datatype, counter}, {allow_mult, true}]
    ),
    
    Client = rt:pbc(Node),

    Profiler =
        case Profile of
            true ->
                spawn(fun() -> general_api_perf:profile(Node) end);
            false ->
                ok
        end,
    
    {TC, _} =
        timer:tc(
            fun() ->
                test_loop(Client, ?COUNTER_COUNT, 0, ?UPDATE_COUNT, {0, 0})
            end
        ),
    ?LOG_INFO(
        "Test with ~w updates to ~w counters in ~w ms" ,
        [?UPDATE_COUNT, ?COUNTER_COUNT, TC div 1000]
    ),
    case Profile of
        true ->
            Profiler ! complete;
        false ->
            ok
    end,
    pass.

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_loop(Client, _CC, UC, UC, _TS) ->
    riakc_pb_socket:stop(Client);
test_loop(Client, CC, C, UC, {TS, MaxTS}) ->
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
        case C rem 1000 of
            0 ->
                ?LOG_INFO(
                    "1000 updates of ~w with to counter took "
                    "mean_micros=~w with max_micros=~w",
                    [
                        C,
                        (TS + TS0) div 1000,
                        max(TS0, MaxTS)
                    ]
                ),
                {0, 0};
            _ ->
                {TS + TS0, max(MaxTS, TS0)}
        end,
    test_loop(Client, CC, C + 1, UC, UpdTSAcc).


    