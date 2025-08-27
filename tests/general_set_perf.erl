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

-module(general_set_perf).
-export([confirm/0, confirm_pb/2]).

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(BUCKET_TYPE, <<"sets">>).
-define(TEST_BUCKET, {?BUCKET_TYPE, <<"TestBucket">>}).
-define(SINGLE_KEY, <<"SingleKey">>).
-define(MEMBER_COUNT, 10000).


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
                    {tictacaae_suspend, true}
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
                    {ring_creation_size, ?DEFAULT_RING_SIZE},
                    {default_bucket_props, [{allow_mult, true}, {n_val, 1}]}
                ]
            }
        ]
       ).

confirm() ->
    [Node] = rt:build_cluster(1, ?CONF),
    rt:wait_for_service(Node, riak_kv),
    confirm_pb(Node, false). % can be changed to confirm_http/1

confirm_pb(Node, Profile) ->

    rt:create_and_activate_bucket_type(
        Node,
        ?BUCKET_TYPE,
        [{datatype, set}, {allow_mult, true}]
    ),
    
    Client = rt:pbc(Node),

    Members =
        lists:map(
            fun(I) ->
                iolist_to_binary(
                    io_lib:format(
                        "~sI~8..0B",
                        [base64:encode_to_string(crypto:strong_rand_bytes(16)), I]
                    )
                )
            end,
            lists:seq(1, ?MEMBER_COUNT)
        ),
    
    Profiler =
        case Profile of
            true ->
                general_api_perf:spawn_profile_fun(Node);
            false ->
                ok
        end,
    
    {TC, _} =
        timer:tc(
            fun() ->
                test_loop(Client, ?SINGLE_KEY, Members, 0, {0, 0, 0})
            end
        ),
    ?LOG_INFO(
        "Test with ~w members completed in ~w ms" ,
        [?MEMBER_COUNT, TC div 1000]
    ),
    case Profile of
        true ->
            Profiler ! complete;
        false ->
            ok
    end,
    pass.

test_loop(Client, _Key, [], _C, _TS) ->
    riakc_pb_socket:stop(Client);
test_loop(Client, Key, [Next|RestMembers], 0, TS) ->
    S0 = riakc_set:new(),
    S1 = riakc_set:add_element(Next, S0),
    ok =
        riakc_pb_socket:update_type(
            Client,
            ?TEST_BUCKET,
            Key,
            riakc_set:to_op(S1)
        ),
    test_loop(Client, Key, RestMembers, 1, TS);
test_loop(Client, Key, [Next|RestMembers], C, {TS, MaxTS, FetchTS}) ->
    {FetchTime, {ok, S0}} =
        timer:tc(
            fun() -> riakc_pb_socket:fetch_type(Client, ?TEST_BUCKET, Key) end
        ),
    S1 = riakc_set:add_element(Next, S0),
    {TS0, ok} =
        timer:tc(
            fun() ->
                riakc_pb_socket:update_type(
                    Client,
                    ?TEST_BUCKET,
                    Key,
                    riakc_set:to_op(S1)
                )
            end
        ),
    UpdTSAcc =
        case C rem 100 of
            0 ->
                ?LOG_INFO(
                    "100 updates with size up to ~w "
                    "took mean_ms=~w with max_ms=~w fetch mean_ms=~w",
                    [
                        C,
                        (TS + TS0) div (100 * 1000),
                        MaxTS div 1000,
                        (FetchTS + FetchTime) div (100 * 1000)
                    ]
                ),
                {0, 0, 0};
            _ ->
                {TS + TS0, max(MaxTS, TS0), FetchTS + FetchTime}
        end,
    test_loop(Client, Key, RestMembers, C + 1, UpdTSAcc).


    