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

-module(general_api_perf).
-export([confirm/0, profile/1, confirm_pb/1, confirm_http/1]).

-export([get_clients/3, perf_test/8, request_pause/1, get_bucketprefix/2]).

-import(secondary_index_tests, [http_query/3, pb_query/3]).
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(CLIENT_COUNT, 4).
-define(QUERY_EVERY, 1000).
-define(GET_EVERY, 1).
-define(GETS_PER_GET, 4).
-define(DONT_GET_BEFORE, 10000).
-define(UPDATE_EVERY, 8).
-define(LOG_EVERY, 5000).
-define(KEY_COUNT, 25000).
-define(NODE_COUNT, 1).
-define(VERSION, current).
-define(OBJECT_SIZE_BYTES, 1024).
-define(PROFILE_PAUSE, 10000).
-define(PROFILE_LENGTH, 20).
-define(REQUEST_PAUSE_UPTO, 3).
-define(N_VAL, 3).
-define(ALLOW_MULT, false).
-define(INDEX_ENTRIES, 6).
-define(USE_TYPED_BUCKET, true).
-define(TEST_TYPE, confirm_http). % or confirm_http
-define(PROFILE, false).
-define(MEMORY_TYPES, [total, processes, processes_used, atom, binary, ets]).
-define(MEMORY_PROFILE, true).
-define(MEAN_QUERY_RESULTS, 200).

-define(FIELD_LIST,
    ["bin1", "bin2", "bin3", "bin4", "bin5", "bin6", "bin7", "bin8"]
).

-if(?OTP_RELEASE > 23).
-define(RPC_MODULE, erpc).
-else.
-define(RPC_MODULE, rpc).
-endif.

-define(CONF,
        [
            {riak_kv,
                [
                    {anti_entropy, {off, []}},
                    {delete_mode, keep},
                    {tictacaae_active, active},
                    {tictacaae_parallelstore, leveled_ko}
                ]
            },
            {leveled,
                [
                    {compression_method, zstd}
                ]
            },
            {riak_core,
                [
                    {ring_creation_size, ?DEFAULT_RING_SIZE},
                    {
                        default_bucket_props,
                        [{allow_mult, ?ALLOW_MULT}, {n_val, ?N_VAL}]
                    }
                ]
            }
        ]
       ).

confirm() ->
    [Nodes] = rt:build_clusters([{?NODE_COUNT, ?VERSION, ?CONF}]),
    ?LOG_INFO("Test to be run on nodes ~0p", [Nodes]),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Nodes),
    erlang:apply(?MODULE, ?TEST_TYPE, [hd(Nodes)]).

confirm_pb(Node) ->
    perf_test(Node, riakc_pb_socket, ?CLIENT_COUNT).

confirm_http(Node) ->
    perf_test(Node, rhc, ?CLIENT_COUNT).

perf_test(Node, ClientMod, ClientCount) ->
    Clients = get_clients(ClientCount, Node, ClientMod),
    BucketPrefix = get_bucketprefix(Node, ?USE_TYPED_BUCKET),
    perf_test(
        Node,
        ClientMod,
        Clients,
        BucketPrefix,
        ?KEY_COUNT,
        ?OBJECT_SIZE_BYTES,
        ?PROFILE,
        ?MEMORY_PROFILE
    ).

perf_test(Node, ClientMod, Clients, BP, KeyCount, ObjSize, Profile, MemProf) ->
    Query =
        case rt:get_backends() of
            bitcask ->
                false;
            _ ->
                true
        end,
    perf_test(Node, ClientMod, Clients, BP, KeyCount, ObjSize, Profile, MemProf, Query).

perf_test(Node, ClientMod, Clients, BP, KeyCount, ObjSize, Profile, MemProf, Query) ->
    Buckets =
        case BP of
            {BT, BPrefix} ->
                lists:map(
                    fun(I) ->
                        {
                            BT,
                            list_to_binary(io_lib:format("~s~w", [BPrefix, I]))
                        }
                    end,
                    lists:seq(1, length(Clients))
                );
            BPrefix ->
                lists:map(
                    fun(I) ->
                        list_to_binary(io_lib:format("~s~w", [BPrefix, I]))
                    end,
                    lists:seq(1, length(Clients))
                )
        end,
        
    ClientBPairs = lists:zip(Clients, Buckets),

    TestProcess = self(),
    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({C, B}) ->
            fun() ->
                V = base64:encode(crypto:strong_rand_bytes(ObjSize)),
                lists:foreach(
                    fun(I) ->
                        act(C, ClientMod, B, I, V, Query)
                    end,
                    lists:seq(1, KeyCount)
                ),
                TestProcess ! complete
            end
        end,
    SpawnFuns = lists:map(SpawnUpdateFun, ClientBPairs),
    lists:foreach(fun spawn/1, SpawnFuns),
    Profiler =
        case Profile of
            true ->
                spawn(fun() -> profile(Node) end);
            false ->
                none
        end,
    MemProfiler =
        case MemProf of
            true ->
                spawn(fun() -> memory_profile(Node) end);
            false ->
                none
        end,
    ?LOG_INFO("Profilers spawned ~w ~w", [Profiler, MemProfiler]),

    ok = receive_complete(0, length(Clients)),

    case Profile of
        true ->
            ?LOG_INFO(
                "Sending complete to profiler ~w ~w",
                [Profiler, is_process_alive(Profiler)]
            ),
            Profiler ! complete;
        _ ->
            ok
    end,
    case MemProf of
        true ->
            ?LOG_INFO(
                "Sending complete to memory profiler ~w ~w",
                [MemProfiler, is_process_alive(MemProfiler)]
            ),
            MemProfiler ! complete;
        false ->
            ok
    end,

    close_clients(Clients, ClientMod),

    EndTime = os:system_time(millisecond),
    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),
    pass.

receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive complete ->
        ?LOG_INFO("Received complete ~w of ~w", [T + 1, Target]),
        receive_complete(T + 1, Target)
    end.


get_bucketprefix(_Node, false) ->
    <<"BucketName">>;
get_bucketprefix(Node, true) ->
    rt:create_activate_and_wait_for_bucket_type(
        [Node],
        <<"BucketTypeName">>,
        [{allow_mult, ?ALLOW_MULT}, {n_val, ?N_VAL}]
    ),
    {<<"BucketTypeName">>, <<"BucketName">>}.

get_clients(ClientsPerNode, Node, ClientMod) ->
    lists:map(
        fun(N) ->
            case ClientMod of
                riakc_pb_socket ->
                    rt:pbc(N);
                rhc ->
                    rt:httpc(N)
            end
        end,
        lists:flatten(lists:duplicate(ClientsPerNode, Node))
    ).

close_clients(Clients, ClientMod) ->
    case ClientMod of
        riakc_pb_socket ->
            lists:foreach(
                fun(C) -> riakc_pb_socket:stop(C) end,
                Clients
            );
        rhc ->
            ok
    end.

profile(Node) ->
    receive
        complete ->
            ok
    after ?PROFILE_PAUSE ->
        ?RPC_MODULE:call(Node, riak_kv_util, profile_riak, [?PROFILE_LENGTH]),
        profile(Node)
    end.

memory_profile(Node) ->
    memory_profile(Node, none, 0).

memory_profile(Node, Totals, Loops) ->
    Pause = rand:uniform(?PROFILE_PAUSE * 2),
    receive
        complete ->
            case Loops of
                Loops when Loops > 0 ->
                    MeanTotals =
                        lists:map(
                            fun({N, T}) -> {N, T div (Loops * 1000)} end,
                            Totals
                        ),
                    ?LOG_INFO("Memory used ~0p", [MeanTotals]),
                    ok;
                _ ->
                    ok
            end
    after Pause ->
        MemStats = ?RPC_MODULE:call(Node, erlang, memory, [?MEMORY_TYPES]),
        case Totals of
            none ->
                memory_profile(Node, MemStats, Loops + 1);
            Totals ->
                UpdTotals =
                    lists:map(
                        fun({{N, T0}, {N, TA}}) -> 
                            {N, T0 + TA}
                        end,
                        lists:zip(MemStats, Totals)
                    ),
                memory_profile(Node, UpdTotals, Loops + 1)
            end
    end.

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_index(N) ->
    list_to_binary(io_lib:format("I~8..0B", [N])).

to_meta(N) ->
    list_to_binary(io_lib:format("M~8..0B", [N])).

act(Client, ClientMod, Bucket, I, V, Query) ->
    K = to_key(I),
    Obj = riakc_obj:new(Bucket, K, <<I:32/integer, V/binary>>),
    MD0 = riakc_obj:get_metadata(Obj),
    FieldList = lists:sublist(?FIELD_LIST, ?INDEX_ENTRIES),
    MD1 =
        lists:foldl(
            fun(IdxName, MDAcc) ->
                riakc_obj:set_secondary_index(
                    MDAcc,
                    {{binary_index, IdxName},
                    [to_index(I), to_index(I + 1)]}
                )
            end,
            MD0,
            FieldList
        ),
    MD2 =
        lists:foldl(
            fun(MDEntry, MDAcc) ->
                riakc_obj:set_user_metadata_entry(MDAcc, MDEntry)
            end,
            MD1,
            [{<<"K1">>, to_meta(I)}, {<<"K2">>, to_meta(I + 1)}]
        ),
    case ClientMod:put(Client, riakc_obj:update_metadata(Obj, MD2)) of
        ok ->
            ok;
        PutError ->
            ?LOG_ERROR("Put error ~0p", [PutError])
    end,
    case I rem ?UPDATE_EVERY of
        0  when I > 1000 ->
            UpdK = to_key(rand:uniform(I - 1000)),
            case ClientMod:get(Client, Bucket, UpdK) of
                {ok, PrvObj} ->
                    NewObj =
                        riakc_obj:update_value(PrvObj, <<I:32/integer, V/binary>>),
                    PrvMD = riakc_obj:get_metadata(PrvObj),
                    NewMD =
                        riakc_obj:add_secondary_index(
                            PrvMD,
                            {
                                {binary_index, "upd_index"},
                                [to_index(I),
                                to_index(I+1)]
                            }
                        ),
                    _ =
                        ClientMod:put(
                            Client,
                            riakc_obj:update_metadata(NewObj, NewMD)
                        );
                UpdError ->
                    ?LOG_ERROR("Update error ~0p", [UpdError])
            end;
        _ ->
            ok
    end,
    case I rem ?GET_EVERY of
        0 when I > ?DONT_GET_BEFORE ->
            lists:foreach(
                fun(_I) ->
                    GetResponse =
                        ClientMod:get(
                            Client,
                            Bucket,
                            to_key(rand:uniform(I - 1000))
                        ),
                    case GetResponse of
                        {ok, _PrevObj} ->
                            ok;
                        GetError ->
                            ?LOG_ERROR("Get error ~0p", [GetError])
                    end
                end,
                lists:seq(1, ?GETS_PER_GET)
            );  
        _ ->
            ok
    end,
    case {I rem ?QUERY_EVERY, Query} of
        {0, true} when I > ?QUERY_EVERY ->
            QueryResults = rand:uniform(?MEAN_QUERY_RESULTS div 2),
            QueryPoint = max(1, rand:uniform(I)),
            QueryLow = 1 + max(0, QueryPoint - QueryResults),
            _ActQueryResults = ((QueryPoint - QueryLow) + 1) * 2,
            QueryResult =
                case ClientMod of
                    riakc_pb_socket ->
                        ClientMod:get_index_range(
                            Client,
                            Bucket,
                            {
                                binary_index,
                                lists:nth(
                                    rand:uniform(?INDEX_ENTRIES), FieldList)
                            },
                            to_index(QueryLow), to_index(QueryPoint),
                            []
                        );
                    rhc ->
                        ClientMod:get_index(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(rand:uniform(5), FieldList)},
                            {to_index(QueryLow), to_index(QueryPoint)}
                        )
                end,
            case QueryResult of
                {ok, _} ->
                    ok;
                QueryError ->
                    ?LOG_ERROR("Query error ~0p", [QueryError])
            end;
        _ ->
            ok
    end,
    case I rem ?LOG_EVERY of
        0 ->
            ?LOG_INFO("Client ~p at ~w", [Client, I]);
        _ ->
            ok
    end,
    request_pause(?TEST_TYPE)
    .

request_pause(profile) ->
    timer:sleep(rand:uniform(?REQUEST_PAUSE_UPTO));
request_pause(measure) ->
    ok.
