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
-export([confirm/0, spawn_profile_fun/1, confirm_pb/1, confirm_http/1]).

-export([get_clients/3, perf_test/7]).

-import(secondary_index_tests, [http_query/3, pb_query/3]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(CLIENT_COUNT, 12).
-define(QUERY_EVERY, 100).
-define(GET_EVERY, 1).
-define(UPDATE_EVERY, 2).
-define(LOG_EVERY, 2000).
-define(KEY_COUNT, 20000).
-define(OBJECT_SIZE_BYTES, 512).
-define(PROFILE_PAUSE, 10000).
-define(PROFILE_LENGTH, 50).
-define(REQUEST_PAUSE_UPTO, 3).

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
                    {tictacaae_parallelstore, leveled_ko},
                    {tictacaae_storeheads, true},
                    {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
                    {tictacaae_suspend, true}
                ]
            },
            {leveled,
                [
                    {compaction_runs_perday, 48},
                    {journal_objectcount, 20000}
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
    confirm_pb(Node). % can be changed to confirm_http/1

confirm_pb(Node) ->
    perf_test(Node, riakc_pb_socket, ?CLIENT_COUNT).

confirm_http(Node) ->
    perf_test(Node, rhc, ?CLIENT_COUNT).

perf_test(Node, ClientMod, ClientCount) ->
    Clients = get_clients(ClientCount, Node, ClientMod),
    BucketPrefix = <<"BucketName">>,
    perf_test(
        Node,
        ClientMod,
        Clients,
        BucketPrefix,
        ?KEY_COUNT,
        ?OBJECT_SIZE_BYTES,
        true
    ).

perf_test(Node, ClientMod, Clients, BPrefix, KeyCount, ObjSize, Profile) ->
    Buckets =
        lists:map(
            fun(I) ->
                list_to_binary(io_lib:format("~s~w", [BPrefix, I]))
            end,
            lists:seq(1, length(Clients))
        ),
    ClientBPairs = lists:zip(Clients, Buckets),

    TestProcess = self(),
    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({C, B}) ->
            fun() ->
                V = base64:encode(crypto:strong_rand_bytes(ObjSize)),
                lists:foreach(
                    fun(I) ->
                        act(C, ClientMod, B, I, V)
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
                spawn_profile_fun(Node);
            false ->
                none
        end,

    ok = receive_complete(0, length(Clients)),

    case Profile of
        true ->
            Profiler ! complete;
        _ ->
            ok
    end,

    close_clients(Clients, ClientMod),

    EndTime = os:system_time(millisecond),
    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),
    pass.

receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive complete -> receive_complete(T + 1, Target) end.
    
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


spawn_profile_fun(Node) ->
    spawn(fun() -> profile(Node) end).

profile(Node) ->
    receive
        complete ->
            ok
    after ?PROFILE_PAUSE ->
        ?RPC_MODULE:call(Node, riak_kv_util, profile_riak, [?PROFILE_LENGTH]),
        profile(Node)
    end.


to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_index(N) ->
    list_to_binary(io_lib:format("I~8..0B", [N])).

to_meta(N) ->
    list_to_binary(io_lib:format("M~8..0B", [N])).

act(Client, ClientMod, Bucket, I, V) ->
    K = to_key(I),
    Obj = riakc_obj:new(Bucket, K, <<I:32/integer, V/binary>>),
    MD0 = riakc_obj:get_metadata(Obj),
    FieldList =
        ["bin1", "bin2", "bin3", "bin4", "bin5", "bin6", "bin7", "bin8"],
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
    ok = ClientMod:put(Client, riakc_obj:update_metadata(Obj, MD2)),
    case I rem ?UPDATE_EVERY of
        0  when I > 1000 ->
            UpdK = to_key(rand:uniform(I - 1000)),
            {ok, PrvObj} = ClientMod:get(Client, Bucket, UpdK),
            NewObj =
                riakc_obj:update_value(PrvObj, <<I:32/integer, V/binary>>),
            PrvMD = riakc_obj:get_metadata(PrvObj),
            NewMD =
                riakc_obj:add_secondary_index(
                    PrvMD,
                    {{binary_index, "upd_index"}, [to_index(I), to_index(I+1)]}
                ),
            ok =
                ClientMod:put(
                    Client,
                    riakc_obj:update_metadata(NewObj, NewMD)
                );
        _ ->
            ok
    end,
    case I rem ?GET_EVERY of
        0 when I > 1000 ->
            {ok, _PastObj} =
                ClientMod:get(Client, Bucket, to_key(rand:uniform(I - 1000)));
        _ ->
            ok
    end,
    case I rem ?QUERY_EVERY of
        0 when I > ?QUERY_EVERY ->
            {ok, ?INDEX_RESULTS{keys=HttpResKeys}} =
                case ClientMod of
                    riakc_pb_socket ->
                        ClientMod:get_index_range(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(rand:uniform(5), FieldList)},
                            to_index(I - 99), to_index(I),
                            []
                        );
                    rhc ->
                        ClientMod:get_index(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(rand:uniform(5), FieldList)},
                            {to_index(I - 99), to_index(I)}
                        )
                end,
            ?assertMatch(200, length(HttpResKeys));
        _ ->
            ok
    end,
    case I rem ?LOG_EVERY of
        0 ->
            ?LOG_INFO("Client ~p at ~w", [Client, I]);
        _ ->
            ok
    end,
    timer:sleep(rand:uniform(?REQUEST_PAUSE_UPTO))
    .

