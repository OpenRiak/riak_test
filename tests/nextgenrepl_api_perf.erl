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

-module(nextgenrepl_api_perf).
-export([confirm/0, confirm_pb/2, confirm_http/2]).

-import(secondary_index_tests, [http_query/3, pb_query/3]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(CLIENT_COUNT, 4).
-define(QUERY_EVERY, 5000).
-define(GET_EVERY, 1).
-define(LOG_EVERY, 10000).
-define(KEY_COUNT, 50000).
-define(OBJECT_SIZE_BYTES, 32768).
-define(REQUEST_PAUSE_UPTO, 3).
-define(SNK_WORKERS, 16).
-define(FIELD_LIST, ["bin1"]).

-if(?OTP_RELEASE > 23).
-define(RPC_MODULE, erpc).
-else.
-define(RPC_MODULE, rpc).
-endif.

-define(CONFIG(SrcQueueDefns, WireCompress),
        [{riak_kv,
          [
            {anti_entropy, {off, []}},
            {delete_mode, keep},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
            {tictacaae_storeheads, true},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {tictacaae_suspend, true},
            {delete_mode, keep},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_compressonwire, WireCompress}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {default_bucket_props, [{allow_mult, true}, {n_val, 1}]}
          ]}]
       ).

-define(SNK_CONFIG(ClusterName, PeerList), 
    [{riak_kv, 
        [{replrtq_enablesink, true},
            {replrtq_sinkqueue, ClusterName},
            {replrtq_sinkpeers, PeerList},
            {replrtq_sinkworkers, ?SNK_WORKERS}]}]).

confirm() ->
    ClusterASrcQ = "cluster_b:any",
    ClusterBSrcQ = "cluster_a:any",

    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {1, ?CONFIG(ClusterASrcQ, true)},
            {1, ?CONFIG(ClusterBSrcQ, true)}
        ]),
    
    confirm_pb(ClusterA, ClusterB)
        % or use confirm_http/2 to test with HTTP API
    .


confirm_pb(ClusterA, ClusterB) ->
    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfig =
        fun(Node, Acc) ->
            {pb, {IP, Port}} = lists:keyfind(pb, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":pb"
        end,
    reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB),

    perf_test(hd(ClusterA), riakc_pb_socket, ?CLIENT_COUNT),

    get_memory(hd(ClusterA)),
    get_memory(hd(ClusterB)),

    pass.

confirm_http(ClusterA, ClusterB) ->
    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfig =
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":http"
        end,
    reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB),

    perf_test(hd(ClusterA), rhc, ?CLIENT_COUNT),

    get_memory(hd(ClusterA)),
    get_memory(hd(ClusterB)),

    pass.

reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB) ->
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end,
        ClusterA
    ),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end,
        ClusterB
    ).

get_memory(Node) ->
    MemStats = ?RPC_MODULE:call(Node, erlang, memory, []),
    ?LOG_INFO("MemStats for Node ~w ~p", [Node, MemStats]),
    Top10 = ?RPC_MODULE:call(Node, riak_kv_util, top_n_binary_total_memory, [10]),
    ?LOG_INFO("Top 10 processes for binary memory:"),
    lists:foreach(
        fun(BinProc) -> ?LOG_INFO("Process memory stat ~p", [BinProc]) end,
        Top10
    ),
    ?LOG_INFO("Top 50 processes for binary - summary by initial call"),
    Top50Summary =
        ?RPC_MODULE:call(
            Node, riak_kv_util, summarise_binary_memory_by_initial_call, [50]
        ),
    lists:foreach(fun(ICSumm) -> ?LOG_INFO("~p", [ICSumm]) end, Top50Summary),
    Top5 = ?RPC_MODULE:call(Node, riak_kv_util, top_n_process_total_memory, [5]),
    ?LOG_INFO("Top 5 processes for process memory:"),
    lists:foreach(
        fun(BinProc) -> ?LOG_INFO("Process memory stat ~p", [BinProc]) end,
        Top5
    ),
    ?LOG_INFO("Top 100 processes for process memory - summary by initial call"),
    Top100Summary =
        ?RPC_MODULE:call(
            Node, riak_kv_util, summarise_process_memory_by_initial_call, [100]
        ),
    lists:foreach(fun(ICSumm) -> ?LOG_INFO("~p", [ICSumm]) end, Top100Summary).

perf_test(Node, ClientMod, ClientCount) ->
    Clients = get_clients(ClientCount, Node, ClientMod),
    Buckets =
        lists:map(
            fun(I) -> list_to_binary(io_lib:format("BucketName~w", [I])) end,
            lists:seq(1, ClientCount)
        ),
    ClientBPairs = lists:zip(Clients, Buckets),

    TestProcess = self(),
    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({C, B}) ->
            fun() ->
                V = crypto:strong_rand_bytes(?OBJECT_SIZE_BYTES),
                lists:foreach(
                    fun(I) ->
                        act(C, ClientMod, B, I, V)
                    end,
                    lists:seq(1, ?KEY_COUNT)
                ),
                TestProcess ! complete
            end
        end,
    SpawnFuns = lists:map(SpawnUpdateFun, ClientBPairs),
    lists:foreach(fun spawn/1, SpawnFuns),

    Profiler = spawn(fun() -> general_api_perf:profile(Node) end),

    ok = receive_complete(0, length(Clients)),

    Profiler ! complete,

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


to_key(N) ->
    list_to_binary(
        io_lib:format(
            "K~8..0B-ExtendedKey"
            "ToMakeThisKeyAtLeast64Bytes"
            "InExpectationThatThisChangesBinaryHandling"
        ,
        [N])
    ).

to_index(N) ->
    list_to_binary(
        io_lib:format(
            "I~8..0B-UnExtendedKey"
        ,
        [N])
    ).

to_meta(N) ->
    list_to_binary(io_lib:format("M~8..0B", [N])).

act(Client, ClientMod, Bucket, I, V) ->
    K = to_key(I),
    Obj = riakc_obj:new(Bucket, K, <<I:32/integer, V/binary>>),
    MD0 = riakc_obj:get_metadata(Obj),
    FieldList = ?FIELD_LIST,
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
                                lists:nth(
                                    rand:uniform(length(FieldList)),
                                    FieldList
                                )
                            },
                            to_index(I - 99), to_index(I),
                            []
                        );
                    rhc ->
                        ClientMod:get_index(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(
                                    rand:uniform(length(FieldList)),
                                    FieldList
                                )
                            },
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
