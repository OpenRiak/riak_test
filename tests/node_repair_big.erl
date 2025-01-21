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

-module(node_repair_big).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(general_api_perf, [perf_test/7, get_clients/3]).
-import(verify_tictac_aae, [wipe_out_partition/2]).

-define(DEFAULT_RING_SIZE, 32).
-define(CLIENT_COUNT_PERNODE, 1).
-define(KEY_COUNT, 20000).
-define(OBJECT_SIZE_BYTES, 512).
-define(NODE_COUNT, 4).
-define(PROFILE_PAUSE, 500).
-define(PROFILE_LENGTH, 100).

-if(?OTP_RELEASE > 23).
-define(RPC_MODULE, erpc).
-else.
-define(RPC_MODULE, rpc).
-endif.

-define(CONF,
        [
            {riak_kv,
                [
                    {repair_deferred,           true},
                    {anti_entropy,              {off, []}},
                    {delete_mode,               keep},
                    {tictacaae_active,          active},
                    {tictacaae_parallelstore,   leveled_ko},
                    {tictacaae_storeheads,      true},
                    {tictacaae_rebuildtick,     3600000},
                    {tictacaae_suspend,         true}
                ]
            },
            {leveled,
                [
                    {compaction_runs_perday,    48},
                    {journal_objectcount,       50000},
                    {compression_method,        zstd}
                ]
            },
            {riak_core,
                [
                    {ring_creation_size,        ?DEFAULT_RING_SIZE},
                    {default_bucket_props,      [{allow_mult, false}, {n_val, 3}]},
                    {handoff_concurrency,       4},
                    {forced_ownership_handoff,  8},
                    {vnode_inactivity_timeout,  4000},
                    {vnode_management_timer,    4000},
                    {repair_span,               double_pair}
                ]
            }
        ]
       ).

confirm() ->
    Nodes = rt:build_cluster(?NODE_COUNT, ?CONF),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Nodes),
    node_repair_test(Nodes).

node_repair_test(Nodes) when is_list(Nodes), length(Nodes) > 2 ->
    ?LOG_INFO("Commencing initial load for repair test"),
    Clients = 
        lists:flatten(
            lists:map(
                fun(N) ->
                    get_clients(?CLIENT_COUNT_PERNODE, N, riakc_pb_socket)
                end,
                [hd(Nodes)]
            )
        ),
    ?LOG_INFO(
        "Load ~w using ~w clients",
        [?KEY_COUNT, Clients]
    ),
    perf_test(
        hd(Nodes),
        riakc_pb_socket,
        Clients,
        <<"Bucket1">>,
        ?KEY_COUNT,
        ?OBJECT_SIZE_BYTES,
        false
    ),
    NodeToFail = lists:last(Nodes),
    ?LOG_INFO("Picked a node to fail - ~w", [NodeToFail]),
    TheirPartitions = get_partitions_for_node(NodeToFail),
    rt:stop_and_wait(NodeToFail),
    Clients2 = 
        lists:flatten(
            lists:map(
                fun(N) ->
                    get_clients(?CLIENT_COUNT_PERNODE, N, riakc_pb_socket)
                end,
                [hd(Nodes)]
            )
        ),
    KeyCount2 = ?KEY_COUNT div 2,
    ?LOG_INFO(
        "Load ~w without ~w using ~w clients",
        [KeyCount2, NodeToFail, Clients2]
    ),
    perf_test(
        hd(Nodes),
        riakc_pb_socket,
        Clients2,
        <<"Bucket2">>,
        KeyCount2,
        ?OBJECT_SIZE_BYTES,
        false
    ),

    lists:foreach(
        fun(P) -> wipe_out_partition(NodeToFail, P) end, TheirPartitions),
    rt:start_and_wait(NodeToFail),
    rt:wait_until_transfers_complete(Nodes),
    rt:wait_until_node_handoffs_complete(NodeToFail),
    
    KeyCount3 = ?KEY_COUNT div 4,
    Clients3 = 
        lists:flatten(
            lists:map(
                fun(N) ->
                    get_clients(?CLIENT_COUNT_PERNODE, N, riakc_pb_socket)
                end,
                [hd(Nodes)]
            )
        ),
    ?LOG_INFO(
        "Load ~w with ~w using ~w clients",
        [KeyCount3, NodeToFail, Clients3]
    ),
    perf_test(
        hd(Nodes),
        riakc_pb_socket,
        Clients3,
        <<"Bucket3">>,
        KeyCount3,
        ?OBJECT_SIZE_BYTES,
        false
    ),

    ?LOG_INFO("Calling for node to be repaired"),
    ?RPC_MODULE:call(NodeToFail, riak_client, repair_node, []),

    ExpectedKeyCount =
        ?KEY_COUNT * length(Clients) +
            KeyCount2 * length(Clients2) +
            KeyCount3 * length(Clients3),
    ?LOG_INFO("Tracking repair transfers is hard - wait until count is good"),

    P = spawn_profile_fun(hd(Nodes)),
    ok = wait_for_all_handoffs_and_repairs([NodeToFail]),
    P ! complete,

    rt:wait_until(fun() -> ExpectedKeyCount == count_all_keys(NodeToFail) end),
    ?LOG_INFO("Now double-check it wasn't a fluke"),
    {TC, AllKeyCount} = timer:tc(fun() -> count_all_keys(NodeToFail) end),
    ?LOG_INFO("Counted ~w keys in ~w ms", [AllKeyCount, TC div 1000]),
    ?assertMatch(
        ExpectedKeyCount, 
        AllKeyCount
        ),
    pass
    .

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

get_partitions_for_node(Node) ->
    {ok, Ring} =
        ?RPC_MODULE:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = ?RPC_MODULE:call(Node, riak_core_ring, all_owners, [Ring]),
    TheirPartitions =
        lists:filtermap(
            fun({P, N}) -> case N of Node -> {true, P}; _ -> false end end,
        Owners
        ),
    ?LOG_INFO("Node ~w owns indexes ~0p", [Node, TheirPartitions]),
    TheirPartitions.

count_all_keys(Node) ->
    PB = rt:pbc(Node),
    {ok, Buckets} = riakc_pb_socket:aae_list_buckets(PB, 3),
    AllKeyCount =
        lists:sum(
            lists:map(
                fun(B) ->
                    {ok, Count} =
                        riakc_pb_socket:aae_erase_keys(
                            PB, B, all, all, all, count),
                    Count
                end,
                Buckets
            )
        ),
    AllKeyCount.

wait_for_all_handoffs_and_repairs([]) ->
    ok;
wait_for_all_handoffs_and_repairs([N|Rest]) ->
    HOs = ?RPC_MODULE:call(N, riak_core_vnode_manager, all_handoffs, []),
    ?LOG_INFO("Vnode manager on ~w reports ~0p", [N, HOs]),
    timer:sleep(4000),
    case length(HOs) of
        0 ->
            HOsUpd =
                ?RPC_MODULE:call(
                    N, riak_core_vnode_manager, all_handoffs, []),
            ?LOG_INFO("Vnode manager on ~w reports ~0p", [N, HOsUpd]),
            case length(HOsUpd) of
                0 ->
                    wait_for_all_handoffs_and_repairs(Rest);
                _ ->
                    wait_for_all_handoffs_and_repairs([N|Rest])
            end;
        _ ->
            wait_for_all_handoffs_and_repairs([N|Rest])
    end.            

