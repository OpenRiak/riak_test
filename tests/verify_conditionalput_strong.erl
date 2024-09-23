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
-module(verify_conditionalput_strong).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DEFAULT_RING_SIZE, 32).
-define(TEST_LOOPS, 32).
-define(NUM_NODES, 6).
-define(CLAIMANT_TICK, 5000).
-define(MAX_RANDOM_SLEEP, 10000).
-define(CLIENTS_PER_NODE, 24).
-define(MAX_CYCLE_TIME, 30000).

-define(CONF(Mult, LWW, CondPutMode, TokenMode),
        [{riak_kv,
          [
            {anti_entropy, {off, []}},
            {delete_mode, keep},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
            {tictacaae_storeheads, true},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {tictacaae_suspend, true},
            {conditional_put_mode, CondPutMode},
            {token_request_mode, TokenMode}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {vnode_inactivity_timeout, 15000},
            {forced_ownership_handoff, 8},
            {handoff_concurrency, 8},
            {claimant_tick, ?CLAIMANT_TICK},
            {choose_claim_fun, choose_claim_v4},
            {default_bucket_props, [{allow_mult, Mult}, {last_write_wins, LWW}]}
          ]}]
       ).

confirm() ->
    Nodes1 =
        rt:build_cluster(
            ?NUM_NODES, ?CONF(false, true, api_only, head_only)
        ),
    build_wait_loop(Nodes1),

    ?LOG_INFO("----------------"),
    ?LOG_INFO("Testing with conditional check at API only"),
    ?LOG_INFO("... no consensus will be achieved with concurrent PUTs"),
    ?LOG_INFO("----------------"),

    false =
        hd(
            test_conditional(
                {weak, lww},
                Nodes1,
                <<"pbcWeak - to fail">>,
                ?TEST_LOOPS,
                riakc_pb_socket
            )
        ),
    false =
        hd(
            test_conditional(
                {weak, lww},
                Nodes1,
                <<"httpWeak - to fail">>,
                ?TEST_LOOPS,
                rhc
            )
        ),

    reset_conditional_cpm(Nodes1, prefer_token),
    ?LOG_INFO("----------------"),
    ?LOG_INFO("Testing with stronger conditional put - but head_only"),
    ?LOG_INFO("Testing without failure, as no consensus used"),
    ?LOG_INFO("----------------"),

    R1 =
        test_conditional(
            {strong, lww},
            Nodes1,
            <<"pbcStrong">>,
            ?TEST_LOOPS,
            riakc_pb_socket,
            false,
            single
        ),
    ?assert(hd(R1)),
    R2 =
        test_conditional(
            {strong, lww},
            Nodes1,
            <<"httpStrong">>,
            ?TEST_LOOPS,
            rhc,
            false,
            single
        ),
    ?assert(hd(R2)),

    true = test_nonematch(Nodes1, <<"pbcNoneMatch">>, riakc_pb_socket),
    true = test_nonematch(Nodes1, <<"httpNoneMatch">>, rhc),

    rt:clean_cluster(Nodes1),

    Nodes3 =
        rt:build_cluster(
            ?NUM_NODES, ?CONF(true, false, prefer_token, primary_consensus)
        ),
    build_wait_loop(Nodes3),

    [N3|RestNodes3] = Nodes3,
    Me = self(),

    ?LOG_INFO("----------------"),
    ?LOG_INFO("Testing with stronger conditional put and consensus"),
    ?LOG_INFO("Testing concurrent to failure and cluster change"),
    ?LOG_INFO("----------------"),

    R3 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"AllUpNodeTest">>,
            ?TEST_LOOPS,
            riakc_pb_socket
        ),
    ?assert(hd(R3)),
    
    R4 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"AllUpNodeTestHTTP">>,
            ?TEST_LOOPS,
            rhc
        ),
    ?assert(hd(R4)),
    
    true = test_nonematch(Nodes3, <<"pbcNoneMatch">>, riakc_pb_socket),
    true = test_nonematch(Nodes3, <<"httpNoneMatch">>, rhc),

    spawn_stop(N3, Me),
    R5 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"StopNodeTest">>,
            ?TEST_LOOPS,
            riakc_pb_socket,
            true,
            four
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),
    ?assert(hd(R5)),

    spawn_start(N3, Me),
    R6 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"StartNodeTest">>,
            ?TEST_LOOPS * 6,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),
    ?assert(hd(R6)),

    spawn_leave(N3, RestNodes3, Me),
    R7 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"LeaveNodeTest">>,
            ?TEST_LOOPS * 8,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),
    ?assert(hd(R7)),

    spawn_join(N3, RestNodes3, Me),
    R8 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"JoinNodeTest">>,
            ?TEST_LOOPS * 6,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),
    ?assert(hd(R8)),

    spawn_kill(N3, Me),
    R9 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"BrutalKillNodeTest">>,
            ?TEST_LOOPS,
            riakc_pb_socket,
            true,
            four
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),
    ?assert(hd(R9)),

    spawn_start(N3, Me),
    R10 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"RestartNodeTest">>,
            ?TEST_LOOPS * 6,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),
    ?assert(hd(R10)),

    spawn_kill(N3, Me),
    R11 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"BrutalReKillNodeTest">>,
            ?TEST_LOOPS,
            riakc_pb_socket,
            true,
            four
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),
    ?assert(hd(R11)),

    spawn_start(N3, Me),
    R12 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"ReResstartNodeTest">>,
            ?TEST_LOOPS * 6,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),
    ?assert(hd(R12)),

    reset_conditional_trm([N3] ++ RestNodes3, basic_consensus),
    ?LOG_INFO("----------------"),
    ?LOG_INFO("Testing with reduced stronger_conditional_nval"),
    ?LOG_INFO("----------------"),

    spawn_kill(N3, Me),
    R13 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"BrutalReKillNodeTestN3">>,
            ?TEST_LOOPS,
            riakc_pb_socket,
            true,
            four
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),
    ?assert(hd(R13)),

    spawn_start(N3, Me),
    R14 =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"ReResstartNodeTestN3">>,
            ?TEST_LOOPS * 6,
            riakc_pb_socket
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),
    ?assert(hd(R14)),

    ?LOG_INFO("----------------"),
    ?LOG_INFO("Results summary"),
    ?LOG_INFO("----------------"),
    lists:foreach(
        fun(R) ->
            ?LOG_INFO(
                "Result ~w for test ~p ~p average time ~w max time ~w",
                R
            )
        end,
        [R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14]
    ),

    pass.

reset_conditional_trm([], _TRM) ->
    ok;
reset_conditional_trm([Node|Rest], TRM) ->
    ok =
        rpc:call(
            Node,
            application,
            set_env,
            [riak_kv, token_request_mode, TRM]
        ),
reset_conditional_trm(Rest, TRM).

reset_conditional_cpm([], _CPM) ->
    ok;
reset_conditional_cpm([Node|Rest], CPM) ->
    ok =
        rpc:call(
            Node,
            application,
            set_env,
            [riak_kv, conditional_put_mode, CPM]
        ),
    reset_conditional_cpm(Rest, CPM).

test_nonematch(Nodes, Bucket, ClientMod) ->
    ClientsPerNode = 10,

    Clients = get_clients(ClientsPerNode, Nodes, ClientMod),
    
    ?LOG_INFO("----------------"),
    ?LOG_INFO(
        "Testing none_match condition on PUTs - parallel clients ~s client ~w",
        [Bucket, ClientMod]
    ),
    ?LOG_INFO("----------------"),

    StartTime = os:system_time(millisecond),

    TestProcess = self(),

    SpawnUpdateFun =
        fun({_I, C}) ->
            fun() ->
                R =
                    ClientMod:put(
                        C,
                        riakc_obj:new(Bucket, to_key(1), <<0:32/integer>>),
                        [if_none_match]
                    ),
                true = check_nomatch_conflict(ClientMod, R),
                {ok, FinalObj} = ClientMod:get(C, Bucket, to_key(1)),
                <<0:32/integer>> = riakc_obj:get_value(FinalObj),
                TestProcess ! complete
            end
        end,
    lists:foreach(
        fun(ClientRef) -> spawn(SpawnUpdateFun(ClientRef)) end,
        Clients),

    ok = receive_complete(0, length(Clients)),

    EndTime = os:system_time(millisecond),

    close_clients(Clients, ClientMod),

    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),

    true.


test_conditional(Type, Nodes, Bucket, Loops, ClientMod) ->
    test_conditional(Type, Nodes, Bucket, Loops, ClientMod, false, four).

test_conditional(Type, Nodes, Bucket, Loops, ClientMod, KillScenario, Multi) ->
    ClientsPerNode =
        case Multi of
            four ->
                ?CLIENTS_PER_NODE;
            single ->
                ?CLIENTS_PER_NODE div 2
        end,

    Clients = get_clients(ClientsPerNode, Nodes, ClientMod),
    
    ?LOG_INFO("----------------"),
    ?LOG_INFO(
        "Testing with ~w condition on PUTs - parallel clients ~s client ~w",
        [Type, Bucket, ClientMod]
    ),
    ?LOG_INFO("----------------"),

    Keys =
        case Multi of
            four ->
                lists:map(
                    fun(I) ->
                        I4 = 4 * I,
                        [
                            to_key(I4),
                            to_key(I4 - 1),
                            to_key(I4 - 2),
                            to_key(I4 - 3)
                        ]
                    end,
                    lists:seq(1, Loops)
                );
            single ->
                lists:map(fun(I) -> [to_key(I)] end, lists:seq(1, Loops))
        end,

    Results =
        lists:map(
            fun(KeysPerRun) ->
                test_concurrent_conditional_changes(
                    Bucket, KeysPerRun, Clients, ClientMod, KillScenario
                )
            end,
            Keys
        ),

    close_clients(Clients, ClientMod),

    % print_stats(hd(Nodes)),
    
    NCount = length(Nodes),
    %% Total should be n(n+1)/2
    Expected =
        ((ClientsPerNode * NCount) * (ClientsPerNode * NCount + 1)) div 2,
    {FinalValues, Timings} = lists:unzip(Results),
    MeanTimings = lists:sum(Timings) div length(Timings),
    MaxTimings = lists:max(Timings),
    ?LOG_INFO(
        "Average time per result ~w ms",
        [MeanTimings]
    ),
    ?LOG_INFO(
        "Maximum time per result ~w ms",
        [MaxTimings]
    ),

    ?assert(check_no_tokens(Nodes)),

    ?assert(MaxTimings < ?MAX_CYCLE_TIME),

    [
        lists:all(fun(R) -> R == Expected end, FinalValues),
        Bucket,
        ClientMod,
        MeanTimings,
        MaxTimings
    ].

test_concurrent_conditional_changes(
        Bucket, KeysPerRun, Clients, ClientMod, KillScenario) ->
    [{1, C1}|_Rest] = Clients,

    ok =
        lists:foreach(
            fun(Key) ->
                ClientMod:put(C1, riakc_obj:new(Bucket, Key, <<0:32/integer>>))
            end,
            KeysPerRun
        ),
    TestProcess = self(),

    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({I, C}) ->
            fun() ->
                Key =
                    case KeysPerRun of
                        [K] ->
                            K;
                        KeyList when length(KeyList) == 4 ->
                            lists:nth(
                                (I rem 4) + 1,
                                KeyList
                            )
                    end,
                R =
                    try_conditional_put(
                        ClientMod, C, I, Bucket, Key, KillScenario
                    ),
                case R of
                    ok ->
                        TestProcess ! complete;
                    error ->
                        TestProcess ! error
                end
            end
        end,
    lists:foreach(
        fun(ClientRef) -> spawn(SpawnUpdateFun(ClientRef)) end,
        Clients),
    
    ok = receive_complete(0, length(Clients)),
    EndTime = os:system_time(millisecond),

    FinalValue =
        lists:sum(
            lists:map(
                fun(Key) ->
                    {ok, FinalObj} =
                        ClientMod:get(C1, Bucket, Key, [{r, 3}, {pr, 2}]),
                    <<FinalV:32/integer>> = riakc_obj:get_value(FinalObj),
                    FinalV
                end,
                KeysPerRun
            )
        ),
    
    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),
    ?LOG_INFO("Test had final value of ~w", [FinalValue]),
    
    {FinalValue, EndTime - StartTime}.


receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive
        complete -> receive_complete(T + 1, Target);
        error -> error
    end.

try_conditional_put(ClientMod, C, I, B, K, KillScenario) ->
    {ok, Obj} =
        case ClientMod:get(C, B, K, [{timeout, 17000}]) of
            {ok, FetchedObj} ->
                {ok, FetchedObj};
            R ->
                ?LOG_INFO("Request error ~p from client ~p", [R, C]),
                R
        end,
    <<V:32/integer>> = 
        try
            riakc_obj:get_value(Obj)
        catch
            exit:siblings ->
                ?LOG_INFO("Siblings on ~p ~p ~p", [C, B, K]),
                <<>>
        end,

    Obj1 = riakc_obj:update_value(Obj, <<(V + I):32/integer>>),
    PutRsp = ClientMod:put(C, Obj1, [if_not_modified, {timeout, 23000}]),
    case check_match_conflict(ClientMod, PutRsp, KillScenario) of
        true ->
            try_conditional_put(ClientMod, C, I, B, K, KillScenario);
        false ->
            ok;
        error ->
            error
    end.

check_match_conflict(riakc_pb_socket, {error, <<"modified">>}, _) ->
    true;
check_match_conflict(rhc, {error, {ok, "409", _Headers, _Message}}, _) ->
    true;
check_match_conflict(_, ok, _) ->
    false;
check_match_conflict(riakc_pb_socket, {error, <<"session_remote_exit">>}, true) ->
    true;
check_match_conflict(riakc_pb_socket, {error, <<"session_noconnection">>}, true) ->
    true;
check_match_conflict(ClientMod, Response, _) ->
    lager:error("Unexpected: ~w ~w", [ClientMod, Response]),
    error.

check_nomatch_conflict(riakc_pb_socket, {error, <<"match_found">>}) ->
    true;
check_nomatch_conflict(rhc, {error, {ok, "412", _Headers, _Message}}) ->
    true;
check_nomatch_conflict(_, ok) ->
    true.

to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

spawn_stop(Node, P) ->
    F =
        fun() ->
            random_sleep(),
            rt:stop_and_wait(Node),
            change_complete(P)
        end,
    spawn(F).

spawn_start(Node, P) ->
    F =
        fun() ->
            random_sleep(),
            rt:start_and_wait(Node),
            build_wait_loop([Node]),
            change_complete(P)
        end,
    spawn(F).

spawn_leave(Node, Rest, P) ->
    F =
        fun() ->
            random_sleep(),
            ok = rt:staged_leave(Node),
            rt:wait_until_ring_converged(Rest),
            rt:plan_and_commit(Node),
            rt:wait_until_ring_converged([Node|Rest]),
            lists:foreach(fun(N) -> rt:wait_until_ready(N) end, Rest),
            ?LOG_INFO("Sleeping claimant_tick before checking transfer progress"),
            timer:sleep(?CLAIMANT_TICK),
            ok = rt:wait_until_transfers_complete(Rest),
            lists:foreach(
                fun(N) -> rt:wait_until_node_handoffs_complete(N) end,
                Rest),
            ok = rt:wait_until_transfers_complete(Rest),
            rt:wait_until_unpingable(Node),
            change_complete(P)
        end,
    spawn(F).

spawn_join(Node, Rest, P) ->
    F =
        fun() ->
            random_sleep(),
            rt:start_and_wait(Node),
            ok = rt:staged_join(Node, hd(Rest)),
            rt:wait_until_ring_converged(Rest),
            rt:plan_and_commit(Node),
            rt:wait_until_ring_converged([Node|Rest]),
            lists:foreach(fun(N) -> rt:wait_until_ready(N) end, Rest),
            ?LOG_INFO("Sleeping claimant_tick before checking transfer progress"),
            timer:sleep(?CLAIMANT_TICK),
            ok = rt:wait_until_transfers_complete(Rest),
            lists:foreach(
                fun(N) -> rt:wait_until_node_handoffs_complete(N) end,
                Rest),
            ok = rt:wait_until_transfers_complete(Rest),
            rt:wait_until_pingable(Node),
            change_complete(P)
        end,
    spawn(F).

spawn_kill(Node, P) ->
    F = fun() -> random_sleep(), rt:brutal_kill(Node), change_complete(P) end,
    spawn(F).


change_complete(P) ->
    ?LOG_INFO("Spawned node change complete"),
    P ! node_change_complete.

random_sleep() ->
    timer:sleep(rand:uniform(?MAX_RANDOM_SLEEP)).

get_clients(ClientsPerNode, Nodes, ClientMod) ->
    lists:zip(
        lists:seq(1, ClientsPerNode * length(Nodes)),
        lists:map(
            fun(N) ->
                case ClientMod of
                    riakc_pb_socket ->
                        rt:pbc(N);
                    rhc ->
                        rt:httpc(N)
                end
            end,
            lists:flatten(lists:duplicate(ClientsPerNode, Nodes)))
    ).

close_clients(Clients, ClientMod) ->
    case ClientMod of
        riakc_pb_socket ->
            lists:foreach(
                fun({_I, C}) -> riakc_pb_socket:stop(C) end,
                Clients
            );
        rhc ->
            ok
    end.


build_wait_loop(Nodes) ->
    rt:wait_until_transfers_complete(Nodes),
    wait_until_node_handoffs_complete(Nodes),
    timer:sleep(10000),
    rt:wait_until_transfers_complete(Nodes),
    wait_until_node_handoffs_complete(Nodes).


wait_until_node_handoffs_complete([]) ->
    ok;
wait_until_node_handoffs_complete([Node0|Rest]) ->
    ?LOG_INFO("Wait until Node's transfers complete ~p", [Node0]),
    F = fun(Node) ->
                Handoffs = rpc:call(Node, riak_core_handoff_manager, status, [{direction, outbound}]),
                ?LOG_INFO("Handoffs: ~p", [Handoffs]),
                Handoffs =:= []
        end,
    ?assertEqual(ok, rt:wait_until(Node0, F)),
    wait_until_node_handoffs_complete(Rest).


check_no_tokens(Nodes) ->
    check_no_tokens(Nodes, []).

check_no_tokens([], []) ->
    true;
check_no_tokens([], Failures) ->
    Failures;
check_no_tokens([Node|Rest], Result) ->
    case rpc:call(Node, riak_kv_token_manager, stats, []) of
        {0, 0, 0} ->
            check_no_tokens(Rest, Result);
        Other ->
            check_no_tokens(Rest, [{Node, Other}|Result])
    end.
