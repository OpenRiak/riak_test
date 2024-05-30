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
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 64).
-define(TEST_LOOPS, 32).
-define(NUM_NODES, 6).
-define(CLAIMANT_TICK, 5000).
-define(MAX_RANDOM_SLEEP, 10000).

-define(CONF(Mult, LWW, Strong),
        [{riak_kv,
          [
            {anti_entropy, {off, []}},
            {delete_mode, keep},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
            {tictacaae_storeheads, true},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {tictacaae_suspend, true},
            {claimant_tick, ?CLAIMANT_TICK},
            {vnode_management_timer, 2000},
            {vnode_inactivity_timeout, 4000},
            {forced_ownership_handoff, 16},
            {handoff_concurrency, 16},
            {choose_claim_fun, choose_claim_v4},
            {strong_conditional_put, Strong}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {default_bucket_props, [{allow_mult, Mult}, {last_write_wins, LWW}]}
          ]}]
       ).


confirm() ->
    Nodes1 = rt:build_cluster(?NUM_NODES, ?CONF(false, true, false)),

    false = test_conditional({weak, lww}, Nodes1),

    rt:clean_cluster(Nodes1),

    Nodes2 = rt:build_cluster(?NUM_NODES, ?CONF(false, true, true)),

    true = test_conditional({strong, lww}, Nodes2),

    rt:clean_cluster(Nodes2),

    Nodes3 = rt:build_cluster(?NUM_NODES, ?CONF(true, false, true)),

    true = test_conditional({strong, allow_mult}, Nodes3),

    [N3|RestNodes3] = Nodes3,
    Me = self(),

    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"AllUpNodeTest">>,
            ?TEST_LOOPS,
            false
        ),

    spawn_stop(N3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"StopNodeTest">>,
            ?TEST_LOOPS * 2,
            false
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),

    spawn_start(N3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"StartNodeTest">>,
            ?TEST_LOOPS * 2,
            false
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),

    spawn_leave(N3, RestNodes3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"LeaveNodeTest">>,
            ?TEST_LOOPS * 12,
            true
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),

    spawn_join(N3, RestNodes3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"JoinNodeTest">>,
            ?TEST_LOOPS * 10,
            true
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),

    spawn_kill(N3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"BrutalKillNodeTest">>,
            ?TEST_LOOPS * 2,
            false
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_unpingable(N3),

    spawn_start(N3, Me),
    true =
        test_conditional(
            {strong, allow_mult},
            RestNodes3,
            <<"ResstartNodeTest">>,
            ?TEST_LOOPS * 2,
            false
        ),
    receive node_change_complete -> ok end,
    rt:wait_until_pingable(N3),


    pass.


test_conditional(Type, Nodes) ->
    test_conditional(Type, Nodes, <<"ConditionBucket">>, ?TEST_LOOPS, false).

test_conditional(Type, Nodes, Bucket, Loops, ExpectTokenErrors) ->
    ClientsPerNode = 10,
    NCount = length(Nodes),

    Clients =
        lists:zip(
            lists:seq(1, ClientsPerNode * NCount),
            lists:map(
                fun(N) -> rt:pbc(N) end,
                lists:flatten(lists:duplicate(10, Nodes)))
        ),
    
    lager:info("----------------"),
    lager:info(
        "Testing with ~w condition on PUTs - parallel clients ~s",
        [Type, Bucket]
    ),
    lager:info("----------------"),

    Keys = lists:map(fun(I) -> to_key(I) end, lists:seq(1, Loops)),

    Results =
        lists:map(
            fun(K) ->
                test_concurrent_conditional_changes(
                    Bucket, K, Clients, ExpectTokenErrors
                )
            end,
            Keys
        ),

    lists:foreach(fun({_I, C}) -> riakc_pb_socket:stop(C) end, Clients),

    % print_stats(hd(Nodes)),
    
    %% Total should be n(n+1)/2
    Expected =
        ((ClientsPerNode * NCount) * (ClientsPerNode * NCount + 1)) div 2,
    lists:all(fun(R) -> R == Expected end, Results).

test_concurrent_conditional_changes(Bucket, Key, Clients, ExpectTokenErrors) ->
    [{1, C1}|_Rest] = Clients,

    ok = riakc_pb_socket:put(C1, riakc_obj:new(Bucket, Key, <<0:32/integer>>)),
    TestProcess = self(),

    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({I, C}) ->
            fun() ->
                ok =
                    try_conditional_put(
                        riakc_pb_socket, C, I, Bucket, Key, ExpectTokenErrors
                    ),
                TestProcess ! complete
            end
        end,
    lists:foreach(
        fun(ClientRef) -> spawn(SpawnUpdateFun(ClientRef)) end,
        Clients),
    
    ok = receive_complete(0, length(Clients)),
    EndTime = os:system_time(millisecond),

    {ok, FinalObj} = riakc_pb_socket:get(C1, Bucket, Key),
    <<FinalV:32/integer>> = riakc_obj:get_value(FinalObj),

    lager:info("Test took ~w ms", [EndTime - StartTime]),
    lager:info("Test had final value of ~w", [FinalV]),
    
    FinalV.


receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive complete -> receive_complete(T + 1, Target) end.

try_conditional_put(ClientMod, C, I, B, K, ExpectTokenErrors) ->
    {ok, Obj} = ClientMod:get(C, B, K),
    <<V:32/integer>> = riakc_obj:get_value(Obj),
    Obj1 = riakc_obj:update_value(Obj, <<(V + I):32/integer>>),
    PutRsp = ClientMod:put(C, Obj1, [if_not_modified]),
    case check_match_conflict(ClientMod, PutRsp, ExpectTokenErrors) of
        true ->
            try_conditional_put(ClientMod, C, I, B, K, ExpectTokenErrors);
        false ->
            ok
    end.

check_match_conflict(riakc_pb_socket, {error, <<"modified">>}, _ETE) ->
    true;
check_match_conflict(rhc, {error, {ok, "412", _Headers, _Message}}, _ETE) ->
    true;
check_match_conflict(riakc_pb_socket, {error, <<"\"Token Error\"">>}, true) ->
    timer:sleep(1000),
    true;
check_match_conflict(_, ok, _ETE) ->
    false.


to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

spawn_stop(Node, P) ->
    F =
        fun() ->
            random_sleep(), rt:stop_and_wait(Node), change_complete(P)
        end,
    spawn(F).

spawn_start(Node, P) ->
    F =
        fun() ->
            random_sleep(), rt:start_and_wait(Node), change_complete(P)
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
            lager:info("Sleeping claimant_tick before checking transfer progress"),
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
            lager:info("Sleeping claimant_tick before checking transfer progress"),
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
    lager:info("Spawned node change complete"),
    P ! node_change_complete.

random_sleep() ->
    timer:sleep(rand:uniform(?MAX_RANDOM_SLEEP)).

% print_stats(Node) ->
%     Stats =
%         erpc:call(
%             Node,
%             fun() ->
%                 lists:zip(
%                     riak_core_node_watcher:nodes(riak_kv),
%                     erpc:multicall(
%                         riak_core_node_watcher:nodes(riak_kv),
%                         fun() -> riak_kv_token_manager:stats() end
%                     )
%                 )
%             end
%         ),
%     Status =
%         erpc:call(
%             Node,
%             fun() ->
%                 lists:zip(
%                     riak_core_node_watcher:nodes(riak_kv),
%                     erpc:multicall(
%                         riak_core_node_watcher:nodes(riak_kv),
%                         fun() -> sys:get_state(riak_kv_token_manager) end
%                     )
%                 )
%             end
%         ),
%     lager:info("Token stats ~p", [Stats]),
%     lager:info("Token status ~p", [Status]).