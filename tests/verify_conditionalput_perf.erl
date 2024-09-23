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
-module(verify_conditionalput_perf).
-export([confirm/0, spawn_profile_fun/1]).

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(TEST_LOOPS, 32).
-define(NUM_NODES, 6).
-define(CLAIMANT_TICK, 5000).

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
            {claimant_tick, ?CLAIMANT_TICK},
            {vnode_management_timer, 2000},
            {vnode_inactivity_timeout, 4000},
            {forced_ownership_handoff, 16},
            {handoff_concurrency, 16},
            {choose_claim_fun, choose_claim_v4},
            {default_bucket_props, [{allow_mult, Mult}, {last_write_wins, LWW}]}
          ]}]
       ).

confirm() ->
    

    Nodes2 =
        rt:build_cluster(
            ?NUM_NODES, ?CONF(false, true, prefer_token, basic_consensus)
        ),

    spawn_profile_fun(hd(Nodes2)),
    true =
        test_conditional(
            {strong, lww}, Nodes2, <<"pbcStrong">>, ?TEST_LOOPS * 2, riakc_pb_socket
        ),
    
    spawn_profile_fun(hd(Nodes2)),
    true =
        test_conditional(
            {strong, lww}, Nodes2, <<"httpStrong">>, ?TEST_LOOPS * 2, rhc
        ),

    pass.


test_conditional(Type, Nodes, Bucket, Loops, ClientMod) ->
    ClientsPerNode = 5,

    Clients = get_clients(ClientsPerNode, Nodes, ClientMod),
    
    ?LOG_INFO("----------------"),
    ?LOG_INFO(
        "Testing with ~w condition on PUTs - parallel clients ~s client ~w",
        [Type, Bucket, ClientMod]
    ),
    ?LOG_INFO("----------------"),

    Keys = lists:map(fun(I) -> to_key(I) end, lists:seq(1, Loops)),

    Results =
        lists:map(
            fun(K) ->
                test_concurrent_conditional_changes(
                    Bucket, K, Clients, ClientMod
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
    ?LOG_INFO(
        "Average time per result ~w ms",
        [lists:sum(Timings) div length(Timings)]
    ),
    ?LOG_INFO(
        "Maximum time per result ~w ms",
        [lists:max(Timings)]
    ),
    lists:all(fun(R) -> R == Expected end, FinalValues).

test_concurrent_conditional_changes(Bucket, Key, Clients, ClientMod) ->
    [{1, C1}|_Rest] = Clients,

    ok = ClientMod:put(C1, riakc_obj:new(Bucket, Key, <<0:32/integer>>)),
    TestProcess = self(),

    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({I, C}) ->
            fun() ->
                ok =
                    try_conditional_put(
                        ClientMod, C, I, Bucket, Key
                    ),
                TestProcess ! complete
            end
        end,
    lists:foreach(
        fun(ClientRef) -> spawn(SpawnUpdateFun(ClientRef)) end,
        Clients),
    
    ok = receive_complete(0, length(Clients)),
    EndTime = os:system_time(millisecond),

    {ok, FinalObj} = ClientMod:get(C1, Bucket, Key, [{r, 3}, {pr, 2}]),
    <<FinalV:32/integer>> = riakc_obj:get_value(FinalObj),

    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),
    ?LOG_INFO("Test had final value of ~w", [FinalV]),
    
    {FinalV, EndTime - StartTime}.


receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive complete -> receive_complete(T + 1, Target) end.

try_conditional_put(ClientMod, C, I, B, K) ->
    {ok, Obj} = ClientMod:get(C, B, K),
    <<V:32/integer>> = riakc_obj:get_value(Obj),
    Obj1 = riakc_obj:update_value(Obj, <<(V + I):32/integer>>),
    PutRsp = ClientMod:put(C, Obj1, [if_not_modified]),
    case check_match_conflict(ClientMod, PutRsp) of
        true ->
            try_conditional_put(ClientMod, C, I, B, K);
        false ->
            ok
    end.

check_match_conflict(riakc_pb_socket, {error, <<"modified">>}) ->
    true;
check_match_conflict(rhc, {error, {ok, "409", _Headers, _Message}}) ->
    true;
check_match_conflict(_, ok) ->
    false.

to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).


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


spawn_profile_fun(Node) ->
    spawn(
        fun() ->
            timer:sleep(1000),
            erpc:call(Node, riak_kv_util, profile_riak, [1000]),
            timer:sleep(1000),
            erpc:call(Node, riak_kv_util, profile_riak, [1000]),
            timer:sleep(1000),
            erpc:call(Node, riak_kv_util, profile_riak, [1000])
        end
    ).