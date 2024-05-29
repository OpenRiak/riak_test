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
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 32).
-define(NUM_NODES, 6).

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

    false = test_conditional(weak, Nodes1),

    rt:clean_cluster(Nodes1),

    Nodes2 = rt:build_cluster(?NUM_NODES, ?CONF(false, true, true)),

    true = test_conditional(strong, Nodes2),

    rt:clean_cluster(Nodes2),

    Nodes3 = rt:build_cluster(?NUM_NODES, ?CONF(true, false, true)),

    true = test_conditional(strong, Nodes3),

    pass.


test_conditional(Type, Nodes) ->
    Clients =
        lists:zip(
            lists:seq(1, 60),
            lists:map(
                fun(N) -> rt:pbc(N) end,
                lists:flatten(lists:duplicate(10, Nodes)))
        ),
    
    lager:info("----------------"),
    lager:info(
        "Testing with ~w condition on PUTs - parallel clients no failures",
        [Type]
    ),
    lager:info("----------------"),
    
    [{1, C1}|_Rest] = Clients,

    Bucket = <<"WeakConditionBucket">>,
    Key = <<"WeakConditionKey">>,

    ok = riakc_pb_socket:put(C1, riakc_obj:new(Bucket, Key, <<0:32/integer>>)),
    TestProcess = self(),

    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({I, C}) ->
            fun() ->
                ok = try_conditional_put(riakc_pb_socket, C, I, Bucket, Key),
                TestProcess ! complete
            end
        end,
    lists:foreach(
        fun(ClientRef) -> spawn(SpawnUpdateFun(ClientRef)) end,
        Clients),
    
    ok = receive_complete(0, 60),
    EndTime = os:system_time(millisecond),

    {ok, FinalObj} = riakc_pb_socket:get(C1, Bucket, Key),
    <<FinalV:32/integer>> = riakc_obj:get_value(FinalObj),

    lager:info("Weak condition test took ~w ms", [EndTime - StartTime]),
    lager:info("Weak condition final value is ~w", [FinalV]),

    lists:foreach(fun({_I, C}) -> riakc_pb_socket:stop(C) end, Clients),

    %% Total should be n(n+1)/2
    FinalV == 1830.

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
check_match_conflict(rhc, {error, {ok, "412", _Headers, _Message}}) ->
    true;
check_match_conflict(_, ok) ->
    false.