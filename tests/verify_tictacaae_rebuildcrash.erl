%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Martin Sumner.
%% Copyright (c) 2023 Workday, Inc.
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
%% @doc Verification of Active Anti Entropy.
%% 
%% 
-module(verify_tictacaae_rebuildcrash).
-behavior(riak_test).

-export([confirm/0]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 32).
-define(CFG(PrimaryOnly, InitialSkip, MaxResults, KR),
    [
        {
            riak_kv,
            [
                % Speedy AAE configuration
                {anti_entropy, {off, []}},
                {tictacaae_active, active},
                {tictacaae_parallelstore, leveled_ko},
                    % if backend not leveled will use parallel key-ordered
                    % store
                {tictacaae_primaryonly, PrimaryOnly},
                {tictacaae_stepinitialtick, InitialSkip},
                {tictacaae_maxresults, MaxResults},
                {tictacaae_repairloops, 4},
                {tictacaae_enablekeyrange, KR},

                % Simplify by setting a small worker_pool
                {worker_pool_strategy, single},
                {node_worker_pool_size, 1}
            ]
        },
        {
            riak_core,
                [
                    {ring_creation_size, ?DEFAULT_RING_SIZE},
                    {handoff_concurrency,       4},
                    {forced_ownership_handoff,  8},
                    {vnode_inactivity_timeout,  4000},
                    {vnode_management_timeout,  4000}
                ]
        }
    ]
).
-define(NUM_NODES, 2).
-define(NUM_KEYS, 400000).
-define(BUCKET, <<"test_bucket">>).

confirm() ->

    ?LOG_INFO("Test with no rebuilds - and no startup skip and no key ranges"),
    Nodes =
        rt:build_cluster(
            ?NUM_NODES,
            ?CFG(true, false, 64, false)
        ),
    rt:wait_until_transfers_complete(Nodes),
    ok = load_initial_data(Nodes),
    ?LOG_INFO(
        "Prompt a rebuild - but first since startup so nothing should happen"
    ),
    ok = prompt_all_to_rebuild(hd(Nodes)),
    timer:sleep(1000),
    ?LOG_INFO(
        "Prompt a rebuild - second rebuild should prompt a rebuild"
    ),
    ok = prompt_all_to_rebuild(hd(Nodes)),

    ok = rt:wait_until(fun() -> build_status(hd(Nodes)) end, 60, 1000),

    ?LOG_INFO(
        "Prompt another rebuild - these may be terminated by killing pool"
    ),

    ok = prompt_all_to_rebuild(hd(Nodes)),
    ok = report_pool_state(hd(Nodes)),
    timer:sleep(1000),
    ok = report_pool_state(hd(Nodes)),
    ok = kill_pool_sup(hd(Nodes)),
    ?LOG_INFO("Pool killed and restarted"),

    ok = rt:wait_until(fun() -> build_status(hd(Nodes)) end, 60, 1000),

    ?LOG_INFO(
        "Prompt another rebuild all now that rebuilds were cancelled"
    ),
    ok = prompt_all_to_rebuild(hd(Nodes)),
    ok = rt:wait_until(fun() -> build_status(hd(Nodes)) end, 60, 1000),

    pass
    .

load_initial_data(Nodes) ->
    ?LOG_INFO("Loading ~w objects", [?NUM_KEYS]),
    ST = os:system_time(millisecond),
    Node = hd(Nodes),
    Slice = ?NUM_KEYS div 10,
    lists:foreach(
        fun(I) ->
            S = (I - 1) * Slice + 1,
            E = I * Slice,
            KVL = test_data(S, E),
            write_data(Node, KVL)
        end,
        lists:seq(1, 10)
    ),
    ?LOG_INFO("Load complete in ~w ms", [os:system_time(millisecond) - ST]),
    ok.

kill_pool_sup(Node) ->
    SupPid =
        erpc:call(Node, erlang, whereis, [riak_core_node_worker_pool_sup]),
    _Killed = erpc:call(Node, erlang, exit, [SupPid, kill]),
    rt:wait_until(
        fun() ->
            UpdPid =
                erpc:call(
                    Node,
                    erlang,
                    whereis,
                    [riak_core_node_worker_pool_sup]
                ),
            UpdPid =/= undefined
        end),
    rt:wait_until(
        fun() ->
            RegNames = erpc:call(Node, erlang, registered, []),
            not lists:member(node_worker_pool, RegNames)
        end
    ),
    WorkerPools = erpc:call(Node, riak_kv_app, get_worker_pools, []),
    erpc:call(Node, riak_core, register, [WorkerPools]),
    ok.

report_pool_state(Node) ->
    SupPid =
        erpc:call(Node, erlang, whereis, [riak_core_node_worker_pool_sup]),
    [Pool] = erpc:call(Node, supervisor, which_children, [SupPid]),
    PoolState = erpc:call(Node, sys, get_state, [element(2, Pool)]),
    ?LOG_INFO("PoolState of ~0p for Pool ~w", [PoolState, Pool]),
    ok.

prompt_all_to_rebuild(Node) ->
    {ok, Ring} = erpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    LocalOwners =
        lists:filter(
            fun({_VNP, Owner}) -> Owner == Node end,
            erpc:call(Node, riak_core_ring, all_owners, [Ring])
        ),
    erpc:call(
        Node,
        riak_kv_vnode,
        aae_prompt_nextrebuild,
        [LocalOwners, 5]
    ),
    timer:sleep(5000 + 1),
    erpc:call(
        Node,
        riak_kv_vnode,
        aae_rebuildpoke,
        [LocalOwners]
    ),
    ?LOG_INFO("Prompted rebuilds on Owners ~w", [LocalOwners]),
    ok.

build_status(Node) ->
    AAEStatus =
        erpc:call(Node, riak_kv_tictacaae_cli, get_aae_progress_report, []),
    BuildStatus =
        lists:map(
            fun(S) ->
                {status, BS} = lists:keyfind(status, 1, S),
                BS
            end,
            AAEStatus
        ),
    Rebuilding =
        length(lists:filter(fun(S) -> S == rebuilding end, BuildStatus)),
    Partial =
        length(lists:filter(fun(S) -> S == partial end, BuildStatus)),
    Built =
        length(lists:filter(fun(S) -> S == built end, BuildStatus)),
    ?LOG_INFO(
        "Build status for ~w vnodes - rebuilding=~w partial=~w built=~w",
        [length(AAEStatus), Rebuilding, Partial, Built]
    ),
    Rebuilding == 0.


to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    write_data(Node, KVs, Opts, ?BUCKET).

write_data(Node, KVs, Opts, Bucket) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, Bucket, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(Bucket, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.
