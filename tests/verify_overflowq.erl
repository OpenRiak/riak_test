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
%% @doc
%% Run erase_keys and find_tombs - with use of persistence
%% (i.e. queue may overflow)
-module(verify_overflowq).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NUM_NODES, 4).
-define(LOOP_COUNT, 50).
-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_del">>).

-define(CONFIG(RingSize, NVal, DeleteMode, QueueLimit), [
        {riak_core,
            [
             {ring_creation_size, RingSize},
             {default_bucket_props,
                 [
                     {n_val, NVal},
                     {allow_mult, true},
                     {dvv_enabled, true}
                 ]}
            ]
        },
        {riak_kv,
          [
            {anti_entropy, {off, []}},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
            {tictacaae_storeheads, true},
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600000},
            {tictacaae_exchangetick, 300000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {ttaaefs_maxresults, 128},
            {eraser_queue_limit, QueueLimit},
            {reaper_queue_limit, QueueLimit},
            {delete_mode, DeleteMode}
          ]}
        ]).

confirm() ->
    ?LOG_INFO("Test erasing and reaping of keys - overflow queue"),
    Nodes1 = rt:build_cluster(?NUM_NODES, ?CONFIG(16, 3, keep, 1000)),
    pass = test_eraseandreap(Nodes1, 1000),
    rt:clean_cluster(Nodes1),

    ?LOG_INFO("Test erasing and reaping of keys - no overflow"),
    Nodes2 = rt:build_cluster(?NUM_NODES, ?CONFIG(16, 3, keep, 100000)),
    test_eraseandreap(Nodes2, 100000).

test_eraseandreap(Nodes, MaxQueueSize) ->
    KeyCount = 50000,
    Mod = nextgenrepl_deletewithfailure,

    [Node1|_Rest] = Nodes,
    Mod:write_to_cluster(Node1, 1, KeyCount, new_obj),
    {ok, K0} = Mod:aae_fold(Node1,
                        pb,
                        {erase_keys,
                            ?TEST_BUCKET, all, all, all,
                            count}),
    ?assertMatch(KeyCount, K0),

    {ok, K1} = Mod:aae_fold(Node1,
                        pb,
                        {erase_keys,
                            ?TEST_BUCKET, all, all, all,
                            local}),
    ?assertMatch(KeyCount, K1),
    wait_for_deletes_to_queue(Nodes, MaxQueueSize),

    ?LOG_INFO(
        "Counting keys to be erased - should be 0"
    ),
    {ok, 0} = 
        Mod:wait_for_outcome
            (Mod, aae_fold,
                [Node1, pb,
                    {erase_keys,
                        ?TEST_BUCKET, all, all, all,
                        count}],
                    {ok, 0},
                    ?LOOP_COUNT),

    {ok, K2} = Mod:aae_fold(Node1,
                        pb,
                        {reap_tombs,
                            ?TEST_BUCKET, all, all, all,
                            local}),
    ?assertMatch(KeyCount, K2),
    wait_for_reaps_to_queue(Nodes, MaxQueueSize),

    ?LOG_INFO(
        "Counting tombs to be reaped - should be 0"
    ),
    0 = Mod:wait_for_outcome
            (Mod,
                length_aae_fold,
                [Node1,
                    pb,
                    {find_tombs,
                        ?TEST_BUCKET, all, all, all}],
                    0,
                    ?LOOP_COUNT),

    pass.


wait_for_reaps_to_queue([], _MaxQueueSize) ->
    ?LOG_INFO(
        "Reaps queued on all nodes - "
        "change_method of local distributed reaps");
wait_for_reaps_to_queue([N|Rest], MaxQueueSize) ->
    rt:wait_until(
        fun() ->
            {mqueue_lengths, MQLs} =
                lists:keyfind(
                    mqueue_lengths,
                    1,
                    rpc:call(N, riak_kv_reaper, reap_stats, [])),
            ?LOG_INFO("Reap queue lengths ~w on ~w", [MQLs, N]),
            QS = lists:sum(lists:map(fun({_P, L}) -> L end, MQLs)),
            ?assert(QS =< MaxQueueSize),
            QS > 0
        end
    ),
    ?LOG_INFO("Reaps queued on Node ~p", [N]),
    wait_for_reaps_to_queue(Rest, MaxQueueSize).

wait_for_deletes_to_queue([], _MaxQueueSize) ->
    ?LOG_INFO(
        "Deletes queued on all nodes - "
        "change_method of local distributed erases");
wait_for_deletes_to_queue([N|Rest], MaxQueueSize) ->
    rt:wait_until(
        fun() ->
            {mqueue_lengths, MQLs} =
                lists:keyfind(
                    mqueue_lengths,
                    1,
                    rpc:call(N, riak_kv_eraser, delete_stats, [])),
            ?LOG_INFO("Erase queue lengths ~w on ~w", [MQLs, N]),
            QS = lists:sum(lists:map(fun({_P, L}) -> L end, MQLs)),
            ?assert(QS =< MaxQueueSize),
            QS > 0
        end
    ),
    ?LOG_INFO("Deletes queued on Node ~p", [N]),
    wait_for_deletes_to_queue(Rest, MaxQueueSize).
