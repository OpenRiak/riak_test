%% -------------------------------------------------------------------
%%
%% Copyright (c) 2025 TI Tokyo.
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
-module(riak_admin_tictacaae_console_tests).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REBUILD_WAIT, 4).
-define(REBUILD_DELAY, 3600).
-define(REBUILD_TICK, 3600000).  %% don't tick for an hour!
-define(EXCHANGE_TICK, 3600 * 1000).   %% don't exchange during test
-define(CONFIG, [
        {riak_core,
            [
             {default_bucket_props,
                 [
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
            {tictacaae_storeheads, true},
            {tictacaae_rebuildwait, ?REBUILD_WAIT},
            {tictacaae_rebuilddelay, ?REBUILD_DELAY},
            {tictacaae_exchangetick, ?EXCHANGE_TICK},
            {tictacaae_rebuildtick, ?REBUILD_TICK},
            {ttaaefs_maxresults, 128},
            {tombstone_pause, 2},
            {delete_mode, keep}
          ]}
        ]).

confirm() ->
    Nodes = [Node1, Node2] = rt:build_cluster(2, ?CONFIG),
    rt:wait_until_nodes_ready(Nodes),

    ch1a_tests(Node1, Node2),
    ch1c_tests(Node1, Node2),
    ch1d_tests(Node1, Node2),

    ch2_tests(Node1, Node2),

    pass.


-define(TTAAE_ENVVAR_SPECS,
        [{"rebuildtick", integer_to_list(?REBUILD_TICK), integer_to_list(?REBUILD_TICK + 1)},
         {"exchangetick", integer_to_list(?EXCHANGE_TICK), integer_to_list(?EXCHANGE_TICK + 1)},
         {"maxresults", "64", "65"}
        ]).

ch1a_tests(Node1, Node2) ->
    CheckEnvVarF =
        fun(Var, Val) ->
                Cmd = ff("tictacaae ~s -n ~s", [Var, Node1]),
                Expect = ff("|~s| *~s *|", [Node1, Val]),
                ok = check_admin_cmd(Node1, Cmd, Expect),
                ok = check_admin_cmd(Node2, Cmd, Expect)
        end,
    SetEnvVarF =
        fun(Var, Val) ->
                Cmd = ff("tictacaae ~s ~s -n ~s", [Var, Val, Node1]),
                ok = check_admin_cmd(Node1, Cmd, any)
        end,
    [begin
         CheckEnvVarF(Var, OrigVal),
         SetEnvVarF(Var, NewVal),
         CheckEnvVarF(Var, NewVal)
     end || {Var, OrigVal, NewVal} <- ?TTAAE_ENVVAR_SPECS],
    ok.

node_partitions(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    [P || {P, Owner} <- rpc:call(Node, riak_core_ring, all_owners, [Ring]), Owner =:= Node].


-define(TTAAE_VNODE_SPECS,
        [{"tokenbucket", "true", "false"},
         {"storeheads", "true", "false"}
        ]).

ch1c_tests(Node1, Node2) ->
    PP1 = node_partitions(Node1),
    Px = lists:nth(rand:uniform(length(PP1)), PP1),
    CheckEnvVarF =
        fun(Var, Val) ->
                Cmd = ff("tictacaae ~s -n ~s -p ~b", [Var, Node1, Px]),
                Expect = ff("| *~s *| *~b *| *~s *|", [Node1, Px, Val]),
                ok = check_admin_cmd(Node1, Cmd, Expect),
                ok = check_admin_cmd(Node2, Cmd, Expect)
        end,
    SetEnvVarF =
        fun(Var, Val) ->
                Cmd = ff("tictacaae ~s ~s -n ~s -p ~b", [Var, Val, Node1, Px]),
                Expect = ff("Set ~s to ~s on partition ~b on ~s\n", [Var, Val, Px, Node1]),
                ok = check_admin_cmd(Node1, Cmd, Expect)
        end,
    [begin
         CheckEnvVarF(Var, OrigVal),
         SetEnvVarF(Var, NewVal),
         CheckEnvVarF(Var, NewVal)
     end || {Var, OrigVal, NewVal} <- ?TTAAE_VNODE_SPECS],
    ok.


-define(TTAAE_VNODE_SPECS_2,
        [{{?REBUILD_WAIT, ?REBUILD_DELAY}, {?REBUILD_WAIT, ?REBUILD_DELAY + 1}},
         {{?REBUILD_WAIT, ?REBUILD_DELAY + 1}, {?REBUILD_WAIT + 1, ?REBUILD_DELAY + 1}},
         {{?REBUILD_WAIT + 1, ?REBUILD_DELAY + 1}, {?REBUILD_WAIT + 1, ?REBUILD_DELAY}}
        ]).

ch1d_tests(Node1, Node2) ->
    PP1 = node_partitions(Node1),
    Px = lists:nth(rand:uniform(length(PP1)), PP1),
    CheckEnvVarF =
        fun({RW, RD}) ->
                Cmd = ff("tictacaae rebuild-schedule -n ~s -p ~b", [Node1, Px]),
                Expect = ff("| *~s *| *~b *| *RW: ~b, RD: ~b *|", [Node1, Px, RW, RD]),
                ok = check_admin_cmd(Node1, Cmd, Expect),
                ok = check_admin_cmd(Node2, Cmd, Expect)
        end,
    SetEnvVarF =
        fun({RW, RD}) ->
                Cmd = ff("tictacaae rebuild-schedule ~b ~b -n ~s -p ~b", [RW, RD, Node1, Px]),
                Expect = ff("Set rebuild.schedule to RW: ~b, RD: ~b on partition ~b on ~s\n", [RW, RD, Px, Node1]),
                ok = check_admin_cmd(Node1, Cmd, Expect)
        end,
    [begin
         CheckEnvVarF(OrigVal),
         SetEnvVarF(NewVal),
         CheckEnvVarF(NewVal)
     end || {OrigVal, NewVal} <- ?TTAAE_VNODE_SPECS_2],
    ok.


-define(NON_ASCII_BUCKET_NAME, <<3, 4, 5>>).
-define(ASCII_BUCKET_NAME, <<"regular_bucket">>).
-define(TMP_FILE, "/tmp/riak-test-aaefold-results").
-define(SAMPLE1, [{<<"k1">>, <<"v">>},
                  {<<"k2">>, <<"v">>},
                  {<<"k3">>, <<"v">>},
                  {<<"k4">>, <<"v">>},
                  {<<"k5">>, <<"v">>}
                 ]).
-define(DELETED_KEYS, [<<"k1">>, <<"k3">>]).

ch2_tests(Node1, _Node2) ->
    PB = rt:pbc(Node1),

    ok = write_some_data(PB),
    list_buckets_test(Node1),
    find_keys_test(Node1),
    count_keys_test(Node1),

    ok = delete_some_data(PB),
    find_tombstones_test(Node1),
    count_tombstones_test(Node1),

    ok = write_some_data(PB),
    ok = delete_some_data(PB),
    reap_tombstones_test(Node1),

    object_stats_test(Node1),

    erase_keys_test(Node1),
    repair_keys_test(Node1),

    ok.

write_some_data(PB) ->
    io:format("Writing some 5 keys\n"),
    [ok = riakc_pb_socket:put(
            PB, riakc_obj:new(?ASCII_BUCKET_NAME, K, V)) || {K, V} <- ?SAMPLE1],
    %% also test hex representation of non-printable bucket names
    ok = riakc_pb_socket:put(
           PB, riakc_obj:new(?NON_ASCII_BUCKET_NAME,
                             <<"key">>, <<"value">>)),
    ok.

list_buckets_test(Node) ->
    NVal = 3,
    Cmd = ff("tictacaae fold list-buckets nval=~b -o ~s", [NVal, ?TMP_FILE]),
    NonAsciiNameHexEncoded = iolist_to_binary(["0x", mochihex:to_hex(?NON_ASCII_BUCKET_NAME)]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                BB = mochijson2:decode(Doc),
                true = lists:member(?ASCII_BUCKET_NAME, BB),
                true = lists:member(NonAsciiNameHexEncoded, BB),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

find_keys_test(Node) ->
    Cmd1 = ff("tictacaae fold find-keys bucket=~s key_range=all modified_range=all sibling_count=0 -o ~s",
              [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    %% also, test parsing of date strings
    TS1 = calendar:system_time_to_rfc3339(os:system_time(second) - 3600),
    TS2 = calendar:system_time_to_rfc3339(os:system_time(second) + 3600),
    Cmd2 = ff("tictacaae fold find-keys bucket=~s key_range=all modified_range=~s,~s sibling_count=0 -o ~s",
              [?ASCII_BUCKET_NAME, TS1, TS2, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                KK = [K || #{<<"key">> := K} <- mochijson2:decode(Doc, [{format, map}])],
                ?assertEqual(KK, [K || {K, _} <- ?SAMPLE1]),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd1, AssertFun),
    ok = check_admin_cmd(Node, Cmd2, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

count_keys_test(Node) ->
    Cmd = ff("tictacaae fold count-keys bucket=~s key_range=all modified_range=all sibling_count=0 -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                Counted = mochijson2:decode(Doc),
                ?assertEqual(Counted, length(?SAMPLE1)),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

delete_some_data(PB) ->
    io:format("Deleting some 2 keys\n"),
    [begin
         riakc_pb_socket:delete(PB, ?ASCII_BUCKET_NAME, Key)
     end || Key <- ?DELETED_KEYS],
    ok.

find_tombstones_test(Node) ->
    Cmd = ff("tictacaae fold find-tombstones bucket=~s key_range=all modified_range=all segments=all -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                KK = [K || #{<<"key">> := K} <- mochijson2:decode(Doc, [{format, map}])],
                ?assertEqual(KK, ?DELETED_KEYS),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

count_tombstones_test(Node) ->
    Cmd = ff("tictacaae fold count-tombstones bucket=~s key_range=all modified_range=all segments=all -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                Counted = mochijson2:decode(Doc),
                ?assertEqual(length(?DELETED_KEYS), Counted),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

reap_tombstones_test(Node) ->
    Cmd = ff("tictacaae fold reap-tombstones bucket=~s key_range=all modified_range=all segments=all change_method=local -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                Counted = mochijson2:decode(Doc),
                ?assertEqual(2, Counted),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

object_stats_test(Node) ->
    Cmd = ff("tictacaae fold object-stats bucket=~s key_range=all modified_range=all -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                #{<<"total_count">> := TC,
                  <<"total_size">> := TS,
                  <<"sizes">> := [#{<<"min">> := Size1Min, <<"max">> := Size1Max}],
                  <<"siblings">> := [#{<<"min">> := Sibl1Min, <<"max">> := Sibl1Max}]} =
                    mochijson2:decode(Doc, [{format, map}]),
                io:format("TotalSize of some keys is ~b\n", [TS]),
                ?assertEqual(TC, 3),
                ?assertEqual(Size1Min, 2),
                ?assertEqual(Size1Max, 3),
                ?assertEqual(Sibl1Min, 2),
                ?assertEqual(Sibl1Max, 3),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

erase_keys_test(Node) ->
    Cmd = ff("tictacaae fold erase-keys bucket=~s key_range=all modified_range=all segments=all change_method=local -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                Res = mochijson2:decode(Doc),
                ?assertEqual(3, Res),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

repair_keys_test(Node) ->
    Cmd = ff("tictacaae fold repair-keys bucket=~s key_range=all modified_range=all -o ~s",
             [?ASCII_BUCKET_NAME, ?TMP_FILE]),
    AssertFun =
        fun(Out) ->
                assert_cmd_output(Out),
                Doc = wait_until_file_appears(?TMP_FILE),
                Res = mochijson2:decode(Doc),
                ?assertEqual(3, Res),
                ok
        end,
    ok = check_admin_cmd(Node, Cmd, AssertFun),
    ok = file:delete(?TMP_FILE),
    ok.

assert_cmd_output(A) ->
    ?assert(nomatch /= string:find(A, "Results will be written to " ++ ?TMP_FILE)).

wait_until_file_appears(File) ->
    wait_until_file_appears(File, 10000).
wait_until_file_appears(_, 0) ->
    throw(aae_fold_results_file_not_written);
wait_until_file_appears(File, W) ->
    case file:read_file(?TMP_FILE) of
        {ok, <<>>} ->
            timer:sleep(100),
            wait_until_file_appears(File, W - 100);
        {ok, Doc} ->
            Doc;
        _ ->
            timer:sleep(100),
            wait_until_file_appears(File, W - 100)
    end.

check_admin_cmd(Node, Cmd, any) ->
    S = string:tokens(Cmd, " "),
    {ok, _} = rt:admin(Node, S),
    ok;
check_admin_cmd(Node, Cmd, AssertFun) when is_function(AssertFun) ->
    S = string:tokens(Cmd, " "),
    {ok, Out} = rt:admin(Node, S),
    AssertFun(Out);
check_admin_cmd(Node, Cmd, Expect) ->
    S = string:tokens(Cmd, " "),
    {ok, Out} = rt:admin(Node, S),
    case re:run(Out, Expect) of
        nomatch ->
            fail;
        _ ->
            ok
    end.

ff(F, A) ->
    lists:flatten(io_lib:format(F, A)).
