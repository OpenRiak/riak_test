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
-module(open_source_replication).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET2i, <<"2ibucket">>).

%%%
%%% What This Test Does
%%%

%%% This tests checks that you can successfully upgrade from two versions of
%%% the old open source riak (without replication) to the new version of
%%% riak that has the newly open sourced riak in it
%%%
%%% The transitions are:
%%% 2.0.5 -> 2.2.5
%%% 2.2.3 -> 2.2.5
%%%

%%% The test protocol described here is:
%%% * create cluster A in the old version
%%% * create cluster B in the old version
%%% * write to A
%%% * confirm that you can't read from B

%%% Upgrade Cluster A to the new version
%%% Upgrade Cluster B to the new version
%%% write to A
%%% confirm that you can't read from B

%%% enable real time replication on A
%%% enable real time replication on B
%%% Write to A
%%% confirm that you can read from B

confirm() ->

    %% set up the intial state
    %% two clusters
    %% * no replication on either of them (old open source)

    rt:set_backend(eleveldb),

    [ANodes, BNodes] = rt:build_clusters([{3, previous, []}, {3, previous, []}]),

    AllNodes = ANodes ++ BNodes,

    rt:wait_until_nodes_ready(ANodes),
    rt:wait_until_nodes_ready(BNodes),

    %% in the first test protocol both clusters are on the old version
    %% so no replication happens

    rt:log_to_nodes(AllNodes, "starting replication test"),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    rt:log_to_nodes(AllNodes, "Write data to A while both are in old state (no repl)"),
    ?LOG_INFO("Write data to A while both are in old state (no repl)"),

    ok = run_simple_write_test(AFirst, BFirst, no_repl),
    ok = run_2i_write_test(AFirst, BFirst, no_repl),

    %% in the second test protocol we upgrade the first cluster and write to it
    %% there is still no replication
    ?LOG_INFO("upgrade cluster A"),

    UpgradeNodeFn = fun(Node) ->
                            ?LOG_INFO("Upgrading ~0p", [Node]),
                            ok = rt:upgrade(Node, current),
                            ok = rt:wait_for_service(Node, riak_kv)
                    end,
    [ok = UpgradeNodeFn(X) || X <- ANodes],

    rt:log_to_nodes(AllNodes, "Write data to A after the first cluster has been updated but the second is in old state (no repl)"),
    ?LOG_INFO("Write data to A after the first cluster has been updated but the second is in old state (no repl)"),

    ok = run_simple_write_test(AFirst, BFirst, no_repl),
    ok = run_2i_write_test(AFirst, BFirst, no_repl),

    %% in the third test protocol we upgrade the second cluster and when we write to
    %% the first there is still no replication because its not enabled

    ?LOG_INFO("upgrade cluster B"),

    [ok = UpgradeNodeFn(X) || X <- BNodes],
    rt:log_to_nodes(AllNodes, "Write data to A after both clusters have been updated (no repl)"),
    ?LOG_INFO( "Write data to A after both clusters have been updated (no repl)"),

    ok = run_simple_write_test(AFirst, BFirst, no_repl),
    ok = run_2i_write_test(AFirst, BFirst, no_repl),

    %% in the fourth test protocol we enable replication on both clusters
    %% then when we write to A we can read from B

    Conf2 = [
             {riak_repl,
              [
               {fullsync_on_connect, false},
               {fullsync_interval,   disabled},
               {diff_batch_size,     10},
               {data_root,           "./data/riak_repl/"}
              ]},
             {riak_kv,
              [
               %% Specify fast building of AAE trees
               {anti_entropy,             {on, []}},
               {anti_entropy_build_limit, {100, 1000}},
               {anti_entropy_concurrency, 100}
              ]
             }
            ],
    rt:set_advanced_conf(all, Conf2),

    Len = length(ANodes ++ BNodes),
    Ports = lists:seq(10016, 10006 + 10 * Len, 10),
    %% DevPaths = rt_config:get(rtdev_path),
    %% ?LOG_INFO("DevPaths is ~0p", [DevPaths]),
    %% DevPath = [filename:join([X, "dev"]) || {current, X} <- DevPaths],
    %% ?LOG_INFO("DevPath is ~0p", [DevPath]),
    %% ?LOG_INFO("Nodes are ~0p", [ANodes ++ BNodes]),
    %% Devs = [filename:join([DevPath, H]) || [H | _Rest] <-
    %%                  [string:tokens(atom_to_list(X), "@") || X <- ANodes ++ BNodes]],
    %% ?LOG_INFO("Devs is ~0p", [Devs]),
    NodesAndPorts = lists:zip(ANodes ++ BNodes, Ports),
    ?LOG_INFO("NodesAndPorts is ~0p", [NodesAndPorts]),
    SetConfFun = fun(Node, Port) ->
                         ReplConf = [
                                     {riak_core, [{cluster_mgr,  {"127.0.0.1", Port}}]}
                                    ],
                         ?LOG_INFO("Setting ReplConf ~0p on ~0p", [ReplConf, Node]),
                         ok = rt:set_advanced_conf(Node, ReplConf)
                 end,
    [ok = SetConfFun(Node, Port) || {Node, Port} <- NodesAndPorts],
    [ok = rt:wait_until_pingable(X) || X <- ANodes ++ BNodes],

    %% test V3 replication (index by zero [sigh] bloody nerds count proper already)
    rt:log_to_nodes(AllNodes, "run replication2 tests"),
    ?LOG_INFO("About to go into replication2:replication/3"),
    fin = replication2:replication(ANodes, BNodes, false),

    %% final test set of 2i
    ?LOG_INFO("running replicated 2i tests"),
    ok = run_2i_write_test(AFirst, BFirst, repl),

    pass.

run_2i_write_test(WriteClusterNode, ReadClusterNode, no_repl) ->
    PBC1 = rt:pbc(WriteClusterNode),
    PBC2 = rt:pbc(ReadClusterNode),

    Offset = make_timestamp(),

    [put_an_object(PBC1, Offset, N) || N <- lists:seq(0, 20)],

    K = fun secondary_index_tests:int_to_key/1,

    %% check that these things exist on one cluster only

    assertExactQuery([{pb, PBC1}], [K(5 + Offset)], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC2}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC1}], [K(5 + Offset)], <<"field2_int">>, 5 + Offset),
    assertExactQuery([{pb, PBC2}], [], <<"field2_int">>, 5 + Offset),
    assertExactQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(5, 9)],
                     <<"field3_int">>, 5 + Offset),
    assertExactQuery([{pb, PBC2}], [], <<"field3_int">>, 5 + Offset),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 18)],
                     <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC2}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 19)],
                     <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC2}], [], <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 17)],
                     <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),
    assertRangeQuery([{pb, PBC2}], [], <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),

    %% delete objects test the write cluster only
    ?LOG_INFO("Delete an object, verify deletion..."),
    ToDel = [
             secondary_index_tests:int_to_key(5 + Offset),
             secondary_index_tests:int_to_key(11 + Offset)
            ],
    [?assertMatch(ok, riakc_pb_socket:delete(PBC1, ?BUCKET2i, KD)) || KD <- ToDel],
    ?LOG_INFO("Make sure the tombstone is reaped..."),
    ?assertMatch(ok, rt:wait_until(fun() ->
                                           rt:pbc_really_deleted(PBC1, ?BUCKET2i, ToDel)
                                   end)),

    assertExactQuery([{pb, PBC1}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC1}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC1}], [], <<"field2_int">>, 5 + Offset),

    %% now run some tests checking both clusters again

    assertExactQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(6, 9)],
                     <<"field3_int">>, 5 + Offset),
    assertExactQuery([{pb, PBC2}], [], <<"field3_int">>, 5 + Offset),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 18), N /= 11],
                     <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC2}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 19), N /= 11],
                     <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC2}], [], <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC1}], [K(N + Offset) || N <- lists:seq(10, 17), N /= 11],
                     <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),
    assertRangeQuery([{pb, PBC2}], [], <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),

    ok;
run_2i_write_test(WriteClusterNode, ReadClusterNode, repl) ->
    PBC1 = rt:pbc(WriteClusterNode),
    PBC2 = rt:pbc(ReadClusterNode),

    Offset = make_timestamp(),

    %% write to write cluster
    [put_an_object(PBC1, Offset, N) || N <- lists:seq(0, 20)],

    K = fun secondary_index_tests:int_to_key/1,

    %% check you can read from the read cluster

    assertExactQuery([{pb, PBC2}], [K(5 + Offset)], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC2}], [K(5 + Offset)], <<"field2_int">>, 5 + Offset),
    assertExactQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(5, 9)],
                     <<"field3_int">>, 5 + Offset),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 18)],
                     <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 19)],
                     <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 17)],
                     <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),

    ?LOG_INFO("Delete an object, verify deletion..."),
    ToDel = [
             secondary_index_tests:int_to_key(5 + Offset),
             secondary_index_tests:int_to_key(11 + Offset)
            ],
    [?assertMatch(ok, riakc_pb_socket:delete(PBC1, ?BUCKET2i, KD)) || KD <- ToDel],
    ?LOG_INFO("Make sure the tombstone is reaped..."),
    ?assertMatch(ok, rt:wait_until(fun() ->
                                           rt:pbc_really_deleted(PBC1, ?BUCKET2i, ToDel)
                                   end)),

    assertExactQuery([{pb, PBC2}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC2}], [], <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(5 + Offset)),
    assertExactQuery([{pb, PBC2}], [], <<"field2_int">>, 5 + Offset),

    assertExactQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(6, 9)],
                     <<"field3_int">>, 5 + Offset),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 18), N /= 11],
                     <<"field1_bin">>,
                     secondary_index_tests:int_to_field1_bin(10 + Offset),
                     secondary_index_tests:int_to_field1_bin(18 + Offset)),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 19), N /= 11],
                     <<"field2_int">>, 10 + Offset, 19 + Offset),
    assertRangeQuery([{pb, PBC2}], [K(N + Offset) || N <- lists:seq(10, 17), N /= 11],
                     <<"$key">>,
                     secondary_index_tests:int_to_key(10 + Offset),
                     secondary_index_tests:int_to_key(17 + Offset)),

    ok.

assertExactQuery(C, K, F, V) ->
    ?LOG_INFO("Expecting ~0p Index ~0p Value ~0p", [K, F, V]),
    secondary_index_tests:assertExactQuery(C, K, F, V, {false, false}).

assertRangeQuery(C, K, F, V1, V2) ->
    secondary_index_tests:assertRangeQuery(C, K, F, V1, V2, undefined, {false, false}).

run_simple_write_test(WriteClusterNode, ReadClusterNode, no_repl) ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-no_repl">>,
    ?LOG_INFO("Writing 100 more keys to ~0p", [WriteClusterNode]),
    ?assertEqual([], replication:do_write(WriteClusterNode, 101, 200, TestBucket, 2)),

    ?LOG_INFO("Reading 0 keys written to ~0p on ~0p becuz no replication",
               [WriteClusterNode, ReadClusterNode]),
    ?assertEqual(0, wait_for_reads(ReadClusterNode, 101, 200, TestBucket, 2)),
    ok.

wait_for_reads(Node, Start, End, Bucket, R) ->
    Reads = rt:systest_read(Node, Start, End, Bucket, R),
    DropFun = fun({_, {error, notfound}}) -> true;
                 (_)                      -> false
              end,
    SuccessfulReads = lists:dropwhile(DropFun, Reads),
    length(SuccessfulReads).

%% general 2i utility
put_an_object(Pid, Offset, N) ->
    Key = secondary_index_tests:int_to_key(N + Offset),
    Data = list_to_binary(io_lib:format("data~0p", [N])),
    BinIndex = secondary_index_tests:int_to_field1_bin(N + Offset),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N + Offset},
               % every 5 items indexed together
               {"field3_int", N - (N rem 5) +Offset}
              ],
    secondary_index_tests:put_an_object(Pid, Key, Data, Indexes).

make_timestamp() ->
    {_Mega, Sec, Milli} = os:timestamp(),
    1000 * Sec + Milli.
