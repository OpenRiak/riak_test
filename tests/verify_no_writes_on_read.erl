%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_no_writes_on_read).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NUM_NODES, 3).
-define(BUCKET, <<"bucket">>).

confirm() ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    ?LOG_INFO("Running with backend ~0p", [Backend]),
    ?assertEqual(bitcask, Backend),
    [Node1 | _Rest] = _Nodes = rt:build_cluster(?NUM_NODES),
    PBC = rt:pbc(Node1),
    ?LOG_INFO("Setting last write wins on bucket"),
    B = ?BUCKET,
    ?assertMatch(ok, rpc:call(Node1, riak_core_bucket, set_bucket, [B, [{last_write_wins, true}]])),
    BProps = rpc:call(Node1, riak_core_bucket, get_bucket, [B]),
    ?LOG_INFO("Bucket properties ~0p", [BProps]),
    K = <<"Key">>,
    V = <<"Value">>,
    Obj = riakc_obj:new(B, K, V),
    ?LOG_INFO("Writing a simple object"),
    riakc_pb_socket:put(PBC,Obj),
    ?LOG_INFO("Waiting some time to let the stats update"),
    timer:sleep(10000),
    OrigStats = get_write_stats(Node1),
    ?LOG_INFO("Stats are now ~0p", [OrigStats]),
    Read1 = fun(_N) ->
                    ?assertMatch({ok,_O}, riakc_pb_socket:get(PBC, B, K))
            end,
    ?LOG_INFO("Repeatedly read that object. There should be no writes"),
    lists:foreach(Read1, lists:seq(1,100)),
    ?LOG_INFO("Waiting some time to let the stats update"),
    timer:sleep(10000),
    Stats = get_write_stats(Node1),
    ?LOG_INFO("Stats are now ~0p", [Stats]),
    ?assertEqual(OrigStats, Stats),
    riakc_pb_socket:stop(PBC),
    pass.


get_write_stats(Node) ->
    Stats = rpc:call(Node, riak_kv_stat, get_stats, []),
    Puts = proplists:get_value(vnode_puts, Stats),
    ReadRepairs = proplists:get_value(read_repairs, Stats),
    [{puts, Puts}, {read_repairs, ReadRepairs}].

