%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
%% Automated test for issue riak_core#154
%% Hinted handoff does not occur after a node has been restarted in Riak 1.1
-module(gh_riak_core_154).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    %% Increase handoff concurrency on nodes
    NewConfig = [{riak_core, [{handoff_concurrency, 1024}]}],
    Nodes = rt:build_cluster(2, NewConfig),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    [Node1, Node2] = Nodes,

    ?LOG_INFO("Write data while ~0p is offline", [Node2]),
    rt:stop(Node2),
    rt:wait_until_unpingable(Node2),
    ?assertEqual([], rt:systest_write(Node1, 1000, 3)),

    ?LOG_INFO("Verify that ~0p is missing data", [Node2]),
    rt:start(Node2),
    rt:stop(Node1),
    rt:wait_until_unpingable(Node1),
    ?assertMatch([{_,{error,notfound}}|_],
                 rt:systest_read(Node2, 1000, 3)),

    ?LOG_INFO("Restart ~0p and wait for handoff to occur", [Node1]),
    rt:start(Node1),
    rt:wait_for_service(Node1, riak_kv),
    rt:wait_until_transfers_complete([Node1]),

    ?LOG_INFO("Verify that ~0p has all data", [Node2]),
    rt:stop(Node1),
    ?assertEqual([], rt:systest_read(Node2, 1000, 3)),

    ?LOG_INFO("gh_riak_core_154: passed"),
    pass.
