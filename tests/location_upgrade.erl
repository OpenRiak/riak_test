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
-module(location_upgrade).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(location, [setup_location/2]).

-define(N_VAL, 3).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),
    %% Bump up handoff concurrency
    Config = {OldVsn, [{riak_core, [{handoff_concurrency, 30}]}]},
    Nodes = [Node1, Node2] = rt:build_cluster([Config, Config]),

    Claimant = rt:claimant_according_to(Node1),

    ?LOG_INFO("Upgrading claimant node"),
    upgrade(Claimant, current),

    setup_location(Nodes, #{Claimant => "test_location"}),

    ?LOG_INFO("Upgrading all nodes"),
    [rt:upgrade(Node, current) || Node <- Nodes, Node =/= Claimant],

    ?assertEqual([], riak_core_location:check_ring(rt:get_ring(Node1), ?N_VAL, 2)),

    setup_location(Nodes, #{Node1 => "node1_location",
                            Node2 => "This_is_the_node2's_location"}),

    ?LOG_INFO("Downgrading all nodes"),
    [rt:upgrade(Node, previous) || Node <- Nodes, Node =/= Claimant],

    pass.


upgrade(Node, NewVsn) ->
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, [riak_kv]),
    ok.
