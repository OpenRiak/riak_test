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
-module(location_leave).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").

-import(location, [
    algorithm_supported/2,
    assert_no_location_violation/3,
    assert_no_ownership_change/4,
    assert_ring_satisfy_n_val/1,
    plan_and_wait/2,
    setup_location/2
]).

-define(CLAIMANT_TICK, 5000).

-define(RACK_A, "rack_a").
-define(RACK_B, "rack_b").
-define(RACK_C, "rack_c").
-define(RACK_D, "rack_d").

confirm() ->

    % Test takes a long time, so testing other ring sizes is expensive
    Algo4 = choose_claim_v4,
    pass = run_test(64, Algo4, 3, 3),
    pass = run_test(512, Algo4, 3, 3),
    pass.

run_test(RingSize, ClaimAlgorithm, LNV, ActualL) ->
    Conf =
        [
        {riak_kv, [{anti_entropy, {off, []}}]},
        {riak_core,
            [
              {ring_creation_size, RingSize},
              {claimant_tick, ?CLAIMANT_TICK},
              {handoff_concurrency,       max(8, RingSize div 16)},
              {forced_ownership_handoff,  max(8, RingSize div 16)},
              {vnode_inactivity_timeout,  4000},
              {vnode_management_timer,    2000},
              {choose_claim_fun, ClaimAlgorithm},
              {target_location_n_val, 3},
              {full_rebalance_onleave, true},
              {default_bucket_props,
                [{allow_mult, true}, {dvv_enabled, true}]}
              ]}
            ],

    ?LOG_INFO("*************************"),
    ?LOG_INFO("Testing with ring-size ~b", [RingSize]),
    ?LOG_INFO("Testing with claim algorithm ~w", [ClaimAlgorithm]),
    ?LOG_INFO("*************************"),

    AllNodes = rt:deploy_nodes(8, Conf),

    case algorithm_supported(ClaimAlgorithm, hd(AllNodes)) of
        true ->
            really_run_test(ClaimAlgorithm, LNV, ActualL, AllNodes);
        false ->
            ?LOG_INFO("*************************"),
            ?LOG_INFO("Skipping unsupported algorithm ~w", [ClaimAlgorithm]),
            ?LOG_INFO("*************************"),
            pass
    end.
    
really_run_test(ClaimAlgorithm, LNV, ActualL, AllNodes) ->
    [Node1, Node2, Node3, Node4, Node5, Node6, Node7, Node8] = AllNodes,

    rt:staged_join(Node2, Node1),
    rt:staged_join(Node3, Node1),
    rt:staged_join(Node4, Node1),
    rt:staged_join(Node5, Node1),
    rt:staged_join(Node6, Node1),
    rt:staged_join(Node7, Node1),

    setup_location(
        AllNodes -- [Node8],
            #{Node1 => ?RACK_A,
                Node2 => ?RACK_A,
                Node3 => ?RACK_B,
                Node4 => ?RACK_B,
                Node5 => ?RACK_C,
                Node6 => ?RACK_C,
                Node7 => ?RACK_D
                }),
    Ring1 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring1),
    assert_no_location_violation(Ring1, LNV, ActualL),

    ?LOG_INFO("Transferring Node7 to Node8 - no location set"),
    ?LOG_INFO("Cannot set location on replacement node before replacement"),
    commit_transfer(AllNodes -- [Node7], Node7,  Node8),
    Ring2 = rt:get_ring(Node1),
    assert_no_location_violation(Ring2, LNV, ActualL),

    ?LOG_INFO("Give Node 8 same location as Node 7"),
    ?LOG_INFO("Should not prompt changes - Node 8 is in same location"),
    setup_location(AllNodes -- [Node7], #{Node8 => ?RACK_D}),

    Ring3 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring3),
    assert_no_location_violation(Ring3, LNV, ActualL),
    assert_no_ownership_change(Ring3, Ring2, ClaimAlgorithm, false),

    ?LOG_INFO("Rejoin Node 7"),
    rt:start(Node7),
    rt:wait_until_ready(Node7),
    rt:wait_until_pingable(Node7),
    rt:staged_join(Node7, Node1),
    setup_location(AllNodes, #{Node7 => ?RACK_D}),

    Ring4 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring4),
    assert_no_location_violation(Ring4, LNV, ActualL),

    ?LOG_INFO("Leave node 8"),

    ok = rt:staged_leave(Node8),
    rt:wait_until_ring_converged(AllNodes),
    ok = plan_and_wait(Node1, AllNodes -- [Node8]),
    rt:wait_until_unpingable(Node8),
    Ring5 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring5),
    assert_no_location_violation(Ring5, LNV, ActualL),

    ?LOG_INFO("Rejoin Node 8"),
    rt:start(Node8),
    rt:wait_until_ready(Node8),
    rt:wait_until_pingable(Node8),
    rt:staged_join(Node8, Node1),
    setup_location(AllNodes, #{Node8 => ?RACK_D}),

    Ring6 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring6),
    assert_no_location_violation(Ring6, LNV, ActualL),

    ?LOG_INFO("Leave nodes 4 and 6 - 6 node cluster"),

    ok = rt:staged_leave(Node4),
    ok = rt:staged_leave(Node6),
    rt:wait_until_ring_converged(AllNodes),
    ok = plan_and_wait(Node1, AllNodes -- [Node4, Node6]),
    rt:wait_until_unpingable(Node4),
    rt:wait_until_unpingable(Node6),

    Ring7 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring7),
    assert_no_location_violation(Ring7, LNV, ActualL),

    ?LOG_INFO("Rejoin Nodes 4, 6"),
    rt:start(Node4),
    rt:start(Node6),
    rt:wait_until_ready(Node4),
    rt:wait_until_ready(Node6),
    rt:staged_join(Node4, Node1),
    rt:staged_join(Node6, Node1),

    setup_location(
        AllNodes, #{Node4 => ?RACK_B, Node6 => ?RACK_C}),

    Ring8 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring8),
    assert_no_location_violation(Ring8, LNV, ActualL),


    rt:clean_cluster(AllNodes),

    ?LOG_INFO("Cluster cleaned"),


    pass.

-spec commit_transfer([node()], node(), node()) -> ok.
commit_transfer(Nodes, ExitingNode, JoiningNode) ->
    [Claimant|_] = Nodes,
    rt:staged_join(JoiningNode, Claimant),
    rt:wait_until(
        fun() ->
            ok ==
                rpc:call(
                    Claimant,
                    riak_core_claimant, replace, [ExitingNode, JoiningNode])
        end),
    plan_and_wait(Claimant, Nodes),
    rt:wait_until_unpingable(ExitingNode).
