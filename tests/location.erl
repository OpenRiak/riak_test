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
-module(location).
-behavior(riak_test).

-export([confirm/0]).

%% Shared
-export([
    algorithm_supported/2,
    assert_no_location_violation/3,
    assert_no_ownership_change/4,
    assert_ring_satisfy_n_val/1,
    plan_and_wait/2,
    setup_location/2
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N_VAL, 3).
-define(CLAIMANT_TICK, 5000).

-define(RACK_A, "rack_a").
-define(RACK_B, "rack_b").
-define(RACK_C, "rack_c").
-define(RACK_D, "rack_d").

confirm() ->
    % Test takes a long time, so testing other ring sizes is expensive
    % The Ring size of 64 is a distinct scenario to ring size of 32 or
    % 128 (because it does not create a tail violation).

    pass = run_test(64, choose_claim_v2),
    pass = run_test(128, choose_claim_v2),
    pass = run_test(64, choose_claim_v4),
    pass = run_test(256, choose_claim_v4),
    pass = run_test(512, choose_claim_v4),
    pass.

run_test(RingSize, ClaimAlgorithm) ->
    Conf = [
        {riak_kv, [
            {anti_entropy, {off, []}}
        ]},
        {riak_core, [
            {ring_creation_size,        RingSize},
            {choose_claim_fun,          ClaimAlgorithm},
            {claimant_tick,             ?CLAIMANT_TICK},
            {default_bucket_props,      [{allow_mult, true}, {dvv_enabled, true}]},
            {handoff_concurrency,       max(8, RingSize div 16)},
            {forced_ownership_handoff,  max(8, RingSize div 16)},
            {vnode_inactivity_timeout,  4000},
            {vnode_management_timer,    2000}
        ]}
    ],

    ?LOG_INFO("*************************"),
    ?LOG_INFO("Testing with ring-size ~b", [RingSize]),
    ?LOG_INFO("Testing with claim algorithm ~w", [ClaimAlgorithm]),
    ?LOG_INFO("*************************"),

    AllNodes = rt:deploy_nodes(6, Conf),

    case algorithm_supported(ClaimAlgorithm, hd(AllNodes)) of
        true ->
            run_test(RingSize, ClaimAlgorithm, AllNodes);
        false ->
            ?LOG_INFO("*************************"),
            ?LOG_INFO("Skipping unsupported algorithm ~w", [ClaimAlgorithm]),
            ?LOG_INFO("*************************"),
            pass
    end.

run_test(RingSize, ClaimAlgorithm, AllNodes) ->
    [Node1, Node2, Node3, Node4, Node5, Node6] = AllNodes,
    Nodes = [Node1, Node2, Node3, Node4],

    rt:staged_join(Node2, Node1),
    rt:staged_join(Node3, Node1),
    rt:staged_join(Node4, Node1),

    % Set one location
    setup_location(Nodes, #{Node1 => ?RACK_A}),
    Ring1 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring1),

    % Change name of the location and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_B}),
    Ring2 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring2),
    assert_no_ownership_change(Ring1, Ring2, ClaimAlgorithm, false),

    % Two Nodes same location
    setup_location(Nodes, #{Node1 => ?RACK_B,
                            Node2 => ?RACK_B}),
    assert_ring_satisfy_n_val(rt:get_ring(Node1)),

    % Two Nodes different Locations
    setup_location(Nodes, #{Node1 => ?RACK_A,
                            Node2 => ?RACK_B}),
    Ring3 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring3),

    % Change one of node location and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_B}),
    Ring4 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring4),
    assert_no_ownership_change(Ring4, Ring3, ClaimAlgorithm, true),

    % Change both nodes locations and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_B,
                            Node2 => ?RACK_D}),
    assert_ring_satisfy_n_val(rt:get_ring(Node1)),

    % Three Nodes with different Locations
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_D,
                            Node3 => ?RACK_A}),
    Ring5 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring5),
    assert_no_location_violation(Ring5),

    % For Nodes with different Locations
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_D,
                            Node3 => ?RACK_A,
                            Node4 => ?RACK_B}),
    Ring6 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring6),
    assert_no_location_violation(Ring6),

    % Change all nodes locations and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_A,
                            Node2 => ?RACK_B,
                            Node3 => ?RACK_C,
                            Node4 => ?RACK_D}),

    Ring7 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring7),
    assert_no_location_violation(Ring7),
    assert_no_ownership_change(Ring6, Ring7, ClaimAlgorithm, false),

    rt:staged_join(Node5, Node1),

    setup_location(AllNodes, #{Node1 => ?RACK_A,
                               Node2 => ?RACK_B,
                               Node3 => ?RACK_C,
                               Node4 => ?RACK_B,
                               Node5 => ?RACK_A
    }),
    Ring8 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring8),

    case RingSize of
      N when N == 64; N == 256 ->

        rt:staged_join(Node6, Node1),

        setup_location(AllNodes, #{Node1 => ?RACK_A,
                                  Node2 => ?RACK_A,
                                  Node3 => ?RACK_B,
                                  Node4 => ?RACK_B,
                                  Node5 => ?RACK_C,
                                  Node6 => ?RACK_C
                                  }),
        Ring9 = rt:get_ring(Node1),

        assert_ring_satisfy_n_val(Ring9),
        % Because of tail violations need to increase n_val to satisfy diversity of locations
        assert_no_location_violation(Ring9, 4, 3),

        setup_location(AllNodes, #{Node1 => ?RACK_A,
                                  Node2 => ?RACK_B,
                                  Node3 => ?RACK_C,
                                  Node4 => ?RACK_C,
                                  Node5 => ?RACK_B,
                                  Node6 => ?RACK_A
        }),
        Ring10 = rt:get_ring(Node1),
        assert_ring_satisfy_n_val(Ring10),
        % Because of tail violations need to increase n_val to satisfy diversity of locations
        assert_no_location_violation(Ring10, 4, 3);

      N ->
        ?LOG_INFO(
          "Test skipped for ring size ~b =/= 64 - as will fail "
          "for unsolveable tail violations", [N])
    end,

    ?LOG_INFO("Test verify location settings with ring size ~b: Passed",
                [RingSize]),

    rt:clean_cluster(AllNodes),

    ?LOG_INFO("Cluster cleaned"),

    pass.

-spec set_location(node(), string()) -> ok | {fail, term()}.
set_location(Node, Location) ->
    ?LOG_INFO("Set ~0p node location to ~0p", [Node, Location]),
    JoinFun = fun() ->
        {ok, Result} = rt:admin(Node, ["cluster", "location", Location]),
        lists:prefix("Success:", Result)
    end,
    ok = rt:wait_until(JoinFun, 5, 1000).

-spec setup_location([node()], #{node() := string()}) -> ok.
setup_location([OnNode | _] = Nodes, NodeMap) ->
    maps:map(fun set_location/2, NodeMap),
    plan_and_wait(OnNode, Nodes).

-spec plan_and_wait(node(), [node()]) -> ok.
plan_and_wait(Claimant, Nodes) ->
    rt:wait_until_ring_converged(Nodes),
    rt:plan_and_commit(Claimant),
    rt:wait_until_ring_converged(Nodes),
    lists:foreach(fun rt:wait_until_ready/1, Nodes),
    ?LOG_INFO("Sleeping claimant_tick before checking transfer progress"),
    timer:sleep(?CLAIMANT_TICK),
    ok = rt:wait_until_transfers_complete(Nodes),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Nodes),
    ?LOG_INFO(
      "Sleeping claimant_tick  + 1s before confirming transfers complete"),
    timer:sleep(?CLAIMANT_TICK + 1000),
    ok = rt:wait_until_transfers_complete(Nodes).

assert_ring_satisfy_n_val(Ring) ->
  ?LOG_INFO("Ensure that every preflists satisfy n_val"),
  ?assertEqual([], riak_core_ring_util:check_ring(Ring, ?N_VAL)).

assert_no_ownership_change(RingA, RingB, choose_claim_v2, _TolerateChange) ->
  ?LOG_INFO("Ensure no ownership changed"),
  ?assertEqual(
    riak_core_ring:all_owners(RingA), riak_core_ring:all_owners(RingB));
assert_no_ownership_change(RingA, RingB, _, false) ->
  ?LOG_INFO("Ensure no ownership changed"),
  ?assertEqual(
    riak_core_ring:all_owners(RingA), riak_core_ring:all_owners(RingB));
assert_no_ownership_change(RingA, RingB, choose_claim_v4, true) ->
  OwnersA = riak_core_ring:all_owners(RingA),
  OwnersB = riak_core_ring:all_owners(RingB),
  DiffOwners = lists:subtract(OwnersA, OwnersB),
  ?LOG_INFO(
    "choose_claim_v4 does not guarrantee no ownership change on "
    "change of location name - ~b changes out of ~b",
    [length(DiffOwners), length(OwnersA)]),
  ok.

assert_no_location_violation(Ring) ->
  assert_no_location_violation(Ring, ?N_VAL, ?N_VAL).

assert_no_location_violation(Ring, NVal, MinNumberOfDistinctLocation) ->
  ?assertEqual(true, riak_core_location:has_location_set_in_cluster(riak_core_ring:get_nodes_locations(Ring))),
  log_assert_no_location_violation(NVal, MinNumberOfDistinctLocation),
  ?assertEqual([], riak_core_location:check_ring(Ring, NVal, MinNumberOfDistinctLocation)).

log_assert_no_location_violation(Nval, Nval) ->
  ?LOG_INFO("Ensure that every preflists have uniq locations");
log_assert_no_location_violation(NVal, MinNumberOfDistinctLocation) ->
  ?LOG_INFO("Ensure that every preflists (n_val: ~b) have at leaset ~b distinct locations",
             [NVal, MinNumberOfDistinctLocation]).

-spec algorithm_supported(ClaimAlgorithm :: atom(), node()) -> boolean().
algorithm_supported(ClaimAlgorithm, Node) ->
    Swapping = [riak_core_claim_swapping, ClaimAlgorithm, 3],
    Membership = [riak_core_membership_claim, ClaimAlgorithm, 3],
    Original = [riak_core_claim, ClaimAlgorithm, 3],
    CheckFun =
      fun(MFA) ->
        rpc:call(Node, code, ensure_loaded, [hd(MFA)]),
        rpc:call(Node, erlang, function_exported, MFA)
      end,
    CheckFun(Swapping) orelse CheckFun(Membership) orelse CheckFun(Original).