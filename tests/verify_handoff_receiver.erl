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
-module(verify_handoff_receiver).
-behavior(riak_test).

-include_lib("kernel/include/logger.hrl").

-export([confirm/0]).

-define(N_VAL, 1).
-define(CLAIMANT_TICK, 5000).
-define(VNODE_INACTIVITY_TIMEOUT, 4000).
-define(VNODE_MANAGEMENT_TIMEOUT, 2000).
-define(KEY_COUNT, 20000).
-define(INITIATE_TRANSFER_TIMEOUT,
    ?VNODE_INACTIVITY_TIMEOUT * 2 + ?VNODE_MANAGEMENT_TIMEOUT
).
-define(RING_SIZE, 64).
    %% The previous test requires a ring-size of 64, due to issues with
    %% starting previous version nodes with non-default configurations
    %% If previous test is removed, then this may be changed
-define(CLAIM_ALGO, choose_claim_v4).
-define(KV_PROPS, [{anti_entropy, {off, []}}]).
-define(CORE_PROPS, 
    [
        {ring_creation_size,        ?RING_SIZE},
        {choose_claim_fun,          ?CLAIM_ALGO},
        {claimant_tick,             ?CLAIMANT_TICK},
        {
            default_bucket_props,
            [{allow_mult, true}, {dvv_enabled, true}, {n_val, ?N_VAL}]
        },
        {handoff_concurrency,       8},
        {forced_ownership_handoff,  8},
        {vnode_inactivity_timeout,  ?VNODE_INACTIVITY_TIMEOUT},
        {vnode_management_timer,    ?VNODE_MANAGEMENT_TIMEOUT}
    ]
).

-define(CONFIG(TransferPref, ListenerEnabled),
    [
        {
            riak_kv, ?KV_PROPS
        },
        {
            riak_core,
            ?CORE_PROPS ++
                [
                    {handoff_preference, TransferPref},
                    {handoff_listener, ListenerEnabled}
                ]
        }
    ]
).

-define(PREV_CONFIG,
    [
        {riak_kv, ?KV_PROPS},
        {riak_core, ?CORE_PROPS}
    ]
).

confirm() ->

    ?LOG_INFO(
        "Test joining three clusters with valid configurations. "
        "The first cluster will need an update as the third does not have a "
        "listener"
    ),

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(ip, true)},
            {2, ?CONFIG(dp, true)},
            {2, ?CONFIG(dp, false)}]),
    test_joins(ClusterA, ClusterB, ClusterC, true),

    rt:clean_cluster(ClusterA ++ ClusterB ++ ClusterC),

    ?LOG_INFO(
        "Test joining current clusters with previous clusters.  "
        "All clusters must have listener as ClusterA may be pre 3.4.2"
    ),
    ?LOG_INFO(
        "Test requires a ring-size of 64 due to issues with starting previous "
        "versions with non-default settings"
    ),

    [NodeP1, NodeP2, NodeC1, NodeC2, NodeC3, NodeC4] =
        rt:deploy_nodes(
            [previous, previous, current, current, current, current]
        ),

    ok = rt:update_app_config(NodeP1, ?PREV_CONFIG),
    ok = rt:update_app_config(NodeP2, ?PREV_CONFIG),
    ok = rt:update_app_config(NodeC1, ?CONFIG(ip, true)),
    ok = rt:update_app_config(NodeC2, ?CONFIG(ip, true)),
    ok = rt:update_app_config(NodeC3, ?CONFIG(dp, true)),
    ok = rt:update_app_config(NodeC4, ?CONFIG(dp, true)),
    
    test_joins([NodeP1, NodeP2], [NodeC1, NodeC2], [NodeC3, NodeC4], false),

    pass.


test_joins(ClusterA, ClusterB, ClusterC, ReqUpdate) ->
    Mod = nextgenrepl_ttaaefs_manual,

    [NodeA1, NodeA2] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,
    [NodeC1, NodeC2] = ClusterC,

    rt:create_and_activate_bucket_type(
        NodeA1, <<"nval1">>, [{n_val, 1}, {allow_mult, false}]
    ),

    NV1AT = {<<"nval1">>, <<"test_typed_bucketsA">>},
    CommonValBin = <<"CommonValueToWriteForNV4Objects">>,
    ok = 
        Mod:write_to_cluster(
            NodeA1,
            1,
            ?KEY_COUNT,
            NV1AT,
            true,
            CommonValBin
        ),

    NV1AN = <<"test_untyped_bucketsA">>,
    ok =
        Mod:write_to_cluster(
            NodeA2,
            1,
            ?KEY_COUNT,
            NV1AN,
            true,
            CommonValBin
        ),

    rt:join_cluster(ClusterA),

    timer:sleep(?INITIATE_TRANSFER_TIMEOUT),
    rt:wait_until_transfers_complete(ClusterA),

    ok = Mod:read_from_cluster(NodeA2, 1, ?KEY_COUNT, 0, NV1AT, CommonValBin),
    ok = Mod:read_from_cluster(NodeA1, 1, ?KEY_COUNT, 0, NV1AN, CommonValBin),

    ok =
        Mod:write_to_cluster(
            NodeB1,
            ?KEY_COUNT + 1,
            ?KEY_COUNT * 2,
            NV1AN,
            true,
            CommonValBin
        ),
    ok =
        Mod:write_to_cluster(
            NodeB2,
            (?KEY_COUNT * 2) + 1,
            ?KEY_COUNT * 3,
            NV1AN,
            true,
            CommonValBin
        ),

    join(NodeA1, ClusterB),

    timer:sleep(?INITIATE_TRANSFER_TIMEOUT),
    rt:wait_until_transfers_complete(ClusterA ++ ClusterB),

    ok =
        Mod:read_from_cluster(
            NodeA1,
            ?KEY_COUNT + 1,
            ?KEY_COUNT * 2,
            0,
            NV1AN,
            CommonValBin
        ),
    ok = 
        Mod:read_from_cluster(
            NodeA2,
            (?KEY_COUNT * 2) + 1,
            ?KEY_COUNT * 3,
            0,
            NV1AN,
            CommonValBin
        ),
    ok = Mod:read_from_cluster(NodeB2, 1, ?KEY_COUNT, 0, NV1AT, CommonValBin),
    ok = Mod:read_from_cluster(NodeB1, 1, ?KEY_COUNT, 0, NV1AN, CommonValBin),

    case ReqUpdate of
        true ->
            ok = rt:update_app_config(NodeA1, ?CONFIG(dp, true)),
            ok = rt:update_app_config(NodeA2, ?CONFIG(dp, true)),
            ok = rt:wait_until_all_members(ClusterA ++ ClusterB),
            timer:sleep(?INITIATE_TRANSFER_TIMEOUT),
            rt:wait_until_transfers_complete(ClusterA ++ ClusterB);
        false ->
            ok
    end,

    ok =
        Mod:write_to_cluster(
            NodeC1,
            (?KEY_COUNT * 3) + 1,
            ?KEY_COUNT * 4,
            NV1AN,
            true,
            CommonValBin
        ),
    ok =
        Mod:write_to_cluster(
            NodeC2,
            (?KEY_COUNT * 4) + 1,
            ?KEY_COUNT * 5,
            NV1AN,
            true,
            CommonValBin
        ),

    join(NodeA1, ClusterC),
    timer:sleep(?INITIATE_TRANSFER_TIMEOUT),
    rt:wait_until_transfers_complete(ClusterA ++ ClusterB ++ ClusterC),

    ok = Mod:read_from_cluster(NodeC2, 1, ?KEY_COUNT, 0, NV1AT, CommonValBin),
    ok = Mod:read_from_cluster(NodeA1, 1, ?KEY_COUNT * 5, 0, NV1AN, CommonValBin),

    ok.

join(Claimant, OtherNodes) ->
    lists:foreach(
        fun(Node) ->
            rt:staged_join(Node, Claimant)
        end,
        OtherNodes
    ),
    rt:plan_and_commit(Claimant),
    rt:try_nodes_ready(OtherNodes, 10, 500).