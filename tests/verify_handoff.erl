%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
%% Copyright (c) 2018-2023 Workday, Inc.
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
-module(verify_handoff).
-behavior(riak_test).

-export([confirm/0]).

%% Called by verify_handoff_... tests
-export([
    backend_dir/1,
    run_test/4
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% We've got a separate test for capability negotiation and other mechanisms,
%% so the test here is fairly straightforward: get a list of different versions
%% of nodes and join them into a cluster, making sure that each time our data
%% has been replicated
confirm() ->
    NTestItems    = 1000,   %% How many test items to write/verify?
    NTestNodes    = 3,      %% How many nodes to spin up for tests?
    FastTestMode  = true,   %% Set to false for "production tests", true if too slow.

    run_test(FastTestMode, NTestItems, NTestNodes, default).

-spec run_test(
    FastTestMode :: boolean(),
    NTestItems :: pos_integer(),
    NTestNodes :: pos_integer(),
    Encoding :: atom() )
        -> pass | fail | no_return().

run_test(FastTestMode, NTestItems, NTestNodes, Encoding) ->
    ?LOG_INFO("Testing handoff (items: ~0p, encoding: ~0p)", [NTestItems, Encoding]),

    ?LOG_INFO("Spinning up ~b test nodes", [NTestNodes]),
    [RootNode | TestNodes] = Nodes =
        deploy_test_nodes(FastTestMode, Encoding, NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    %% Insert delay into handoff folding to test the efficacy of the
    %% handoff heartbeat addition
    [rt_intercept:add(N, {riak_core_handoff_sender,
                          [{{visit_item, 3}, delayed_visit_item_3}]})
     || N <- Nodes],

    ?LOG_INFO("Populating root node."),
    rt:systest_write(RootNode, NTestItems),
    %% write one object with a bucket type
    rt:create_and_activate_bucket_type(RootNode, <<"type">>, []),
    %% allow cluster metadata some time to propagate
    rt:systest_write(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),

    %% Test ownership handoff on each node:
    ?LOG_INFO("Testing ownership handoff for cluster."),
    lists:foreach(
        fun(TestNode) ->
            test_ownership_handoff(RootNode, TestNode, NTestItems)
        end, TestNodes),

    %% Test hinted handoff by stopping the root node and forcing some handoff
    test_hinted_handoff(Nodes, NTestItems),

    %% Test repair handoff by stopping the root node, deleting data, and running a repair
    test_repair_handoff(Nodes),

    pass.

%% See if we get the same data back from our new nodes as we put into the root node:
test_ownership_handoff(RootNode, NewNode, NTestItems) ->

    ?LOG_INFO("Testing ownership handoff adding node ~0p ...", [NewNode]),

    ?LOG_INFO("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    ?LOG_INFO("Joining new node ~0p with cluster.", [NewNode]),
    rt:staged_join(NewNode, RootNode),
    rt:plan_and_commit(NewNode),
    rt:wait_until_nodes_ready([RootNode, NewNode]),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),

    %% See if we get the same data back from the joined node that we added to the root node.
    %%  Note: systest_read() returns /non-matching/ items, so getting nothing back is good:
    ?LOG_INFO("Validating data after handoff:"),
    Results = rt:systest_read(NewNode, NTestItems),
    ?assertEqual(0, length(Results)),
    ?LOG_INFO("Validating data after handoff - typed bucket:"),
    Results2 = rt:systest_read(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),
    ?assertEqual(0, length(Results2)),
    ?LOG_INFO("Data looks ok.").

test_hinted_handoff([RootNode | OtherNodes] = Nodes, NTestItems) ->

    ?LOG_INFO(
        "Testing hinted handoff by stopping ~0p, doing some writes, and restarting ...",
        [RootNode]),

    rt:stop(RootNode),

    AnotherNode = rt_util:random_element(OtherNodes),
    ?LOG_INFO("Writing ~0p objects to ~0p ...", [NTestItems, AnotherNode]),
    rt:systest_write(AnotherNode, NTestItems),
    Results = rt:systest_read(AnotherNode, NTestItems),
    ?assertMatch(0, length(Results)),

    rt:start(RootNode),
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes).

test_repair_handoff(Nodes) ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    case backend_dir(Backend) of
        [_|_] = BackendDir ->
            test_repair_handoff(Nodes, BackendDir);
        _ ->
            ?LOG_INFO(
                "Skipping handoff repair test for unsupported backend: ~0p",
                [Backend])
    end.

test_repair_handoff([RootNode | _OtherNodes] = Nodes, BackendDir) ->

    ?LOG_INFO(
        "Testing repair handoff by stopping ~0p, deleting some data,"
        " and issuing a repair ...", [RootNode]),

    %% Pick a random partition to delete/repair
    Partition = rt_util:random_element(rt:partitions_for_node(RootNode)),

    rt:stop(RootNode),

    delete_partition_dir(RootNode, BackendDir, Partition),

    rt:start(RootNode),
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    issue_repair(RootNode, Partition),
    rt:wait_until_transfers_complete(Nodes).

backend_dir(bitcask = Backend) ->
    erlang:atom_to_list(Backend);
backend_dir(leveled = Backend) ->
    erlang:atom_to_list(Backend);
backend_dir(eleveldb) ->
    "leveldb";
backend_dir(undefined) ->
    backend_dir(bitcask);
backend_dir(_) ->
    undefined.

delete_partition_dir(Node, BackendDir, Partition) ->
    BEPath = filename:join(BackendDir, erlang:integer_to_list(Partition)),
    rt:clean_data_dir(Node, BEPath).

issue_repair(Node, Partition) ->
    ?LOG_INFO("issuing repair on node ~0p partition ~0p", [Node, Partition]),
    ?assertMatch({ok, [_|_]}, rpc:call(Node, riak_kv_vnode, repair, [Partition])).

-spec deploy_test_nodes(
    FastTestMode :: boolean(),
    Encoding :: atom(),
    NumNodes :: pos_integer() ) -> rtt:nodes().
%% This *should not* need to invoke rt:wait_for_service/2, as it's already
%% been called by rt:deploy_nodes/2.
deploy_test_nodes(FastTestMode, default = Encoding, NumNodes) ->
    NodeConfig = node_config(FastTestMode),
    [RootNode | _] = Nodes = rt:deploy_nodes(NumNodes, NodeConfig),
    assert_supported_encoding(RootNode, Encoding),
    ?LOG_INFO("Using default handoff encoding '~0p'",
        [rt:capability(RootNode, {riak_kv, handoff_data_encoding})]),
    Nodes;
deploy_test_nodes(FastTestMode, Encoding, NumNodes) ->
    EncodingConfig =
        {riak_core, [
            {override_capability, [
                {handoff_data_encoding, [
                    {   use, Encoding},
                    {prefer, Encoding}
                ]}
            ]}
        ]},
    NodeConfig = [EncodingConfig | node_config(FastTestMode)],
    [RootNode | _] = Nodes = rt:deploy_nodes(NumNodes, NodeConfig),
    assert_supported_encoding(RootNode, Encoding),
    CapabilityKey = {riak_kv, handoff_data_encoding},
    CapFun = fun(Node) ->
        ?assertEqual(Encoding, rt:capability(Node, CapabilityKey))
    end,
    _ = rt:pmap(CapFun, Nodes),
    ?LOG_INFO("Using handoff encoding '~0p'", [Encoding]),
    Nodes.

-spec node_config(FastTestMode :: boolean()) -> rtt:app_config().
%% For some testing purposes, making these limits smaller is helpful,
%% or at least exercise some more code.
%% It's questionable what value some of these have in this context,
%% but they're preserved for posterity.
node_config(true) ->
    ?LOG_INFO("WARNING: Using turbo settings for testing."),
    [{riak_core, [
        {forced_ownership_handoff,      8},
        {gossip_limit,                  {10000000, 60000}},
        {handoff_concurrency,           8},
        {handoff_receive_timeout,       2000},
        {ring_creation_size,            8},
        {vnode_inactivity_timeout,      1000}
    ]}];
node_config(_) ->
    [{riak_core, [
        {handoff_acksync_threshold,     5},
        {handoff_batch_threshold_count, 20},
        {handoff_receive_timeout,       2000},
        {ring_creation_size,            8}
    ]}].

assert_supported_encoding(Node, Encoding) ->
    %% Need the full set of capabilities to get the '$supported' record.
    AllCaps = rt:capability(Node, all),
    SupCaps = proplists:get_value('$supported', AllCaps),
    SupEncs = proplists:get_value({riak_kv, handoff_data_encoding}, SupCaps),
    ?LOG_INFO("Supported handoff encodings: ~0p", [SupEncs]),
    Encoding =:= default orelse ?assert(lists:member(Encoding, SupEncs)).
