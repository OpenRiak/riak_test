%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%% @doc Tests to ensure a stalling or blocking fssource process does not
%% cause status call to timeout. Useful for only 2.0 and up (and up is
%% a regression test).
-module(repl_fs_stat_caching).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TEST_BUCKET, <<"repl_fs_stat_caching">>).
-define(RING_SIZE, 32).

confirm() ->
    {{SrcLead, SrcCluster}, {SinkLead, _SinkCluster}} = setup(),
    SinkPort = repl_util:get_cluster_mgr_port(SinkLead),
    repl_util:connect_cluster(SrcLead, "127.0.0.1", SinkPort),

    ?LOG_INFO("Loading source cluster"),
    [] = repl_util:do_write(SrcLead, 1, 1000, ?TEST_BUCKET, 1),

    repl_util:enable_fullsync(SrcLead, "sink"),
    rpc:call(SrcLead, riak_repl_console, fullsync, [["start", "sink"]]),

    % and now, the actual test.
    % find a random fssource, suspend it, and then ensure we can get a
    % status.
    {ok, Suspended} = suspend_an_fs_source(SrcCluster),
    ?LOG_INFO("Suspended: ~0p", [Suspended]),
    {ok, Status} = rt:riak_repl(SrcLead, ["status"]),
    FailLine = "RPC to '" ++ atom_to_list(SrcLead) ++ "' failed: timeout\n",
    ?assertNotEqual(FailLine, Status),

    true = rpc:block_call(node(Suspended), erlang, resume_process, [Suspended]),

    pass.

setup() ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    NodeCount = rt_config:get(num_nodes, 6),

    ?LOG_INFO("Deploy ~b nodes", [NodeCount]),
    Nodes = rt:deploy_nodes(NodeCount, cluster_conf(), [riak_kv, riak_repl]),
    SplitSize = NodeCount div 2,
    {SourceNodes, SinkNodes} = lists:split(SplitSize, Nodes),

    ?LOG_INFO("making cluster Source from ~0p", [SourceNodes]),
    repl_util:make_cluster(SourceNodes),

    ?LOG_INFO("making cluster Sink from ~0p", [SinkNodes]),
    repl_util:make_cluster(SinkNodes),

    SrcHead = hd(SourceNodes),
    SinkHead = hd(SinkNodes),
    repl_util:name_cluster(SrcHead, "source"),
    repl_util:name_cluster(SinkHead, "sink"),

    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),

    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkNodes),

    ok = repl_util:wait_until_leader_converge(SourceNodes),
    ok = repl_util:wait_until_leader_converge(SinkNodes),

    SourceLead = repl_util:get_leader(SrcHead),
    SinkLead = repl_util:get_leader(SinkHead),

    {{SourceLead, SourceNodes}, {SinkLead, SinkNodes}}.

cluster_conf() ->
    [
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {max_fssource_cluster, 3},
            {max_fssource_node, 1},
            {max_fssink_node, 20},
            {rtq_max_bytes, 1048576}
        ]},
        {riak_core, [
            {ring_creation_size,        ?RING_SIZE},
            {handoff_concurrency,       8},
            {forced_ownership_handoff,  8},
            {vnode_inactivity_timeout,  4000},
            {vnode_management_timer,    2000}
        ]}
    ].


suspend_an_fs_source(Nodes) ->
    suspend_an_fs_source(Nodes, 10000).

suspend_an_fs_source([_Node | _Tail], 0) ->
    {error, tries_ran_out};

suspend_an_fs_source([Node | Tail], TriesLeft) ->
    Pids = rpc:call(Node, riak_repl2_fssource_sup, enabled, []),
    case maybe_suspend_an_fs_source(Node, Pids) of
        false ->
            suspend_an_fs_source(Tail ++ [Node], TriesLeft - 1);
        Pid ->
            {ok, Pid}
    end.

maybe_suspend_an_fs_source(_Node, []) ->
    false;

maybe_suspend_an_fs_source(Node, [{_Remote, Pid} | Tail]) ->
    case rpc:block_call(Node, erlang, suspend_process, [Pid]) of
        false ->
            maybe_suspend_an_fs_source(Node, Tail);
        true ->
            Pid
    end.
