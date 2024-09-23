%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(vnode_util).
-compile([export_all, nowarn_export_all]).

-include_lib("kernel/include/logger.hrl").

load(Nodes) ->
    rt:load_modules_on_nodes([?MODULE], Nodes),
    ok.

suspend_vnode(Node, Idx) ->
    ?LOG_INFO("Suspending vnode ~0p/~0p", [Node, Idx]),
    Pid = rpc:call(Node, ?MODULE, remote_suspend_vnode, [Idx], infinity),
    Pid.

remote_suspend_vnode(Idx) ->
    Parent = self(),
    Pid = spawn(fun() ->
                        {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
                        erlang:suspend_process(Pid, []),
                        Parent ! suspended,
                        receive resume ->
                                io:format("Resuming vnode :: ~0p/~0p~n", [node(), Idx]),
                                erlang:resume_process(Pid)
                        end
                end),
    receive suspended -> ok end,
    Pid.

resume_vnode(Pid) ->
    Pid ! resume.

kill_vnode({VIdx, VNode}) ->
    ?LOG_INFO("Killing vnode: ~0p", [VIdx]),
    Pid = vnode_pid(VNode, VIdx),
    rpc:call(VNode, erlang, exit, [Pid, kill]),
    ok = rt:wait_until(fun() ->
                               vnode_pid(VNode, VIdx) /= Pid
                       end).

vnode_pid(Node, Partition) ->
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, riak_kv_vnode]),
    Pid.

rebuild_vnode({VIdx, VNode}) ->
    ?LOG_INFO("Rebuild AAE tree: ~0p", [VIdx]),
    rebuild_aae_tree(VNode, VIdx).

rebuild_aae_tree(Node, Partition) ->
    {ok, Pid} = rpc:call(Node, riak_kv_vnode, hashtree_pid, [Partition]),
    Info = rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []),
    {_, Built} = lists:keyfind(Partition, 1, Info),
    ?LOG_INFO("Forcing rebuild of AAE tree for: ~b", [Partition]),
    ?LOG_INFO("Tree originally built at: ~0p", [Built]),
    rpc:call(Node, riak_kv_index_hashtree, clear, [Pid]),
    ok = rt:wait_until(fun() ->
                               NewInfo = rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []),
                               {_, NewBuilt} = lists:keyfind(Partition, 1, NewInfo),
                               NewBuilt > Built
                       end),
    ?LOG_INFO("Tree successfully rebuilt"),
    ok.
