%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
-module(verify_kv_health_check).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DISABLE_THRESHOLD, 10).
-define(ENABLE_THRESHOLD, 9).
-define(CONFIG,
        [{riak_kv, [{enable_health_checks, true},
                    {vnode_mailbox_limit,
                     {?ENABLE_THRESHOLD, ?DISABLE_THRESHOLD}}]}]).

%% NOTE: As of Riak 1.3.2, health checks are deprecated as they
%% may interfere with the new overload protection mechanisms.
%% This test now exists only for backwards compatibility.
confirm() ->
    [Node1, Node2, _Node3] = rt:build_cluster(3, ?CONFIG),

    %% add intercept that delays handling of vnode commands
    %% on a single node (the "slow" node)
    rt_intercept:load_code(Node1),
    rt_intercept:add(Node1, {riak_kv_vnode,
                             [{{handle_command, 3}, slow_handle_command}]}),
    ?LOG_INFO("Installed intercept to delay handling of requests by kv_vnode on ~0p",
               [Node1]),

    %% make DISABLE_THRESHOLD+5 requests and trigger the health check explicitly
    %% we only need to backup one vnode's msg queue on the node to fail the health check
    %% so we read the same key again and again
    C = rt:pbc(Node2),
    [riakc_pb_socket:get(C, <<"b">>, <<"k">>) || _ <- lists:seq(1,?DISABLE_THRESHOLD+5)],
    ok = rpc:call(Node1, riak_core_node_watcher, check_health, [riak_kv]),

    ?LOG_INFO("health check should disable riak_kv on ~0p shortly", [Node1]),
    ?assertMatch(ok,
                 rt:wait_until(
                   Node1,
                   fun(N) ->
                           Up = rpc:call(N, riak_core_node_watcher, services, [N]),
                           not lists:member(riak_kv, Up)
                   end)),
    ?LOG_INFO("health check successfully disabled riak_kv on ~0p", [Node1]),
    ?LOG_INFO("health check should re-enable riak_kv on ~0p after some messages have been processed",
               [Node1]),
    %% wait for health check timer to do its thing, don't explicitly execute it
    ok = rt:wait_for_service(Node1, riak_kv),
    ?LOG_INFO("health check successfully re-enabled riak_kv on ~0p", [Node1]),
    riakc_pb_socket:stop(C),
    pass.
