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
-module(riak_rex).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% @doc riak_test entry point
confirm() ->
    SetupData = setup(current),
    rex_test(SetupData),
    pass.

setup(Type) ->
    deploy_node(Type).

rex_test(Node) ->
    % validated we can get the rex pid on the node
    RexPid1 = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(Node, node(RexPid1)),
    % kill rex on the node and check that safe_rpc works
    % - as in it doesn't crash
    kill_rex(Node),
    NotPid = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    case rt:otp_release() of
        AtLeast23 when AtLeast23 >= 23 ->
            %% As of OTP 23 killing rex no longer crashes the rpc call
            ?assertMatch(undefined, NotPid);
        _Pre23 ->
            ?assertMatch({badrpc, _}, NotPid)
    end,
    % restart rex
    supervisor:restart_child({kernel_sup, Node}, rex),
    RexPid2 = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(node(RexPid2), Node).


deploy_node(NumNodes, current) ->
    rt:deploy_nodes(NumNodes, conf()).

deploy_node(Type) ->
    NumNodes = rt_config:get(num_nodes, 1),

    ?LOG_INFO("Deploy ~b node", [NumNodes]),
    Node = deploy_node(NumNodes, Type),
    ?LOG_INFO("Node: ~0p", [Node]),
    hd(Node).

kill_rex(Node) ->
    ok = supervisor:terminate_child({kernel_sup, Node}, rex).

conf() ->
    [
     {riak_kv,
      [
       {anti_entropy, {off, []}}
      ]
     }
    ].
