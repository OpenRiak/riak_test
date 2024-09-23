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
-module(ensemble_basic2).
-behavior(riak_test).

-export([confirm/0]).

-compile({parse_transform, rt_intercept_pt}).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    NumNodes = 6,
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    ?LOG_INFO("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    Node = hd(Nodes),
    Ensembles = ensemble_util:ensembles(Node),
    ?LOG_INFO("Killing all ensemble leaders"),
    ok = ensemble_util:kill_leaders(Node, Ensembles),
    ensemble_util:wait_until_stable(Node, NVal),
    Peers = [PeerId || {PeerId, _PeerPid} <- ensemble_util:peers(Node)],
    ?LOG_INFO("Verifying peers wait for riak_kv_service"),
    Delay = rt_config:get(kv_vnode_delay, 5000),
    rt_intercept:add_and_save(Node, {riak_kv_vnode, [{{init, 1}, {[Delay],
                              fun(Args) ->
                                      timer:sleep(Delay),
                                      riak_kv_vnode_orig:init_orig(Args)
                              end}}]}),
    rt:stop_and_wait(Node),
    rt:start(Node),
    ?LOG_INFO("Polling peers while riak_kv starts. We should see none"),
    UpNoPeersFun =
        fun() ->
            PL = ensemble_util:peers(Node),
            NodePeers = [P || {P, _} <- PL],
            NonRootPeers = [P || P <- NodePeers, element(1, P) /= root],
            S = rpc:call(Node, riak_core_node_watcher, services, [Node]),
            case S of
                L when is_list(L) ->
                    case lists:member(riak_kv, L) of
                        true ->
                            true;
                        false ->
                            ?assertEqual([], NonRootPeers)
                    end
            end
        end,
    rt:wait_until(UpNoPeersFun),
    ?LOG_INFO("Perfect. riak_kv is now up and no peers started before that. "
               "Now check they come back up"),
    SPeers = lists:sort(Peers),
    ?assertEqual(
        ok,
        rt:wait_until(
            fun() ->
                L = ensemble_util:peers(Node),
                L2 = lists:sort([P || {P, _} <- L]),
                SPeers == L2
            end
        )
    ),
    ?LOG_INFO("All expected peers are back. Life is good"),
    pass.
