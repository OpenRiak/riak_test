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
-module(ensemble_util).

-compile([export_all, nowarn_export_all]).

-define(DEFAULT_RING_SIZE, 16).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

build_cluster(Num, Config, NVal) ->
    Nodes = rt:deploy_nodes(Num, Config),
    Node = hd(Nodes),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node),
    ensemble_util:wait_until_stable(Node, NVal),
    Nodes.

build_cluster_without_quorum(Num, Config) ->
    Nodes = rt:deploy_nodes(Num, Config),
    rt:setup_log_capture(Nodes),
    Node = hd(Nodes),
    ok = rpc:call(Node, riak_ensemble_manager, enable, []),
    _ = rpc:call(Node, riak_core_ring_manager, force_update, []),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node),
    Nodes.

fast_config(NVal) ->
    fast_config(NVal, ?DEFAULT_RING_SIZE).

fast_config(Nval, RingSize) when is_integer(RingSize) ->
    fast_config(Nval, RingSize, true);
fast_config(Nval, EnableAAE) when is_boolean(EnableAAE) ->
    fast_config(Nval, ?DEFAULT_RING_SIZE, EnableAAE).

fast_config(NVal, RingSize, EnableAAE) ->
    [config_aae(EnableAAE),
     {riak_core, [{default_bucket_props,
          [
             {n_val, NVal},
             {allow_mult, true},
             {dvv_enabled, true}
          ]},
            {vnode_inactivity_timeout, 15000},
            {forced_ownership_handoff, 8},
            {handoff_concurrency, 8},
            {target_n_val, max(4, NVal)},
            {ring_creation_size, RingSize},
            {enable_consensus, true}]}].

config_aae(true) ->
    {riak_kv, [{anti_entropy_build_limit, {100, 1000}},
               {anti_entropy_concurrency, 100},
               {anti_entropy_tick, 1000},
               {anti_entropy, {on, []}},
               {anti_entropy_timeout, 5000}]};
config_aae(false) ->
    {riak_kv, [{anti_entropy, {off, []}}]}.

ensembles(Node) ->
    rpc:call(Node, riak_kv_ensembles, ensembles, []).

get_leader_pid(Node, Ensemble) ->
    rpc:call(Node, riak_ensemble_manager, get_leader_pid, [Ensemble]).

peers(Node) ->
    rpc:call(Node, riak_ensemble_peer_sup, peers, []).

kill_leader(Node, Ensemble) ->
    case get_leader_pid(Node, Ensemble) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            ok
    end.

kill_leaders(Node, Ensembles) ->
    _ = [kill_leader(Node, Ensemble) || Ensemble <- Ensembles],
    ok.

wait_until_cluster(Nodes) ->
    ?LOG_INFO("Waiting until riak_ensemble cluster includes all nodes"),
    Node = hd(Nodes),
    F = fun() ->
                case rpc:call(Node, riak_ensemble_manager, cluster, []) of
                    Nodes ->
                        true;
                    _ ->
                        false
                end
        end,
    ?assertEqual(ok, rt:wait_until(F)),
    ?LOG_INFO("....cluster ready"),
    ok.

wait_until_stable(Node, Count) ->
    ?LOG_INFO("Waiting until all ensembles are stable"),
    Ensembles = rpc:call(Node, riak_kv_ensembles, ensembles, []),
    wait_until_quorum(Node, root),
    [wait_until_quorum(Node, Ensemble) || Ensemble <- Ensembles],
    ?LOG_INFO("All ensembles have quorum"),
    [wait_until_quorum_count(Node, Ensemble, Count) || Ensemble <- Ensembles],
    ?LOG_INFO("All ensembles have quorum count ~w confirmed", [Count]),
    ?LOG_INFO("....all stable"),
    ok.

wait_until_quorum(Node, Ensemble) ->
    F = fun() ->
                case rpc:call(Node, riak_ensemble_manager, check_quorum,
                        [Ensemble, 10000]) of
                    true ->
                        true;
                    false ->
                        ?LOG_INFO("Quorum not ready: ~0p", [Ensemble]),
                        false
                end
        end,
    ?assertEqual(ok, rt:wait_until(F)).

wait_until_quorum_count(Node, Ensemble, Want) ->
    F = fun() ->
                case rpc:call(Node, riak_ensemble_manager, count_quorum,
                        [Ensemble, 10000]) of
                    Count when Count >= Want ->
                        true;
                    Count ->
                        ?LOG_INFO("Count: ~0p :: ~0p < ~0p", [Ensemble, Count, Want]),
                        false
                end
        end,
    ?assertEqual(ok, rt:wait_until(F)).

wait_for_membership(Node) ->
    ?LOG_INFO("Waiting until ensemble membership matches ring ownership"),
    F = fun() ->
                case rpc:call(Node, riak_kv_ensembles, check_membership, []) of
                    Results when is_list(Results) ->
                        [] =:= [x || false <- Results];
                    _ ->
                        false
                end
        end,
    ?assertEqual(ok, rt:wait_until(F)),
    ?LOG_INFO("....ownership matches"),
    ok.
