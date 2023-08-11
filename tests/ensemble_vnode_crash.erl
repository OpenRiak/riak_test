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
-module(ensemble_vnode_crash).
-behavior(riak_test).

-export([confirm/0]).

-compile({parse_transform, rt_intercept_pt}).

-include_lib("kernel/include/logger.hrl").

-define(M, riak_kv_ensemble_backend_orig).

confirm() ->
    NumNodes = 6,
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    ?LOG_INFO("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    vnode_util:load(Nodes),
    Node = hd(Nodes),
    ensemble_util:wait_until_stable(Node, NVal),

    ?LOG_INFO("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],

    Key1 = hd(Keys),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key1}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {{Key1Idx, Key1Node}, _} = hd(PL),

    PBC = rt:pbc(Node),

    ?LOG_INFO("Writing ~b consistent keys", [1000]),
    WriteFun =
        fun(Key) ->
            ok =
                case rt:pbc_write(PBC, Bucket, Key, Key) of
                    ok ->
                        ok;
                    E ->
                        ?LOG_INFO("Error ~w with Key ~0p", [E, Key]),
                        E
                end
        end,
    lists:foreach(WriteFun, Keys),

    ?LOG_INFO("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    %% Setting up intercept to ensure that
    %% riak_kv_ensemble_backend:handle_down/4 gets called when a vnode or vnode
    %% proxy crashes for a given key
    ?LOG_INFO("Adding Intercept for riak_kv_ensemble_backend:handle_down/4"),
    Self = self(),
    rt_intercept:add(Key1Node, {riak_kv_ensemble_backend, [{{handle_down, 4},
        {[Self],
        fun(Ref, Pid, Reason, State) ->
            Self ! {handle_down, Reason},
            ?M:maybe_async_update_orig(Ref, Pid, Reason, State)
        end}}]}),

    {ok, VnodePid} =
        rpc:call(Key1Node, riak_core_vnode_manager, get_vnode_pid,
                    [Key1Idx, riak_kv_vnode]),

    ?LOG_INFO("Killing Vnode ~0p for Key1 {~0p, ~0p}",
                [VnodePid, Key1Node, Key1Idx]),
    spawn(fun() -> kill_vnode(Key1Node, VnodePid) end),

    ?LOG_INFO("Waiting to receive msg indicating downed vnode"),
    NVal = wait_for_all_handle_downs(0),

    ?LOG_INFO("Wait for stable ensembles"),
    ensemble_util:wait_until_stable(Node, NVal),
    ?LOG_INFO("Re-reading keys"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    ?LOG_INFO("Killing Vnode Proxy for Key1"),
    Proxy = rpc:call(Key1Node, riak_core_vnode_proxy, reg_name, [riak_kv_vnode,
            Key1Idx]),
    ProxyPid = rpc:call(Key1Node, erlang, whereis, [Proxy]),

    ?LOG_INFO("Killing Vnode Proxy ~0p", [Proxy]),
    spawn(fun() -> kill_vnode(Key1Node, ProxyPid) end),

    ?LOG_INFO("Waiting to receive msg indicating downed vnode proxy:"),
    NVal = wait_for_all_handle_downs(0),

    ?LOG_INFO("Wait for stable ensembles"),
    ensemble_util:wait_until_stable(Node, NVal),
    ?LOG_INFO("Re-reading keys"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    pass.

wait_for_all_handle_downs(Count) ->
    receive
        {handle_down, _} ->
            wait_for_all_handle_downs(Count+1)
    after 5000 ->
            Count
    end.


kill_vnode(Key1Node, Pid) ->
    %% Make sure that monitor started
    timer:sleep(1000),
    ?LOG_INFO("Actual kill happening"),
    true = rpc:call(Key1Node, erlang, exit, [Pid, testkill]).
