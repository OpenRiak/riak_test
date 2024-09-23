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
-module(ensemble_basic3).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").

confirm() ->
    NumNodes = 6,
    NVal = 5,
    Quorum = NVal div 2 + 1,
    Config = ensemble_util:fast_config(NVal, 32),
    ?LOG_INFO("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    vnode_util:load(Nodes),
    Node = hd(Nodes),
    Ensembles = ensemble_util:ensembles(Node),
    ?LOG_INFO("Killing all ensemble leaders"),
    ok = ensemble_util:kill_leaders(Node, Ensembles),
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
    All = [VN || {VN, _} <- PL],
    Other = [VN || {VN={_, Owner}, _} <- PL,
                   Owner =/= Node],

    Minority = NVal - Quorum,
    PartitionedVN = lists:sublist(Other, Minority),
    Partitioned = [VNode || {_, VNode} <- PartitionedVN],
    MajorityVN = All -- PartitionedVN,

    PBC = rt:pbc(Node),

    ?LOG_INFO("Partitioning quorum minority: ~0p", [Partitioned]),
    Part = rt:partition(Nodes -- Partitioned, Partitioned),
    ensemble_util:wait_until_stable(Node, Quorum),

    ?LOG_INFO("Writing ~b consistent keys", [1000]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    ?LOG_INFO("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    ?LOG_INFO("Healing partition"),
    rt:heal(Part),

    ?LOG_INFO("Suspending majority vnodes"),
    L = [begin
             ?LOG_INFO("Suspending vnode: ~0p", [VIdx]),
             Pid = vnode_util:suspend_vnode(VNode, VIdx),
             {VN, Pid}
         end || VN={VIdx, VNode} <- MajorityVN],
    L2 = orddict:from_list(L),

    L3 = lists:foldl(fun({VN={VIdx, VNode}, Pid}, Suspended) ->
                        ?LOG_INFO("Resuming vnode: ~0p", [VIdx]),
                        vnode_util:resume_vnode(Pid),
                        ensemble_util:wait_until_stable(Node, Quorum),
                        ?LOG_INFO("Re-reading keys"),
                        [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],
                        ?LOG_INFO("Suspending vnode: ~0p", [VIdx]),
                        Pid2 = vnode_util:suspend_vnode(VNode, VIdx),
                        orddict:store(VN, Pid2, Suspended)
                end, orddict:new(), L2),

    ?LOG_INFO("Resuming all vnodes"),
    [vnode_util:resume_vnode(Pid) || {_, Pid} <- L3],
    ensemble_util:wait_until_stable(Node, NVal),
    ?LOG_INFO("Re-reading keys"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],
    pass.
