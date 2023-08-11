%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%% Copyright (c) 2022 Workday, Inc.
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
-module(verify_removed_capability).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("stdlib/include/assert.hrl").

%% Start 3 nodes, create a capability and join them into a cluster
%%
%% Stop one node then restart it again, it joins the cluster without the
%% capability defined.
%%
%% The capability on the two nodes which have not been shut down should be
%% renegotiated to be the default value.
confirm() ->
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_1},
    V1 = 1,
    V2 = 2,
    ?assertMatch(ok, rpc:call(
        Node_A, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1])),
    ?assertMatch(ok, rpc:call(
        Node_B, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1])),
    ?assertMatch(ok, rpc:call(
        Node_C, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1])),
    ?assertMatch(ok, rt:join_cluster([Node_A,Node_B,Node_C])),
    ?assertMatch(ok, rt:wait_until_ring_converged([Node_A,Node_B,Node_C])),
    ?assertMatch(ok, rt:wait_until_capability(Node_A, Cap_name, V2)),
    ?assertMatch(ok, rt:wait_until_capability(Node_B, Cap_name, V2)),
    ?assertMatch(ok, rt:wait_until_capability(Node_C, Cap_name, V2)),
    ?assertMatch(ok, rt:stop(Node_B)),
    ?assertMatch(ok, rt:start(Node_B)),
    ?assertMatch(ok, rt:wait_until_capability(Node_A, Cap_name, V1)),
    ?assertMatch(ok, rt:wait_until_capability(Node_C, Cap_name, V1)),
    pass.
