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
%% @deprecated snmp is no longer present in Riak 3+
-module(verify_snmp).
-deprecated(module).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    %% Bring up a small cluster
    Config = [{riak_snmp, [{polling_interval, 1000}]}],
    [Node1] = rt:deploy_nodes(1, Config),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    Keys = [{vnodeGets, <<"vnode_gets">>},
        {vnodePuts, <<"vnode_puts">>},
        {nodeGets, <<"node_gets">>},
        {nodePuts, <<"node_puts">>},
        {nodeGetTimeMean, <<"node_get_fsm_time_mean">>},
        {nodeGetTimeMedian, <<"node_get_fsm_time_median">>},
        {nodeGetTime95, <<"node_get_fsm_time_95">>},
        {nodeGetTime99, <<"node_get_fsm_time_99">>},
        {nodeGetTime100, <<"node_get_fsm_time_100">>},
        {nodePutTimeMean, <<"node_put_fsm_time_mean">>},
        {nodePutTimeMedian, <<"node_put_fsm_time_median">>},
        {nodePutTime95, <<"node_put_fsm_time_95">>},
        {nodePutTime99, <<"node_put_fsm_time_99">>},
        {nodePutTime100, <<"node_put_fsm_time_100">>}],

    ?LOG_INFO("Waiting for SNMP to start."),

    rpc:call(Node1, riak_core, wait_for_application, [snmp]),
    rpc:call(Node1, riak_core, wait_for_application, [riak_snmp]),

    ?LOG_INFO("Mapping SNMP names to OIDs"),

    OIDPairs = [begin
        {value, OID} = rpc:call(Node1, snmpa, name_to_oid, [SKey]),
        {OID ++ [0], HKey}
    end || {SKey, HKey} <- Keys],

    ?LOG_INFO("Doing some reads and writes to record some stats."),

    rt:systest_write(Node1, 10),
    rt:systest_read(Node1, 10),

    ?LOG_INFO("Waiting for HTTP Stats to be non-zero"),
    ?assertEqual(ok,
        rt:wait_until(Node1, fun(N) ->
            Stats = rt:get_stats(N),
            proplists:get_value(<<"vnode_gets">>, Stats) =/= 0
        end)),


    verify_eq(OIDPairs, Node1),
    pass.

verify_eq(Keys, Node) ->
    {OIDs, HKeys} = lists:unzip(Keys),
    ?assertEqual(ok,
        rt:wait_until(Node,
            fun(N) ->
                Stats = rt:get_stats(Node),
                SStats = rpc:call(N, snmpa, get, [snmp_master_agent, OIDs]),
                SPairs = lists:zip(SStats, HKeys),
                lists:all(
                    fun({A, B}) ->
                        Stat = proplists:get_value(B, Stats),
                        ?LOG_INFO("Comparing ~0p | Stats ~0p ~~ SNMP ~0p", [B, Stat, A]),
                        A == Stat
                    end,
                    SPairs)
            end)).
