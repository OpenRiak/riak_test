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
-module(verify_tick_change).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    ClusterSize = 4,
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    NewConfig = [],
    Nodes = rt:build_cluster(ClusterSize, NewConfig),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    [Node1|_] = Nodes,
    Bucket = <<"systest">>,
    Start = 0, End = 100,
    W = quorum,
    NewTime = 11,

    write_stuff(Nodes, Start, End, Bucket, W, <<>>),
    read_stuff(Nodes, Start, End, Bucket, W, <<>>),

    ?LOG_INFO("Start ticktime daemon on ~0p, then wait a few seconds",[Node1]),
    rpc:call(Node1, riak_core_net_ticktime, start_set_net_ticktime_daemon,
             [Node1, NewTime]),
    timer:sleep(2*1000),

    ?LOG_INFO("Changing net_ticktime to ~0p", [NewTime]),
    ok = rt:wait_until(
           fun() ->
                   write_read_poll_check(Nodes, NewTime, Start, End, Bucket, W)
           end),
    ?LOG_INFO("If we got this far, then we found no inconsistencies"),
    [begin
        rt:wait_until(
            fun() ->
                RemoteTime = rpc:call(Node, net_kernel, get_net_ticktime, []),
                ?LOG_INFO("Node ~0p tick is ~0p", [Node, RemoteTime]),
                RemoteTime == NewTime
            end
        )
     end || Node <- lists:usort([node()|nodes(connected)])],
    ?LOG_INFO("If we got this far, all nodes are using the same tick time"),

    pass.

make_common() ->
    list_to_binary(io_lib:format("~0p", [os:timestamp()])).

write_stuff(Nodes, Start, End, Bucket, W, Common) ->
    Nd = lists:nth(length(Nodes), Nodes),
    [] = rt:systest_write(Nd, Start, End, Bucket, W, Common).

read_stuff(Nodes, Start, End, Bucket, W, Common) ->
    Nd = lists:nth(length(Nodes), Nodes),
    [] = rt:systest_read(Nd, Start, End, Bucket, W, Common).

is_set_net_ticktime_done(Nodes, Time) ->
    case lists:usort([(catch rpc:call(Node, net_kernel, get_net_ticktime,[]))
                      || Node <- Nodes]) of
        [Time] ->
            true;
        _ ->
            false
    end.

write_read_poll_check(Nodes, NewTime, Start, End, Bucket, W) ->
    Common = make_common(),
    write_stuff(Nodes, Start, End, Bucket, W, Common),
    read_stuff(Nodes, Start, End, Bucket, W, Common),
    is_set_net_ticktime_done(Nodes, NewTime).
