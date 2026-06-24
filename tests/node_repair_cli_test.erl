%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 TI Tokyo.
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
-module(node_repair_cli_test).
-behavior(riak_test).

-export([confirm/0]).
-export([wait_until_repairs_complete/1,
         wait_until_repairs_complete/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    Nodes = rt:build_cluster(2),
    ok = node_repair_empty_test(Nodes),
    ok = node_repair_start_stop_test(Nodes),
    ok = node_repair_restart_test(Nodes),
    pass.

node_repair_empty_test([Node1, Node2]) ->
    ?LOG_INFO("* node repair status is clean on fresh nodes", []),
    {ok, Output1} = rt:admin(Node1, ["node", "repair", "status"]),
    ?assertMatch({match, _}, re:run(Output1, ff("No active node repairs on ~s", [Node1]))),
    {ok, Output2} = rt:admin(Node1, ["node", "repair", "status", "-n", ff("~s", [Node2])]),
    ?assertMatch({match, _}, re:run(Output2, ff("No active node repairs on ~s", [Node2]))),

    {ok, Output3} = rt:admin(Node1, ["node", "repair", "status", "-n", "all"]),
    ?assertMatch({match, _}, re:run(Output3, ff("No active node repairs on ~s", [Node1]))),
    ?assertMatch({match, _}, re:run(Output3, ff("No active node repairs on ~s", [Node2]))),

    {ok, Output3a} = rt:admin(Node1, ["node", "repair", "status", "-n", "all", "-f", "json"]),
    [JsonL1, _] = string:split(Output3a, "\n"),
    [JN1, JN2] = mochijson2:decode(JsonL1, [{format, map}]),
    Node1bin = atom_to_binary(Node1),
    Node2bin = atom_to_binary(Node2),
    #{<<"node">> := Node1bin, <<"status">> := []} = JN1,
    #{<<"node">> := Node2bin, <<"status">> := []} = JN2,

    ok.

node_repair_start_stop_test([Node1, Node2]) ->
    ?LOG_INFO("* node repair can only be started when no nodes run repairs", []),
    {ok, Output1} = rt:admin(Node1, ["node", "repair", "start"]),
    ?assertMatch({match, _}, re:run(Output1, ff("Node repair started on ~s", [Node1]))),

    {ok, Output2} = rt:admin(Node1, ["node", "repair", "status", "-n", "all"]),
    ?assertMatch({match, _}, re:run(Output2, ff("Node repairs on ~s", [Node1]))),

    {ok, Output2a} = rt:admin(Node1, ["node", "repair", "status", "-n", "all", "-f", "json"]),
    [JsonL1, _] = string:split(Output2a, "\n"),
    [JN1, _JN2] = mochijson2:decode(JsonL1, [{format, map}]),
    Node1bin = atom_to_binary(Node1),
    #{<<"node">> := Node1bin, <<"status">> := RepParts1} = JN1,
    ?assert(RepParts1 /= []),

    {ok, Output3} = rt:admin(Node1, ["node", "repair", "start"]),
    ?assertMatch({match, _}, re:run(Output3, ff("There are repairs currently ongoing on node", []))),
    {ok, Output4} = rt:admin(Node2, ["node", "repair", "start"]),
    ?assertMatch({match, _}, re:run(Output4, ff("There are repairs currently ongoing on node", []))),

    ok.

node_repair_restart_test([Node1, _] = Nodes) ->
    ?LOG_INFO("* node repair can be restarted", []),
    %% a repair is in progress, started in the previous test: try stop and resume it

    ok = rt_logger:plugin_logger(Node1),

    {ok, PreStopStatus} = rt:admin(Node1, ["node", "repair", "status", "-f", "json"]),

    RepairStopReason = "justBecause",
    {ok, Output1} = rt:admin(Node1, ["node", "repair", "stop", RepairStopReason]),
    ?assertMatch({match, _}, re:run(Output1, ff("~s: Node repair stopped", [Node1]))),
    {ok, Output2} = rt:admin(Node1, ["node", "repair", "status"]),
    ?assertMatch({match, _}, re:run(Output2, ff("No active node repairs on ~s", [Node1]))),
    rt:expect_in_log(Node1, RepairStopReason),

    {ok, Output3} = rt:admin(Node1, ["node", "repair", "start"]),
    ?assertMatch({match, _}, re:run(Output3, ff("Node repair started on ~s", [Node1]))),

    {ok, PostResumeStatus} = rt:admin(Node1, ["node", "repair", "status", "-f", "json"]),
    ?LOG_INFO("* checking that resumed repairs match the pre-stop state", []),
    ?assertEqual(PreStopStatus, PostResumeStatus),

    wait_until_repairs_complete(Nodes, 500, 2000),

    ok.

ff(F, A) ->
    lists:flatten(io_lib:format(F, A)).

wait_until_repairs_complete(Nodes) ->
    {Delay, Retry} = rt:get_retry_settings(),
    wait_until_repairs_complete(Nodes, Retry, Delay).
wait_until_repairs_complete([N1|_] = Nodes, Retry, Delay) ->
    rt:wait_until(
      fun() ->
              {ok, Out} = rt:admin(N1, ["node", "repair", "status", "-n", "all"]),
              lists:foldl(
                fun(_, false) -> false;
                   (N, true) ->
                        nomatch /= re:run(Out, ff("No active node repairs on ~s", [N])) end,
                true,
                Nodes)
      end, Retry, Delay
     ).
