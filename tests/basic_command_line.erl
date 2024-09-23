%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%% Copyright (c) 2023 Workday, Inc.
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
-module(basic_command_line).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->

    %% Deploy a node to test against
    ?LOG_INFO("Deploy node to test command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),

    %% Verify node-up behavior
    ping_up_test(Node),
    attach_direct_up_test(Node),
    status_up_test(Node),
    console_up_test(Node),
    start_up_test(Node),
    getpid_up_test(Node),

    %% Stop the node, Verify node-down behavior
    stop_test(Node),
    ping_down_test(Node),
    attach_down_test(Node),
    status_down_test(Node),
    console_test(Node),
    start_test(Node),
    getpid_down_test(Node),

    pass.

console_up_test(Node) ->
    ?LOG_INFO("Node is already up, `riak console` should fail"),
    {ok, ConsoleFail} = rt:riak(Node, ["console"]),
    ?assert(rt:str(ConsoleFail, "Node is already running!")).

console_test(Node) ->
    %% Make sure the cluster will start up with /sbin/riak console, then quit
    ?LOG_INFO("Testing riak console on ~s", [Node]),

    %% Console prompt changes with OTP version, but rather than check what's
    %% running with rt:otp_release/1 we'll just use a RegEx that'll be easier
    %% to maintain over time.
    Prompt = {re, "abort with \\^G|press Ctrl\\+G to abort"},
    
    %% Start and stop node, to test console working
    Ops = [
        {expect, Prompt, 30000},    % give it some time to start
        {send, "riak_core_ring_manager:get_my_ring().\n"},
        {expect, "dict,", 10000},
        {send, "q().\n"},
        {expect, "ok"}
    ],
    ?assertMatch({ok, 0}, rt:interact(Node, "console", Ops)),
    rt:wait_until_unpingable(Node).

start_up_test(Node) ->
    %% Try starting again and check you get the node is already running message
    ?LOG_INFO("Testing riak start now will return 'already running'"),
    {ok, StartOut} = rt:riak(Node, ["start"]),
    ?assert(rt:str(StartOut, "Node is already running!")).

start_test(Node) ->
    %% Test starting with /bin/riak start
    ?LOG_INFO("Testing riak start works on ~s", [Node]),
    {ok, Output} = rt:riak(Node, ["start"]),
    StartPass = string:trim(Output),
    ?LOG_INFO("StartPass: ~0p", [StartPass]),
    %% Depending on relx version, a variety of output may be printed.
    ?assert(
        StartPass =:= ""
        orelse rt:str(StartPass, "WARNING")
        orelse rt:str(StartPass, " deprecated") ),
    rt:stop_and_wait(Node).

stop_test(Node) ->
    ?LOG_INFO("Testing riak stop works on ~s", [Node]),
    ?assert(rt:is_pingable(Node)),
    {ok, Output} = rt:riak(Node, ["stop"]),
    StopOut = string:trim(Output),  % trailing newline
    ?assertMatch("ok", StopOut),
    ?assertNot(rt:is_pingable(Node)).

ping_up_test(Node) ->
    ?LOG_INFO("Testing riak ping on ~s", [Node]),
    %% ping / pong
    %% rt:start_and_wait(Node),
    ?LOG_INFO("Node up, should ping"),
    {ok, PongOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PongOut, "pong")).

ping_down_test(Node) ->
    %% ping / pang
    ?LOG_INFO("Node down, should pang"),
    {ok, PangOut} = rt:riak(Node, ["ping"]),
    ?LOG_INFO("Pang like dis ~p", [PangOut]),
    not_running(PangOut).

attach_down_test(Node) ->
    ?LOG_INFO("Testing riak 3+ 'attach' while down"),
    {ok, AttachOut} = rt:riak(Node, ["attach"]),
    not_running(AttachOut).

attach_direct_up_test(Node) ->
    ?LOG_INFO("Testing riak 3+ 'attach'"),
    Ops = [
        {expect, "(^D to exit)"},
        {send, "riak_core_ring_manager:get_my_ring().\n"},
        {expect, "dict,", 10000},
        {send, [4]}     %% 4 = Ctrl + D
    ],
    ?assertMatch({ok, 0}, rt:interact(Node, "attach", Ops)).

status_up_test(Node) ->
    ?LOG_INFO("Test riak admin status on ~s", [Node]),
    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    ?LOG_DEBUG("Result of status: ~s", [StatusOut]),
    ?assertEqual(0, ExitCode),
    ?assert(rt:str(StatusOut, "1-minute stats")),
    ?assert(rt:str(StatusOut, "kernel_version")).

status_down_test(Node) ->
    ?LOG_INFO("Test riak admin status on ~s while down", [Node]),
    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    ?assertNotEqual(0, ExitCode),
    not_running(StatusOut).

getpid_up_test(Node) ->
    ?LOG_INFO("Test riak get pid on ~s", [Node]),
    {ok, Output} = rt:riak(Node, ["pid"]),
    PidOut = string:trim(Output),   % trailing newline
    ?assertMatch([_|_], PidOut),
    ?assertEqual(PidOut, rpc:call(Node, os, getpid, [])).

getpid_down_test(Node) ->
    ?LOG_INFO("Test riak getpid fails on ~s", [Node]),
    {ok, Output} = rt:riak(Node, ["pid"]),
    PidOut = string:trim(Output),   % trailing newline
    %% Depending on relx version, a variety of output may be printed.
    ?assert(
        PidOut =:= ""
        orelse rt:str(PidOut, " not responding to ping")
        orelse rt:str(PidOut, " not running") ).

not_running(Output) ->
    %% Depending on relx version, a variety of output may be printed.
    ?assert(
        rt:str(Output, " not responding to ping")
        orelse rt:str(Output, " not running")
    ).