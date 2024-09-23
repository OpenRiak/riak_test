%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%% Copyright (c) 2018-2023 Workday, Inc.
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
%%
%% @doc Runs a riak_test module's confirm/0 function.
%%
-module(riak_test_runner).

%% Test metadata accessors
-export([metadata/0, metadata/1]).

%% Internal riak_test API
-export([confirm/4, function_name/1]).

%% Spawned test runner.
-export([return_to_exit/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-type metadata() :: list({atom(), term()}).
-type test_status() :: pass | fail.
-type test_reason() :: undefined | term().

-spec metadata() -> metadata().
%% @doc Fetches test metadata to the spawned test process.
%%
%% If called from any process other than the one invoking the test entry
%% point, the caller will hang forever! This functionality is maintained
%% in support of existing tests, but will be changed in a future revision.
%%
%% To fetch metadata to ANY process, use {@link metadata/1}.
metadata() ->
    riak_test ! metadata,
    receive
        {metadata, TestMeta} -> TestMeta
    end.

-spec metadata(Pid :: pid()) -> metadata().
%% @doc Fetches test metadata to a specified process.
%%
%% To fetch test metadata from any process, invoke```
%%  riak_test_runner:metadata(erlang:self())'''
metadata(Pid) ->
    riak_test ! {metadata, Pid},
    receive
        {metadata, TestMeta} -> TestMeta
    end.

-spec confirm(
    TestName :: atom(),
    Outdir :: rtt:fs_path(),
    TestMetaData :: metadata(),
    HarnessArgs :: list(string()) )
        -> list({atom(), term()}).
%% @private
%% Runs a test's entry function after setting up a per-test log file and a
%% log capturing backend.
%%
%% After the test runs the log handlers are removed and the captured logs are
%% included in the returned result.
%%
confirm(TestName, Outdir, TestMetaData, HarnessArgs) ->
    {Module, Function} = function_name(TestName),
    start_loggers(Module, Outdir),
    rt:setup_harness(Module, HarnessArgs),
    BackendExtras = case proplists:get_value(multi_config, TestMetaData) of
        undefined -> [];
        Value -> [{multi_config, Value}]
    end,
    Backend = rt:set_backend(
        proplists:get_value(backend, TestMetaData), BackendExtras),
    {ElapsedMS, Status, Reason} = case check_prereqs(Module) of
        true ->
            execute(TestName, Module, Function, TestMetaData);
        not_present ->
            {1, fail, test_does_not_exist};
        _ ->
            {1, fail, all_prereqs_not_present}
    end,
    Result = case Status of
        fail ->
            case Reason of
                test_does_not_exist ->
                    Reason;
                all_prereqs_not_present ->
                    Reason;
                _ ->
                    Status
            end;
        _ ->
            Status
    end,
    ?LOG_NOTICE("~s Test Run Complete ~0p", [TestName, Result]),
    Logs = stop_loggers(),

    RetList = [
        {test, TestName},
        {status, Status},
        {result, Result},
        {log, unicode:characters_to_binary(Logs)},
        {backend, Backend},
        {elapsed_ms, ElapsedMS}
        | proplists:delete(backend, TestMetaData)
    ],
    case Status of
        fail ->
            [{reason, erlang:iolist_to_binary(
                io_lib:format("~p", [Reason]))} | RetList];
        _ ->
            RetList
    end.

%% @hidden
start_loggers(TestName, Outdir) ->
    ?assertMatch(ok, rt_logger:start()),
    Verbose = rt_config:get(verbose, false),
    LogLevel = rt_config:get(log_level, info),
    LogFile = filename:join([Outdir, log, TestName]) ++ ".test.log",
    ?assertMatch(ok,
        riak_test_logger_backend:start(Verbose, LogLevel, LogFile)).

%% @hidden
stop_loggers() ->
    %% Get the logs first, don't know who may be holding them.
    Logs = riak_test_logger_backend:get_logs(),
    rt_logger:stop(),
    riak_test_logger_backend:stop(),
    Logs.

-spec execute(
    TestName :: atom(),
    Module :: module(),
    Function :: atom(),
    TestMetaData :: metadata() )
        -> {ElapsedMilliSecs :: rtt:millisecs(),
            Status :: test_status(), Reason :: test_reason()}.
%% @hidden
%% Does some group_leader swapping, in the style of EUnit.
execute(TestName, Module, Function, TestMetaData) ->
    ?LOG_NOTICE("Running Test ~s", [TestName]),

    %% Take a snapshot of logger's primary config to restore after the test,
    %% because some tests might change it. Hopefully they won't mess with
    %% the individual handlers, which would be far more hassle to deal with.
    PrimLogConfig = logger:get_primary_config(),
    ThisPid = erlang:self(),

    OldGroupLeader = erlang:group_leader(),
    NewGroupLeader = riak_test_group_leader:new_group_leader(ThisPid),
    erlang:group_leader(NewGroupLeader, ThisPid),

    {0, UName} = rt:cmd("uname", ["-a"]),
    ?LOG_INFO("Test Runner `uname -a` : ~s", [string:trim(UName)]),
    Timeout = rt_config:get(test_timeout, undefined),

    TrapExit = erlang:process_flag(trap_exit, true),
    Start = erlang:monotonic_time(),
    Pid = proc_lib:spawn_link(?MODULE, return_to_exit, [Module, Function, []]),
    Timer = case erlang:is_integer(Timeout) of
        true ->
            erlang:send_after(Timeout, ThisPid, test_took_too_long);
        _ ->
            undefined
    end,
    {Status, Reason} = rec_loop(Pid, TestMetaData),
    Timer =:= undefined orelse
        erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    Finish = erlang:monotonic_time(),
    TrapExit orelse erlang:process_flag(trap_exit, TrapExit),

    riak_test_group_leader:tidy_up(OldGroupLeader),
    ElapsedMS = erlang:convert_time_unit(
        (Finish - Start), native, millisecond),

    %% Reset logger's primary config before logging anything else.
    logger:set_primary_config(PrimLogConfig),

    case Status of
        fail ->
            HeadChars = "==================",
            ErrorHeader = lists:flatten([
                HeadChars, $\s, erlang:atom_to_list(TestName),
                " failure reason ", HeadChars
            ]),
            ErrorFooter = lists:duplicate(erlang:length(ErrorHeader), $=),
            ?LOG_ERROR("~n~s~n~p~n~s", [ErrorHeader, Reason, ErrorFooter]);
        _ ->
            ok
    end,
    {ElapsedMS, Status, Reason}.

-spec function_name(TestName :: atom()) -> {module(), atom()}.
%% @private
%% Given a `TestName' atom that is either `module' or `module:function' returns
%% the distinct `Module' and `Function' atoms of the test's entry point.
%%
%% If only a module name is supplied, the default function name `confirm' is
%% returned.
%%
%% The function `Module:Function/0` must be exported.
function_name(TestName) ->
    TMString = erlang:atom_to_list(TestName),
    Tokz = string:tokens(TMString, ":"),
    case erlang:length(Tokz) of
        1 ->
            {TestName, confirm};
        2 ->
            [Module, Function] = Tokz,
            {erlang:list_to_atom(Module), erlang:list_to_atom(Function)}
    end.

%% @hidden
rec_loop(Pid, TestMetaData) ->
    receive
        test_took_too_long ->
            erlang:exit(Pid, kill),
            {fail, test_timed_out};
        metadata ->
            Pid ! {metadata, TestMetaData},
            rec_loop(Pid, TestMetaData);
        {metadata, P} ->
            P ! {metadata, TestMetaData},
            rec_loop(Pid, TestMetaData);
        {'EXIT', Pid, normal} ->
            {pass, undefined};
        {'EXIT', Pid, Error} ->
            {fail, Error}
    end.

%% @hidden
%% A return of `fail' must be converted to a non normal exit since
%% status is determined by `rec_loop'.
%%
%% @see rec_loop/3
-spec return_to_exit(
    Module :: module(), Function :: atom(), Args :: list()) -> no_return().
return_to_exit(Module, Function, Args) ->
    case erlang:apply(Module, Function, Args) of
        pass ->
            erlang:exit(normal);
        fail ->
            erlang:exit(fail)
    end.

-spec check_prereqs(Module :: module()) -> boolean() | not_present.
%% @hidden
check_prereqs(Module) ->
    try Module:module_info(attributes) of
        Attrs ->
            Prereqs = proplists:get_all_values(prereq, Attrs),
            P2 = [ {Prereq, rt_local:which(Prereq)} || Prereq <- Prereqs],
            ?LOG_INFO("~s prereqs: ~0p", [Module, P2]),
            [?LOG_WARNING("~s prereq '~s' not installed.", [Module, P])
                || {P, false} <- P2],
            lists:all(fun({_, Present}) -> Present end, P2)
    catch
        _DontCare:_Really ->
            not_present
    end.
