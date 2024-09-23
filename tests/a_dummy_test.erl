%% -------------------------------------------------------------------
%%
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
%%
%% @doc A sandbox for investigating/confirming `riak_test' functionality.
%%
%% Among other uses, this is the only way to check out behaviors that rely
%% on the TestMetaData context.
%%
%% Edit at will, but refrain from committing changes with no ongoing value.
%%
%% This test should probably NEVER be included in any group file!
%% @end
%%
%% The general structure is to use macros to turn test features on and off.
%%
-module(a_dummy_test).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
% -include_lib("stdlib/include/assert.hrl").

%% Macros set to false trigger "never called" warnings
-dialyzer(no_unused).

-define(SHOW_METADATA,      false).
-define(SHOW_REDBUG_DFLT,   false).
-define(DO_REDBUG_ON_OFF,   false).

-spec confirm() -> pass | fail.
confirm() ->
    ?SHOW_METADATA andalso show_metadata(),

    ?SHOW_REDBUG_DFLT andalso show_redbug_defaults(),

    ?DO_REDBUG_ON_OFF andalso do_redbug_on_off(),


    pass.

show_metadata() ->
    show_props("Test Metadata", riak_test_runner:metadata()).

show_redbug_defaults() ->
    show_props("Redbug Default Opts", rt_redbug:default_trace_options()).

do_redbug_on_off() ->
    ?LOG_INFO("Starting redbug state: ~0p", [rt_redbug:is_tracing_applied()]),
    rt_redbug:set_tracing_applied(false),
    ThisNode = erlang:node(),
    RTP = "rt_redbug:is_tracing_applied/0",
    Callback = fun(Msg) -> ?LOG_INFO("redbug callback: ~0p", [Msg]) end,
    Opts = #{arity => true, print_fun => Callback, msgs => 1},
    ?LOG_INFO("start trace when disabled: ~0p", [rt_redbug:trace(ThisNode, RTP, Opts)]),
    ?LOG_INFO("enable tracing: was ~0p", [rt_redbug:set_tracing_applied(true)]),
    ?LOG_INFO("start trace when enabled: ~0p", [rt_redbug:trace(ThisNode, RTP, Opts)]),
    ?LOG_INFO("trigger a trace message: ~0p", [rt_redbug:is_tracing_applied()]),
    ?LOG_INFO("disable tracing: was ~0p", [rt_redbug:set_tracing_applied(false)]),
    ?LOG_INFO("stop tracing when disabled: ~0p", [rt_redbug:stop()]).

%% General purpose proplist/map dumper
show_props(Heading, []) ->
    ?LOG_INFO("~s: <empty>", [Heading]);
show_props(Heading, [_|_] = Props) ->
    Lines = [io_lib:format("\n    ~0p", [E]) || E <- Props],
    ?LOG_INFO("~s:~s", [Heading, Lines]);
show_props(Heading, Map) when erlang:is_map(Map) ->
    show_props(Heading, maps:to_list(Map)).
