%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
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
%% @doc Confirm correct logger configuration in test context.
%%
%% There *should* be four logger handlers we've configured while running
%% a test:
%%
%% 1.   The overall console handler, named 'default'
%%      Verbose per config, default multi-line configurable
%%
%% 2.   The overall file handler, named 'rt_file_h'
%%      Always verbose, multi-line
%%      Output file: <outdir>/log/riak_test.log
%%
%% 3.   The per-test file handler, named 'riak_test_file_logger'
%%      Always verbose, multi-line
%%      Output file: <outdir>/log/<test-module>.test.log
%%
%% 4.   The per-test memory handler, named 'riak_test_mem_logger'
%%      Verbose per config, multi-line
%%
-module(confirm_riak_test_logger).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->

    HConfigs = lists:sort(fun handler_compare/2, logger:get_handler_config()),

    print_handlers(HConfigs),
    confirm_handlers(HConfigs),

    pass.

confirm_handlers(HConfigs) ->
    lists:foreach(fun confirm_handler/1, HConfigs).
%%
%% These patterns need to be kept up to date with the riak_test implementation.
%%
confirm_handler(#{id := default, module := HMod,
        formatter := {FMod, FConfig} = Formatter} = _HConfig ) ->
    ?assertMatch(logger_std_h, HMod),
    ?assertMatch(logger_formatter, FMod),
    ?assertMatch(#{single_line := _, template := _}, FConfig),
    #{single_line := SingleLine} = FConfig,
    ?assert(erlang:is_boolean(SingleLine)),
    ?assertEqual(Formatter,
        rt_config:logger_formatter(rt_config:get(verbose), true, SingleLine));

confirm_handler(#{id := rt_file_h,
        module := HMod, formatter := {FMod, _FConfig} = Formatter} ) ->
    ?assertMatch(logger_std_h, HMod),
    ?assertMatch(logger_formatter, FMod),
    ?assertEqual(Formatter, rt_config:logger_formatter(true, true, false));

confirm_handler(#{id := riak_test_file_logger,
        module := HMod, formatter := {FMod, _FConfig} = Formatter} ) ->
    ?assertMatch(logger_std_h, HMod),
    ?assertMatch(logger_formatter, FMod),
    ?assertEqual(Formatter, rt_config:logger_formatter(true, true, false));

confirm_handler(#{id := riak_test_mem_logger,
        module := HMod, formatter := {FMod, _FConfig} = Formatter} ) ->
    ?assertMatch(riak_test_logger_backend, HMod),
    ?assertMatch(logger_formatter, FMod),
    ?assertEqual(Formatter,
        rt_config:logger_formatter(rt_config:get(verbose), true, false));

confirm_handler(#{id := Name, module := HMod} = HConfig ) ->
    ?LOG_ERROR("Unknown logger handler ~0p:~0p~n~p", [Name, HMod, HConfig]),
    erlang:error(unknown_handler, [Name, HMod]).


print_handlers(HConfigs) ->
    io:nl(user),
    lists:foreach(fun print_handler/1, HConfigs).

print_handler(#{id := Name, module := Mod, formatter := {_,
        #{single_line := SingleLine, template := Template}}} = HConfig ) ->
    LFmt = if
        SingleLine ->
            "single";
        true ->
            "multi"
    end,
    Newline = case lists:reverse(Template) of
        ["\n" | _] ->
            "";
        _ ->
            "no-"
    end,
    io:format(user,
        "Handler '~s': ~s, ~s-line, ~strailing-newline~n  ~p.~n~n",
        [Name, Mod, LFmt, Newline, HConfig]).

handler_compare(#{id := default}, _) ->
    true;
handler_compare(_, #{id := default}) ->
    false;
handler_compare(#{id := rt_file_h}, _) ->
    true;
handler_compare(_, #{id := rt_file_h}) ->
    false;
handler_compare(#{id := Left}, #{id := Right}) ->
    Left =< Right.
