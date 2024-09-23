%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
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
%% @doc Functions for interacting with the application configuration.
%%
%% Configuration is initialized from the `riak_test.app' file on application
%% load, and further updated from the config file and command-line options.
%%
-module(rt_config).

-export([
    config_or_os_env/1, config_or_os_env/2,
    get/1, get/2,
    get_os_env/1, get_os_env/2,
    load/2,
    logger_filters/1,
    logger_formatter/2, logger_formatter/3,
    set/2
]).

-include_lib("kernel/include/logger.hrl").

%% @doc Get the value of an OS Environment variable. The arity 1 version of
%%      this function will fail the test if it is undefined.
get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined ->
            ?LOG_ERROR("ENV['~s'] is not defined", [Var]),
            erlang:error("Missing environment key", [Var]);
        Value -> Value
    end.

%% @doc Get the value of an OS Evironment variable. The arity 2 version of
%%      this function will return the Default if the OS var is undefined.
get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

%% @doc Load the configuration from the specified config file.
load(Config, undefined) ->
    load(Config, filename:join([os:getenv("HOME"), ".riak_test.config"]));
load(undefined, ConfigFile) ->
    load_dot_config("default", ConfigFile);
load(ConfigName, ConfigFile) ->
    load_dot_config(ConfigName, ConfigFile).

%% @private
load_dot_config(ConfigName, ConfigFile) ->
    case file:consult(ConfigFile) of
        {ok, Terms} ->
            %% First, set up the defaults
            case proplists:get_value(default, Terms) of
                undefined -> meh; %% No defaults set, move on.
                Default -> [set(Key, Value) || {Key, Value} <- Default]
            end,
            %% Now, overlay the specific project
            Config = proplists:get_value(list_to_atom(ConfigName), Terms),
            [set(Key, Value) || {Key, Value} <- Config],
            ok;
        {error, Reason} ->
            erlang:error("Failed to parse config file", [ConfigFile, Reason])
 end.

set(Key, Value) ->
    ok = application:set_env(riak_test, Key, Value).

get(Key) ->
    case kvc:path(Key, application:get_all_env(riak_test)) of
        [] ->
            ?LOG_ERROR("Missing configuration key: ~0p", [Key]),
            erlang:error("Missing configuration key", [Key]);
        Value ->
            Value
    end.

get(Key, Default) ->
    case kvc:path(Key, application:get_all_env(riak_test)) of
        [] -> Default;
        Value -> Value
    end.

-spec config_or_os_env(atom()) -> term().
config_or_os_env(Config) ->
    OSEnvVar = to_upper(atom_to_list(Config)),
    case {get_os_env(OSEnvVar, undefined), get(Config, undefined)} of
        {undefined, undefined} ->
            MSG = io_lib:format(
                "Neither riak_test.~0p nor ENV['~0p'] are defined",
                [Config, OSEnvVar]),
            erlang:error(binary_to_list(iolist_to_binary(MSG)));
        {undefined, V} ->
            ?LOG_INFO("Found riak_test.~s: ~s", [Config, V]),
            V;
        {V, _} ->
            ?LOG_INFO("Found ENV[~s]: ~s", [OSEnvVar, V]),
            set(Config, V),
            V
    end.

-spec config_or_os_env(atom(), term()) -> term().
config_or_os_env(Config, Default) ->
    OSEnvVar = to_upper(atom_to_list(Config)),
    case {get_os_env(OSEnvVar, undefined), get(Config, undefined)} of
        {undefined, undefined} -> Default;
        {undefined, V} ->
            ?LOG_INFO("Found riak_test.~s: ~s", [Config, V]),
            V;
        {V, _} ->
            ?LOG_INFO("Found ENV[~s]: ~s", [OSEnvVar, V]),
            set(Config, V),
            V
    end.

-spec logger_formatter(
    Verbose :: boolean(), Newline :: boolean(), SingleLine :: boolean() )
        -> {module(), logger_formatter:config()}.
%% @doc Retrieves a logger formatter with the specified characteristics.
logger_formatter(Verbose, Newline, SingleLine) ->
    case logger_formatter(Verbose, Newline) of
        {_, #{single_line := SingleLine}} = Spec ->
            Spec;
        {FModule, FConfig} ->
            {FModule, FConfig#{single_line => SingleLine}}
    end.

-spec logger_formatter(Verbose :: boolean(), Newline :: boolean() )
        -> {module(), logger_formatter:config()}.
%% @doc Retrieves a logger formatter with the specified characteristics.
logger_formatter(Verbose, Newline) ->
    TemplateKey = case {Verbose, Newline} of
        {false, false} ->
            minimal;
        {false, true} ->
            minimal_nl;
        {true, false} ->
            verbose;
        {true, true} ->
            verbose_nl
    end,
    {ok, Templates} = application:get_env(riak_test, log_templates),
    Template = maps:get(TemplateKey, Templates),
    {ok, {FModule, FConfig}} = application:get_env(riak_test, log_formatter),
    {FModule, FConfig#{template => Template}}.

-spec logger_filters(Selector :: atom() | list(atom()) )
        -> list({logger:filter_id(), logger:filter()}).
%% @doc Returns a list of common logger filter specs.
%%
%% If a list is specified, the returned list of filters is in the order
%% of their Selectors.
logger_filters(default) ->
    %% Returned in reverse order
    logger_filters([sasl, progress, emu_crash], []);
logger_filters(all) ->
    %% Returned in reverse order
    logger_filters([sasl, progress, proclib_crash, emu_crash], []);
logger_filters([]) ->
    [];
logger_filters([_|_] = Selectors) ->
    logger_filters(lists:reverse(Selectors), []);
logger_filters(Selector) ->
    logger_filters([Selector], []).

-spec logger_filters(
    Selectors :: atom() | list(atom()),
    Results :: list({logger:filter_id(), logger:filter()}))
        -> list({logger:filter_id(), logger:filter()}).
%% @hidden
%% Stops crash reports from procs spawned by erlang:spawn...
logger_filters([emu_crash | Selectors], Results) ->
    logger_filters(Selectors, [
        {rt_emu_crash_filter,
            {fun logger_emu_crash_filter/2, ?MODULE}}
        | Results]);
%% Stops crash reports from procs spawned by proc_lib:spawn...
logger_filters([proclib_crash | Selectors], Results) ->
    logger_filters(Selectors, [
        {rt_proclib_crash_filter,
            {fun logger_proclib_crash_filter/2, ?MODULE}}
        | Results]);
%% Stops progress reports from proc_lib apps/sups
logger_filters([progress | Selectors], Results) ->
    logger_filters(Selectors, [
        {rt_progress_filter,
            {fun logger_filters:progress/2, stop}}
        | Results]);
%% Stops everything from SASL
logger_filters([sasl | Selectors], Results) ->
    logger_filters(Selectors, [
        {rt_sasl_filter,
            {fun logger_filters:domain/2, {stop, sub, [otp, sasl]}}}
        | Results]);
%% Ignore bad Selectors - no badarg exceptions!
logger_filters([Selector | Selectors], Results) ->
    ?LOG_WARNING("Ignoring unknown filter selector: ~0p", [Selector]),
    logger_filters(Selectors, Results);
%% Let the caller decide whether to re-order the list
logger_filters([], Results) ->
    Results.

-spec logger_emu_crash_filter(logger:log_event(), logger:filter_arg())
        -> logger:filter_return().
%% @hidden Filters out crash reports.
logger_emu_crash_filter(
        #{meta := #{error_logger := #{emulator := true, tag := error}}}, _) ->
    stop;
logger_emu_crash_filter(_LogEvent, _Param) ->
    ignore.

-spec logger_proclib_crash_filter(logger:log_event(), logger:filter_arg())
        -> logger:filter_return().
%% @hidden Filters out crash reports.
logger_proclib_crash_filter(
        #{msg := {report, #{label := {proc_lib, crash}}}}, _) ->
    stop;
logger_proclib_crash_filter(_LogEvent, _Param) ->
    ignore.

to_upper(S) -> lists:map(fun char_to_upper/1, S).
char_to_upper(C) when C >= $a, C =< $z -> C bxor $\s;
char_to_upper(C) -> C.
