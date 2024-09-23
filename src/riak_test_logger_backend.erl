%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.
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
%% @doc This logger handler keeps a retrievable buffer of logs in memory.
%%
%% You may be tempted to think the gen_server part could be implemented as
%% a facade over `rt_logger', but that would make things ever so much more
%% complicated.
%%
-module(riak_test_logger_backend).
-behavior(gen_server).

%% riak_test API
-export([
    get_logs/0,

    %% application-ish interface
    start/3,
    stop/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

% Logger handler
-export([
    log/2
]).

-include_lib("kernel/include/logger.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(HIDE_FROM_EDOC).
-type state() :: rtt:log_lines().
-endif. % HIDE_FROM_EDOC

-define(SERVER,     ?MODULE).
-define(GS_HANDLER, riak_test_mem_logger).
-define(FS_HANDLER, riak_test_file_logger).

%% ===================================================================
%% riak_test API
%% ===================================================================

-spec get_logs() -> rtt:log_lines() | rtt:std_error().
get_logs() ->
    gen_server:call(?SERVER, get_logs).

-spec start(
    Verbose :: boolean(), Level :: logger:level(), LogFile :: rtt:fs_path())
        -> ok | rtt:std_error().
%% @private
start(Verbose, Level, LogFile) ->
    case gen_server:start({local, ?SERVER}, ?MODULE, [], []) of
        {ok, _Pid} ->
            start_handlers(Verbose, Level, LogFile);
        Error ->
            Error
    end.

-spec stop() -> ok.
%% @private
stop() ->
    _ = stop_handlers(),
    case erlang:whereis(?SERVER) of
        undefined ->
            ok;
        _Pid ->
            gen_server:stop(?SERVER)
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init(State :: []) -> {ok, state()}.
%% @private
init([] = State) ->
    {ok, State}.

-spec handle_call(Msg :: term(), From :: {pid(), term()}, State :: state())
        -> {reply, term(), state()}.
%% @private
handle_call(get_logs, _From, [] = State) ->
    {reply, State, State};
handle_call(get_logs, _From, State) ->
    {reply, lists:reverse(State), State};
handle_call(Msg, From, State) ->
    ?LOG_NOTICE("Ignoring ~0p from ~0p", [Msg, From]),
    {reply, not_implemented, State}.

-spec handle_cast(Msg :: term(), State :: state()) -> {noreply, state()}.
%% @private
handle_cast({log, LogLine}, State) ->
    {noreply, [LogLine | State]};
handle_cast(Msg, State) ->
    ?LOG_NOTICE("Ignoring ~0p", [Msg]),
    {noreply, State}.

%% ===================================================================
%% Logger handler callback
%% ===================================================================

-spec log(Event :: logger:log_event(), Config :: logger:handler_config())
        -> ok.
%% @private
log(Event, #{formatter := {FModule, FConfig}}) ->
    gen_server:cast(?SERVER, {log, FModule:format(Event, FConfig)}).

%% ===================================================================
%% Internal
%% ===================================================================

-spec start_handlers(
    Verbose :: boolean(), Level :: logger:level(), LogFile :: rtt:fs_path())
        -> ok | rtt:std_error().
start_handlers(Verbose, Level, LogFile) ->
    HFilters = rt_config:logger_filters(all),
    FSHConfig = #{
        config => #{type => file, file => LogFile,
            file_check => 100, filesync_repeat_interval => 1000},
        level => Level, filters => HFilters,
        formatter => rt_config:logger_formatter(true, true, false)
    },
    GSHConfig = #{
        config => default, level => Level, filters => HFilters,
        %% Include trailing newlines for when riak_test_runner combines
        %% them into a single binary.
        formatter => rt_config:logger_formatter(Verbose, true, false)
    },
    case filelib:ensure_dir(LogFile) of
        ok ->
            case logger:add_handler(?FS_HANDLER, logger_std_h, FSHConfig) of
                ok ->
                    logger:add_handler(?GS_HANDLER, ?MODULE, GSHConfig);
                HError ->
                    HError
            end;
        {error, DirErr} ->
            {error, {DirErr, LogFile}}
    end.

-spec stop_handlers() -> ok.
stop_handlers() ->
    %% Ignore failures, we're closing it all down anyway, and the result is
    %% deliberately ignored.
    %% If you find yourself debugging, these should all be returning 'ok'.
    logger_std_h:filesync(?FS_HANDLER),
    logger:remove_handler(?FS_HANDLER),
    logger:remove_handler(?GS_HANDLER).


-ifdef(TEST).

log_test() ->
    IMsg = "this is an info message",
    NMsg = "this is a notice message",
    WMsg = "this is a warning message",
    File = filename:join(["/tmp", eunit, ?MODULE, ?FUNCTION_NAME]) ++ ".log",

    %% Disable the default handler so our messages don't clutter the terminal.
    %% We'll re-enable it later with the same configuration.
    {ok, HDefault} = logger:get_handler_config(default),

    application:load(riak_test),
    %% Start ours *before* removing the default, in case something goes wrong.
    ?assertMatch(ok, start(false, notice, File)),
    ?assertMatch(ok, logger:remove_handler(default)),

    % io:format(standard_error,
    %     "~nlogger config:~n~p~n", [logger:get_config()]),

    %% Log some messages - 'info' shouldn't be logged because of start level
    ?LOG_NOTICE(NMsg),
    ?LOG_INFO(IMsg),
    ?LOG_WARNING(WMsg),

    %% Reset the default handler
    ?assertMatch(ok,
        logger:add_handler(default, maps:get(module, HDefault), HDefault)),

    %% Get the logs *before* stopping
    Logs = get_logs(),
    ?assertMatch(ok, stop()),

    ?assert(erlang:is_list(Logs)),
    ?assertEqual(2, erlang:length(Logs)),
    %% Note that these are likely to be deep lists, so flat string search
    %% won't cut it.
    [Log1, Log2] = Logs,

    ReOpts = [{capture, none}],
    ?assertMatch(match, re:run(Log1, NMsg, ReOpts)),
    ?assertMatch(match, re:run(Log2, WMsg, ReOpts)).

-endif. % TEST
