%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%% @deprecated Unused anywhere, let's keep it that way.
-module(rt_local).
-deprecated(module).

-export([
         assert_which/1,
         % download/1,
         home_dir/0,
         install_on_absence/2,
         stream_cmd/1,
         stream_cmd/2,
         url_to_filename/1,
         which/1
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Return the home directory of the riak_test script.
-spec home_dir() -> file:filename().
home_dir() ->
    filename:dirname(filename:absname(escript:script_name())).

%% @doc Wrap 'which' to give a good output if something is not installed
which(Command) ->
    ?LOG_INFO("Checking for presence of ~s", [Command]),
    Cmd = lists:flatten(io_lib:format("which ~s; echo $?", [Command])),
    case rt:str(os:cmd(Cmd), "0") of
        false ->
            ?LOG_WARNING("`~s` is not installed", [Command]),
            false;
        true ->
            true
    end.

%% @doc like rt:which, but asserts on failure
assert_which(Command) ->
    ?assert(which(Command)).

% download(Url) ->
%     ?LOG_INFO("Downloading ~s", [Url]),
%     Filename = url_to_filename(Url),
%     case filelib:is_file(filename:join(rt_config:get(rt_scratch_dir), Filename))  of
%         true ->
%             ?LOG_INFO("Got it ~0p", [Filename]),
%             ok;
%         _ ->
%             ?LOG_INFO("Getting it ~0p", [Filename]),
%             rt_local:stream_cmd("curl  -O -L " ++ Url, [{cd, rt_config:get(rt_scratch_dir)}])
%     end.

url_to_filename(Url) ->
    lists:last(string:tokens(Url, "/")).

%% @doc checks if Command is installed and runs InstallCommand if not
%% ex:  rt:install_on_absence("bundler", "gem install bundler --no-rdoc --no-ri"),
install_on_absence(Command, InstallCommand) ->
    case which(Command) of
        false ->
            ?LOG_INFO("Attempting to install `~s` with command `~s`", [Command, InstallCommand]),
            ?assertCmd(InstallCommand);
        _True ->
            ok
    end.

%% @doc pretty much the same as os:cmd/1 but it will stream the output to logger.
%%      If you're running a long running command, it will dump the output
%%      once per second, as to not create the impression that nothing is happening.
-spec stream_cmd(string()) -> {integer(), string()}.
stream_cmd(Cmd) ->
    Port = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
        [stream, stderr_to_stdout, exit_status]),
    stream_cmd_loop(Port, "", "", os:timestamp()).

%% @doc same as rt:stream_cmd/1, but with options, like open_port/2
-spec stream_cmd(string(), string()) -> {integer(), string()}.
stream_cmd(Cmd, Opts) ->
    Port = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
        [stream, stderr_to_stdout, exit_status] ++ Opts),
    stream_cmd_loop(Port, "", "", os:timestamp()).

stream_cmd_loop(Port, Buffer, NewLineBuffer, Time={_MegaSecs, Secs, _MicroSecs}) ->
    receive
        {Port, {data, Data}} ->
            {_, Now, _} = os:timestamp(),
            NewNewLineBuffer = case Now > Secs of
                true ->
                    ?LOG_INFO(NewLineBuffer),
                    "";
                _ ->
                    NewLineBuffer
            end,
            case rt:str(Data, "\n") of
                true ->
                    ?LOG_INFO(NewNewLineBuffer),
                    Tokens = string:tokens(Data, "\n"),
                    [ ?LOG_INFO(Token) || Token <- Tokens ],
                    stream_cmd_loop(Port, Buffer ++ NewNewLineBuffer ++ Data, "", Time);
                _ ->
                    stream_cmd_loop(Port, Buffer, NewNewLineBuffer ++ Data, os:timestamp())
            end;
        {Port, {exit_status, Status}} ->
            catch port_close(Port),
            {Status, Buffer}
    after rt_config:get(rt_max_wait_time) ->
            {-1, Buffer}
    end.
