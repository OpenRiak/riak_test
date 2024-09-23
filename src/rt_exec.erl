%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
%% Copyright (c) 2020-2023 Workday, Inc.
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
%% @doc Heavily instrumented functions for executing external programs.
%%
%% This module provides functions allowing for highly configurable and
%% observable external program execution.
%%
%% @end
%%
-module(rt_exec).

-compile([
    %% There are some deliberately small, inline-able functions for clarity.
    inline,
    %% Sigh! Lager's parse-transform is incompatible with 'no_auto_import'.
    %% As of OTP 22, "kernel/include/logger.hrl" is incompatible as well.
    % no_auto_import,
    warn_export_vars,
    warn_unused_import,
    warnings_as_errors
]).

% Public API
-export([
    cmd/2, cmd/3, cmd/5, cmd/6,
    cmd_line/1,
    default_linebuf/0, default_linebuf/1,
    interact/3, interact/4, interact/6,
    resolve_cmd/1, resolve_cmd/2,
    resolve_dir/1,
    resolve_file/1,
    spawn_cmd/2, spawn_cmd/4,
    spawned_result/1, spawned_result/2, spawned_result/3
]).

% Public Types
-export_type([
    buf_sz/0,
    cmd_token/0
]).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Some timeouts so we don't need to rely on 'rt_config' in eunit.
-define(TEST_EXEC_TIMEOUT,  25000).
-define(TEST_CONF_TIMEOUT,  (?TEST_EXEC_TIMEOUT + 5000)).
-endif. % TEST

%% `exe' and `args' are included for error reporting
-record(rt_exec_token, {
    port :: port(),
    exe :: rtt:cmd_exe(),
    args :: rtt:cmd_args()
}).

-type buf_sz() :: pos_integer().
-opaque cmd_token() :: #rt_exec_token{}.

-define(DEFAULT_PORT_LINE_BUFFER,   1024).
-define(DEFAULT_EXPECT_TIMEOUT,     5000).
-define(DEFAULT_INTERACTIVE_PAUSE,  1000).
-define(EOF_WAIT_FOR_EXIT_STATUS,   1000).

%% The highest legal value for an 'after' clause.
-define(WAIT_FOREVER_MS, ((1 bsl 32) - 1)).

%% Common message body indicating outer-level timeout.
-define(COMMAND_TIMEOUT_MSG, {timeout, ?MODULE}).

%% Keys MUST be atoms.
-define(LINEBUF_KEY, rt_exec_linebuf).

%% Use ONLY in ?LOG_xxx arg list - return the Head Of List or [] if empty.
-define(LOG_HOL(L), case L of [H|_] -> H; _ -> [] end).

%% Simplified guards
-define(is_compiled_regex(T), (
    erlang:is_tuple(T) andalso erlang:size(T) > 1
        andalso erlang:element(1, T) =:= re_pattern) ).
-define(is_nonempty_binary(T), (
    erlang:is_binary(T) andalso erlang:size(T) > 0) ).
-define(is_pos_integer(T), (erlang:is_integer(T) andalso T > 0) ).
-define(is_ms_timeout(T), ?is_pos_integer(T)).

%% ===================================================================
%% Public API
%% ===================================================================

-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args() )
        -> {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_error().
%%
%% @doc Executes `Cmd' with the default timeout.
%%
%% @equiv cmd(Cmd, cwd, Args, [], string, default_timeout())
%%
%% @see cmd/6
%%
cmd(Cmd, Args) ->
    ?LOG_DEBUG("C: ~0p A: ~0p", [Cmd, Args]),
    case cmd(Cmd, cwd, Args, [], string) of
        {RC, string, Output} ->
            {RC, Output};
        Error ->
            Error
    end.

-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Timeout :: rtt:millisecs() )
        -> {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_error().
%%
%% @doc Executes `Cmd' with a specified timeout.
%%
%% @equiv cmd(Cmd, cwd, Args, [], string, Timeout)
%%
%% @see cmd/6
%%
cmd(Cmd, Args, Timeout) ->
    ?LOG_DEBUG("C: ~0p A: ~0p T: ~0p", [Cmd, Args, Timeout]),
    case cmd(Cmd, cwd, Args, [], string, Timeout) of
        {RC, string, Output} ->
            {RC, Output};
        Error ->
            Error
    end.

-ifdef(COVARIANT_SPECS).
-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var() ),
    Form :: string )
        -> {rtt:cmd_rc(), string, rtt:output_line()} | rtt:cmd_error();
    (
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Form :: rlines | lines )
        -> {rtt:cmd_rc(), rlines | lines, rtt:output_lines()} | rtt:cmd_error().
-else.  % EDoc doesn't grok covariant specs
-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Form :: rtt:cmd_out_format() )
        -> rtt:cmd_result() | rtt:cmd_error().
-endif. % COVARIANT_SPECS
%%
%% @doc Executes `Cmd' with a specified output format.
%%
%% @equiv cmd(Cmd, Dir, Args, Env, Form, default_timeout())
%%
%% @see cmd/6
%%
cmd(Cmd, Dir, Args, Env, Form) ->
    ?LOG_DEBUG("C: ~0p D: ~0p A: ~0p E: ~0p F: ~0p",
        [Cmd, Dir, Args, Env, Form]),
    cmd(Cmd, Dir, Args, Env, Form, default_timeout()).

-ifdef(COVARIANT_SPECS).
-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Form :: string,
    Timeout :: rtt:millisecs() )
        -> {rtt:cmd_rc(), string, rtt:output_line()} | rtt:cmd_error();
    (
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Form :: rlines | lines,
    Timeout :: rtt:millisecs() )
        -> {rtt:cmd_rc(), rlines | lines, rtt:output_lines()} | rtt:cmd_error().
-else.  % EDoc doesn't grok covariant specs
-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Form :: rtt:cmd_out_format(),
    Timeout :: rtt:millisecs() )
        -> rtt:cmd_result() | rtt:cmd_error().
-endif. % COVARIANT_SPECS
%%
%% @doc Executes a command with a specified output format and timeout.
%%
%% Unlike the underlying port invocation, the command and all of its arguments
%% must be strings - atoms and binaries will fail verification and result in
%% a `badarg' exception.
%%
%% Logically equivalent to<br/>
%%  `spawned_result(spawn_cmd(Cmd, Dir, Args, Env), Form, default_timeout())'
%% <br/>while returning all possible errors from either function.
%%
%% Note that `Cmd' <i>MUST</i> be an executable file, not a shell builtin (see
%% `Cmd' to execute a builtin). To execute a commandline containing pipes,
%% wildcards, or logical operators, use `os:cmd/1' or `os:cmd/2'.
%%
%% @param Cmd
%%      The absolute or relative (to `Dir') path to the command to execute, or
%%      an unqualified command name to be located on the `$PATH'.<br/>
%%      To execute a shell builtin, pass `"/bin/sh"' (or whatever shell you
%%      want) in `Cmd' and the name of the builtin as the first element of
%%      `Args', with the remaining elements of `Args' set to the builtin's
%%      parameters.
%%
%% @param Dir
%%      The absolute or relative (to the current directory) path to the
%%      directory from which the command should be run.<br/>
%%      If the atom `cwd' or `pwd' is specified, the command runs from the
%%      current directory.
%%
%% @param Args
%%      The individual command-line parameters to the passed to the command.
%%      <br/>Wildcard expansion is NOT performed, use `filelib:wildcard/1'
%%      while constructing the parameter list if needed.
%%
%% @param Env
%%      The environment of the started process is extended using the provided
%%      specifications. Refer to `erlang:open_port/2' for details.
%%
%% @param Form
%%      Specifies the format of the output.<br/>
%%      Note that all command output to `stdout' and `stderr' is included, so
%%      the two streams' lines may be intermingled.<br/>
%%      If you don't care about program output and will only look at the return
%%      code, the `rlines' format is the most efficient.<br/>
%%      In any output form, `$\r,$\n' sequences do not receive special
%%      handling, but <i>should</i> be converted to `$\n' by the intervening IO
%%      layers.<dl>
%%      <dt>`rlines'</dt><dd>
%%          Output is returned as a list of lines in reverse output order,
%%          including blank lines, without their terminating newline.<br/>
%%          This is the order in which the lines are accumulated, and as such
%%          is most efficient.</dd>
%%      <dt>`lines'</dt><dd>
%%          Output is returned as a list of lines in the order that the
%%          command output them, including blank lines, without their
%%          terminating newline.</dd>
%%      <dt>`string'</dt><dd>
%%          Output is returned in a single string, possibly with embedded
%%          newlines. This output format can be convenient when the output is
%%          to be used in regular expression matching/filtering.<br/>
%%          Successive newlines (blank lines) are <i>NOT</i> stripped nor, by
%%          extension, are trailing newlines.</dd>
%%      </dl>
%%
%% @param Timeout
%%      Specifies how long to wait for execution to complete.
%%
%% @returns <dl>
%%      <dt>`{ReturnCode, Format, Output}'</dt><dd>
%%          The command completed within the allotted time, returning
%%          `ReturnCode', with `Output' formatted as specified by the `Form'
%%          parameter.<br/>
%%          If the `Cmd' program closed its `stdout' before exiting, or any
%%          other condition prevents retrieving the command's result code,
%%          `ReturnCode' will be set to `-1'.</dd>
%%      <dt>`{error, {timeout, {[Exe | Args], OutLines}}}'</dt><dd>
%%          The command did not complete within the specified `Timeout'.<br/>
%%          `Exe' is the absolute path to the executable file, `Args' is the
%%          supplied parameters, and `OutLines' is a list of whatever output
%%          lines were collected from the command (in the order received)
%%          before it timed out.</dd>
%%      <dt>`{error, {Reason, Path}}'</dt><dd>
%%          The command cannot be located or is not executable by the current
%%          user, or a nonexistent or inaccessible directory is specified.<br/>
%%          `Reason' is the Posix error constant representing the error and
%%          `Path' is the specified `Cmd' or `Dir' to which the error applies.
%%      </dd></dl>
%%
%% @see erlang:open_port/2
%% @see os:cmd/1
%%
cmd(Cmd, Dir, Args, Env, Form, Timeout) when ?is_ms_timeout(Timeout) ->
    ?LOG_DEBUG(
        "C: ~0p D: ~0p A: ~0p E: ~0p F: ~0p T: ~0p",
        [Cmd, Dir, Args, Env, Form, Timeout]),
    Opts0 = opt_args(Args, opt_env(Env, opt_form(Form, #{}))),
    case opt_dir(Dir, Opts0) of
        {error, _} = DirError ->
            ?LOG_ERROR("~0p", [DirError]),
            DirError;
        OptsMap ->
            case port_exe(Cmd, OptsMap) of
                {error, _} = CmdError ->
                    ?LOG_ERROR("~0p", [CmdError]),
                    CmdError;
                {_, Exe} = PortCmd ->
                    PortOpts = [in, eof, hide, exit_status,
                        stderr_to_stdout | maps:to_list(OptsMap)],
                    ?LOG_DEBUG("running ~s with ~0p", [Exe, PortOpts]),
                    try
                        Port = erlang:open_port(PortCmd, PortOpts),
                        Result = collect_result(Port, Form, Timeout),
                        ?LOG_DEBUG("~s result: ~0p", [Exe, Result]),
                        case Result of
                            {timeout, Lines} ->
                                {error, {timeout, {[Exe | Args], Lines}}};
                            _ ->
                                Result
                        end
                    catch
                        error:eacces ->
                            {error, {eacces, Exe}}
                    end
            end
    end;
cmd(Cmd, Dir, Args, Env, Form, Timeout) ->
    erlang:error(badarg, [Cmd, Dir, Args, Env, Form, Timeout]).

-spec cmd_line(CmdLineElems :: rtt:cmd_list()) -> rtt:cmdline().
%%
%% @doc Constructs a single command-line string suitable for printing/logging
%%      from the specified command and parameters.
%%
%% The returned string is <i>NOT</i> necessarily suitable for execution
%% (though it probably is).
%%
cmd_line([_Exe | _Params] = CmdLineElems) ->
    lists:join([$\s], lists:map(fun cmd_line_elem/1, CmdLineElems)).

-spec default_linebuf() -> buf_sz().
%%
%% @doc Retrieves the default line-buffer size.
%%
default_linebuf() ->
    rt_config:get(?LINEBUF_KEY, ?DEFAULT_PORT_LINE_BUFFER).

-spec default_linebuf(Size :: buf_sz()) -> ok.
%%
%% @doc Sets the default line-buffer size.
%%
%% Unless you know that you'll be running commands that output very long
%% lines, there's very little reason to ever change this.
%%
default_linebuf(Size) when ?is_pos_integer(Size) ->
    rt_config:set(?LINEBUF_KEY, Size);
default_linebuf(Size) ->
    erlang:error(badarg, [linebuf, Size]).

-spec interact(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Ops :: rtt:interact_list() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().
%%
%% @doc Executes `Cmd' with a sequence of inputs and expected results using
%%      the default timeout.
%%
%% @equiv interact(Cmd, cwd, Args, [], Ops, default_timeout())
%%
%% @see cmd/6
%%
interact(Cmd, Args, Ops) ->
    ?LOG_DEBUG("C: ~0p A: ~0p O: ~0p", [Cmd, Args, Ops]),
    interact(Cmd, cwd, Args, [], Ops, default_timeout()).

-spec interact(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Ops :: rtt:interact_list(),
    Timeout :: rtt:millisecs() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().
%%
%% @doc Executes `Cmd' with a sequence of inputs and expected results.
%%
%% @equiv interact(Cmd, cwd, Args, [], Ops, Timeout)
%%
%% @see cmd/6
%%
interact(Cmd, Args, Ops, Timeout) ->
    ?LOG_DEBUG("C: ~0p A: ~0p O: ~0p T: ~0p", [Cmd, Args, Ops, Timeout]),
    interact(Cmd, cwd, Args, [], Ops, Timeout).

-spec interact(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: list(rtt:env_var()),
    Ops :: rtt:interact_list(),
    Timeout :: rtt:millisecs() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().
%%
%% @doc Executes a command with a sequence of inputs and expected results.
%%
%% Unlike the underlying port invocation, the command and all of its arguments
%% must be strings - atoms and binaries will fail verification and result in
%% a `badarg' exception.
%%
%% Note that `Cmd' <i>MUST</i> be an executable file, not a shell builtin (see
%% `Cmd' to execute a builtin).<br/>
%% Command lines containing pipes, wildcards, or logical operators cannot be
%% executed interactively.
%%
%% @param Cmd
%%      The absolute or relative (to `Dir') path to the command to execute, or
%%      an unqualified command name to be located on the `$PATH'.<br/>
%%      To execute a shell builtin, pass `"/bin/sh"' (or whatever shell you
%%      want) in `Cmd' and the name of the builtin as the first element of
%%      `Args', with the remaining elements of `Args' set to the builtin's
%%      parameters.
%%
%% @param Dir
%%      The absolute or relative (to the current directory) path to the
%%      directory from which the command should be run.<br/>
%%      If the atom `cwd' or `pwd' is specified, the command runs from the
%%      current directory.
%%
%% @param Args
%%      The individual command-line parameters to the passed to the command.
%%      <br/>Wildcard expansion is NOT performed, use `filelib:wildcard/1'
%%      while constructing the parameter list if needed.
%%
%% @param Env
%%      The environment of the started process is extended using the provided
%%      specifications. Refer to `erlang:open_port/2' for details.
%%
%% @param Ops
%%      The sequence of operations to be performed.
%%      Refer to {@link rtt:interact_list()} for details.
%%
%% @param Timeout
%%      Specifies how long to wait overall for execution to complete.<br/>
%%      This parameter takes precedence over the sum of timeouts provided in
%%      the operations list ... sort of.
%%
%% @returns <dl>
%%      <dt>`{ok, ReturnCode}'</dt><dd>
%%          The command completed all specified operations within the allotted
%%          time and exited of its own volition, returning `ReturnCode'.<br/>
%%          If the `Cmd' program closed its `stdout' before exiting, or any
%%          other condition prevents retrieving the command's result code,
%%          `ReturnCode' will be set to `-1'.</dd>
%%      <dt>`{exit, ReturnCode}'</dt><dd>
%%          The command exited of its own volition before all specified
%%          operations were completed, returning `ReturnCode'.<br/>
%%          If the `Cmd' program closed its `stdout' before exiting, or any
%%          other condition prevents retrieving the command's result code,
%%          `ReturnCode' will be set to `-1'.</dd>
%%      <dt>`{timeout, Expected}'</dt><dd>
%%          An operation did not complete within its specified or default
%%          timeout.<br/>
%%          `Expected' is the predicate upon which the function was waiting
%%          when the timeout expired.</dd>
%%      <dt>`{timeout, Exe}'</dt><dd>
%%          The command did not complete within the specified `Timeout'.
%%          <br/>`Exe' is the executable's absolute path.</dd>
%%      <dt>`{error, {Reason, Path}}'</dt><dd>
%%          The command cannot be located or is not executable by the current
%%          user, or a nonexistent or inaccessible directory is specified.<br/>
%%          `Reason' is the Posix error constant representing the error and
%%          `Path' is the specified `Cmd' or `Dir' to which the error applies.
%%      </dd></dl>
%%
interact(Cmd, Dir, Args, Env, Ops, Timeout) when ?is_ms_timeout(Timeout) ->
    ?LOG_DEBUG(
        "C: ~0p D: ~0p A: ~0p E: ~0p O: ~0p T: ~0p",
        [Cmd, Dir, Args, Env, Ops, Timeout]),
    Opts = opt_args(Args, opt_env(Env, #{})),
    case opt_dir(Dir, Opts) of
        {error, _} = DirError ->
            ?LOG_ERROR("~0p", [DirError]),
            DirError;
        OptsMap ->
            case port_exe(Cmd, OptsMap) of
                {error, _} = CmdError ->
                    ?LOG_ERROR("~0p", [CmdError]),
                    CmdError;
                {_, Exe} = PortCmd ->
                    %% Precompile the operations, maybe insert/remove/modify
                    %% ops, and throw a badarg exception on pattern violations
                    OpList = validate_interact_ops(Ops),
                    %% We use the 'eof' option so that we can simulate the
                    %% 'exit_status' if the executable closes its standard
                    %% output stream. This also means the port won't be closed
                    %% automatically, so the erlang:port_command/2 function
                    %% won't throw a 'badarg' when sending data after the
                    %% program exits, and will instead crash the port with a
                    %% pipe error. Trapping that 'EXIT' message allows us to
                    %% determine whether the send operation was really
                    %% successful. Is there a better way?
                    TrapExit = erlang:process_flag(trap_exit, true),
                    PortOpts = [
                        binary, eof, exit_status, hide,
                        {line, default_linebuf()}, stderr_to_stdout,
                        use_stdio | maps:to_list(OptsMap)],
                    ?LOG_DEBUG("running ~s with ~0p", [Exe, PortOpts]),
                    try
                        Port = erlang:open_port(PortCmd, PortOpts),
                        Result = case interact_run(Port, OpList, Timeout) of
                            ?COMMAND_TIMEOUT_MSG ->
                                {timeout, Exe};
                            Return ->
                                Return
                        end,
                        ?LOG_DEBUG("~s result: ~0p", [Exe, Result]),
                        Result
                    catch
                        error:eacces ->
                            {error, {eacces, Exe}}
                    after
                        %% Reset the 'trap_exit' flag to its starting value.
                        TrapExit orelse erlang:process_flag(trap_exit, TrapExit)
                    end
            end
    end;
interact(Cmd, Dir, Args, Env, Ops, Timeout) ->
    erlang:error(badarg, [Cmd, Dir, Args, Env, Ops, Timeout]).

-spec resolve_cmd(Cmd :: rtt:cmd_exe())
        -> rtt:cmd_exe() | {error, file:posix()}.
%%
%% @doc Resolves the specified `Cmd' to the absolute path of the executable
%%      file.
%%
%% Similar to `os:find_executable/1', but with some notable differences.
%%
%% Behaviors:
%%
%%  If `Cmd' is an absolute path, it is evaluated for executability only, no
%%  path search is performed.
%%
%%  If `Cmd' starts with `"./"' or `"../"' it is converted to an absolute path
%%  and evaluated as above.
%%
%%  If `Cmd' contains at least one `/' then the CWD is added at the head of the
%%  `$PATH' before the search is performed.
%%
%%  If the search locates an executable file (or symlink pointing to one)
%%  matching the specified name (on the `$PATH', if applicable) then the
%%  absolute path to the file is returned.
%%
%% @returns
%%      The absolute path of the specified executable file, or an error.<dl>
%%      <dt>`{error, enoent}'</dt><dd>
%%          No filesystem element matches the specified name.</dd>
%%      <dt>`{error, eacces}'</dt><dd>
%%          A filesystem element matching the specified name was found, but
%%          permissions on the file itself or somewhere in its path do not
%%          allow it to be executed by the current user.</dd>
%%      <dt>`{error, eisdir}'</dt><dd>
%%          No executable file matches the specified name, but a directory
%%          matching the name was found.<br/>
%%          <i>It's questionable whether this result has any value.</i></dd>
%%      <dt>`{error, eftype}'</dt><dd>
%%          A filesystem element matching the specified name was found, but it
%%          is not a file or symlink to one.</dd>
%%      </dl>Other Posix errors are possible, though unlikely.
%%
resolve_cmd([$/ | _] = AbsCmd) ->
    resolve_exe_detail(AbsCmd);
resolve_cmd(Cmd) when erlang:is_list(Cmd) ->
    case resolve_file(Cmd) of
        [$/ | _] = AbsCmd ->
            resolve_exe_detail(AbsCmd);
        RelCmd ->
            Path = string:lexemes(os:getenv("PATH", ""), ":"),
            case lists:member($/, RelCmd) of
                true ->
                    resolve_executable([resolve_cwd() | Path], RelCmd);
                _ ->
                    resolve_executable(Path, RelCmd)
            end
    end;
resolve_cmd(Cmd) ->
    erlang:error(badarg, [Cmd]).

-spec resolve_cmd(
    Cmd :: rtt:cmd_exe(),
    Path :: rtt:fs_path() | list(rtt:fs_path()) )
        -> rtt:cmd_exe() | {error, file:posix()}.
%%
%% @doc Resolves the specified `Cmd' to the absolute path of the executable file.
%%
%% Behaves as {@link resolve_cmd/1} except:
%%
%%  `Path' is searched instead of the `$PATH' environment variable.
%%
%%  The CWD is <i>NOT</i> prepended to `Path' for a relative `Cmd'
%%  containing `/'.
%%
resolve_cmd([$/ | _] = AbsCmd, _Path) ->
    resolve_exe_detail(AbsCmd);
resolve_cmd([_|_] = Cmd, [Dir | _] = Path) when erlang:is_list(Dir) ->
    case resolve_file(Cmd) of
        [$/ | _] = AbsCmd ->
            resolve_exe_detail(AbsCmd);
        RelCmd ->
            resolve_executable(Path, RelCmd)
    end;
resolve_cmd([_|_] = Cmd, []) ->
    case resolve_file(Cmd) of
        [$/ | _] = AbsCmd ->
            resolve_exe_detail(AbsCmd);
        _RelCmd ->
            %% No Path, can't be resolved!
            {error, enoent}
    end;
resolve_cmd([_|_] = Cmd, Dir) when erlang:is_list(Dir) ->
    resolve_cmd(Cmd, [Dir]);
resolve_cmd(Cmd, Path) ->
    erlang:error(badarg, [Cmd, Path]).

-spec resolve_dir(Dir :: rtt:fs_path()) -> rtt:fs_path().
%%
%% @doc Resolves the specified directory to a 'clean' absolute path.
%%
%% The resolved directory does not necessarily exist.
%%
%% If the directory contains a leading `"./"' or `"../"' sequence, it is made
%% absolute relative to the current working directory, otherwise it is made
%% absolute as if by `filename:absname/1'.
%%
resolve_dir([$/ | _] = Dir) ->
    Dir;
resolve_dir(".") ->
    resolve_cwd();
resolve_dir("..") ->
    filename:dirname(resolve_cwd());
resolve_dir("./" ++ Dir) ->
    filename:join(resolve_cwd(), string:trim(Dir, leading, "/"));
resolve_dir("../" ++ Dir) ->
    filename:join(resolve_dir(".."), string:trim(Dir, leading, "/"));
resolve_dir(Dir) ->
    filename:absname(Dir).

-spec resolve_file(File :: rtt:fs_path()) -> rtt:fs_path().
%%
%% @doc Resolves the specified file to a 'clean' absolute or relative path.
%%
%% The resolved file does not necessarily exist.
%%
%% If the filename contains a leading `"./"' or `"../"' sequence, it is made
%% absolute relative to the current working directory, otherwise it is
%% returned unchanged.
%%
resolve_file("./" ++ File) ->
    filename:join(resolve_cwd(), string:trim(File, leading, "/"));
resolve_file("../" ++ File) ->
    filename:join(resolve_dir(".."), string:trim(File, leading, "/"));
resolve_file(File) ->
    File.

-spec spawn_cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args() )
        -> cmd_token() | rtt:cmd_exec_err().
%%
%% @doc Spawns the specified command and returns a token for result retrieval.
%%
%% @equiv spawn_cmd(Cmd, cwd, Args, [])
%%
%% @see spawn_cmd/4
%%
spawn_cmd(Cmd, Args) ->
    ?LOG_DEBUG("C: ~0p A: ~0p", [Cmd, Args]),
    spawn_cmd(Cmd, cwd, Args, []).

-spec spawn_cmd(
    Cmd :: rtt:cmd_exe(),
    Dir :: rtt:cmd_dir(),
    Args :: rtt:cmd_args(),
    Env :: rtt:env_vars() )
        -> cmd_token() | rtt:cmd_exec_err().
%%
%% @doc Spawns the specified command and returns a token for result retrieval.
%%
%% Unlike the underlying port invocation, the command and all of its arguments
%% must be strings - atoms and binaries will fail verification and result in
%% a `badarg' exception.
%%
%% Note that `Cmd' <i>MUST</i> be an executable file, not a shell builtin (see
%% `Cmd' to execute a builtin).<br/>
%% Command lines containing pipes, wildcards, or logical operators cannot be
%% executed asynchronously - use `os:cmd/1' or `os:cmd/2'.
%%
%% @param Cmd
%%      The absolute or relative (to `Dir') path to the command to execute, or
%%      an unqualified command name to be located on the `$PATH'.<br/>
%%      To execute a shell builtin, pass `"/bin/sh"' (or whatever shell you
%%      want) in `Cmd' and the name of the builtin as the first element of
%%      `Args', with the remaining elements of `Args' set to the builtin's
%%      parameters.
%%
%% @param Dir
%%      The absolute or relative (to the current directory) path to the
%%      directory from which the command should be run.<br/>
%%      If the atom `cwd' or `pwd' is specified, the command runs from the
%%      current directory.
%%
%% @param Args
%%      The individual command-line parameters to the passed to the command.
%%      <br/>Wildcard expansion is NOT performed, use `filelib:wildcard/1'
%%      while constructing the parameter list if needed.
%%
%% @param Env
%%      The environment of the started process is extended using the provided
%%      specifications. Refer to `erlang:open_port/2' for details.
%%
%% @returns<dl>
%%      <dt>`CommandToken'</dt><dd>
%%          The command was spawned and results must be retrieved with one of
%%          the `spawned_result(CommandToken, ...)' functions.</dd>
%%      <dt>`{error, {Reason, Path}}'</dt><dd>
%%          The command cannot be located or is not executable by the current
%%          user, or a nonexistent or inaccessible directory is specified.<br/>
%%          `Reason' is the Posix error constant representing the error and
%%          `Path' is the specified `Cmd' or `Dir' to which the error applies.
%%      </dd></dl>
%%
%%
%% @see erlang:open_port/2
%%
spawn_cmd(Cmd, Dir, Args, Env) ->
    ?LOG_DEBUG("C: ~0p D: ~0p A: ~0p E: ~0p", [Cmd, Dir, Args, Env]),
    Opts0 = opt_args(Args, opt_env(Env, #{})),
    case opt_dir(Dir, Opts0) of
        {error, _} = DirError ->
            ?LOG_ERROR("~0p", [DirError]),
            DirError;
        OptsMap ->
            case port_exe(Cmd, OptsMap) of
                {error, _} = CmdError ->
                    ?LOG_ERROR("~0p", [CmdError]),
                    CmdError;
                {_, Exe} = PortCmd ->
                    PortOpts = [in, eof, hide, exit_status,
                        stderr_to_stdout | maps:to_list(OptsMap)],
                    ?LOG_DEBUG("running ~s with ~0p", [Exe, PortOpts]),
                    try
                        Port = erlang:open_port(PortCmd, PortOpts),
                        #rt_exec_token{port = Port, exe = Exe, args = Args}
                    catch
                        error:eacces ->
                            {error, {eacces, Exe}}
                    end
            end
    end.

-spec spawned_result(
    CmdToken :: cmd_token() )
        -> {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_timeout_err().
%%
%% @doc Retrieves the result of a previously spawned command.
%%
%% @equiv spawned_result(CmdToken, string, default_timeout())
%%
%% @see spawned_result/3
%%
spawned_result(CmdToken) ->
    ?LOG_DEBUG("X: ~0p", [CmdToken]),
    case spawned_result(CmdToken, string, default_timeout()) of
        {RC, string, Output} ->
            {RC, Output};
        Error ->
            Error
    end.

-spec spawned_result(
    CmdToken :: cmd_token(),
    FormOrTimeout :: rtt:cmd_out_format() | rtt:millisecs() )
        -> rtt:cmd_result() | rtt:cmd_timeout_err().
%%
%% @doc Retrieves the result of a previously spawned command.
%%
%% @param FormOrTimeout <dl>
%%  <dt>if a format atom, equivalent to</dt>
%%      <dd>`spawned_result(CmdToken, Form, default_timeout())'</dd>
%%  <dt>otherwise, equivalent to</dt>
%%      <dd>`spawned_result(CmdToken, string, Timeout)'</dd>
%% </dl>
%%
%% @see spawned_result/3
%%
spawned_result(CmdToken, Timeout) when ?is_ms_timeout(Timeout) ->
    ?LOG_DEBUG("X: ~0p T: ~0p", [CmdToken, Timeout]),
    spawned_result(CmdToken, string, Timeout);
spawned_result(CmdToken, Form) ->
    ?LOG_DEBUG("X: ~0p F: ~0p", [CmdToken, Form]),
    spawned_result(CmdToken, Form, default_timeout()).

-spec spawned_result(
    CmdToken :: cmd_token(),
    Form :: rtt:cmd_out_format(),
    Timeout :: rtt:millisecs() )
        -> rtt:cmd_result() | rtt:cmd_timeout_err().
%%
%% @doc Retrieves the result of a previously spawned command.
%%
%% @param CmdToken
%%      The token returned from a successful invocation of one of the
%%      `spawn_cmd(...)' functions.
%%
%% @param Form
%%      Specifies the format of the output.<br/>
%%      Note that all command output to `stdout' and `stderr' is included, so
%%      the two streams' lines may be intermingled.<br/>
%%      If you don't care about program output and will only look at the return
%%      code, the `rlines' format is the most efficient.<br/>
%%      In any output form, `$\r,$\n' sequences do not receive special
%%      handling, but <i>should</i> be converted to `$\n' by the intervening IO
%%      layers.<dl>
%%      <dt>`rlines'</dt><dd>
%%          Output is returned as a list of lines in reverse output order,
%%          including blank lines, without their terminating newline.<br/>
%%          This is the order in which the lines are accumulated, and as such
%%          is most efficient.</dd>
%%      <dt>`lines'</dt><dd>
%%          Output is returned as a list of lines in the order that the
%%          command output them, including blank lines, without their
%%          terminating newline.</dd>
%%      <dt>`stream'</dt><dd>
%%          Output is returned in a single string, possibly with embedded
%%          newlines. This output format can be convenient when the output is
%%          to be used in regular expression matching/filtering.<br/>
%%          Successive newlines (blank lines) are <i>NOT</i> stripped nor, by
%%          extension, are trailing newlines.</dd>
%%      </dl>
%%
%% @param Timeout
%%      Specifies how long to wait for execution to complete.
%%
%% @returns <dl>
%%      <dt>`{ReturnCode, Format, Output}'</dt><dd>
%%          The command completed within the allotted time, returning
%%          `ReturnCode', with `Output' formatted as specified by the `Form'
%%          parameter.<br/>
%%          If the `Cmd' program closed its `stdout' before exiting, or any
%%          other condition prevents retrieving the command's result code,
%%          `ReturnCode' will be set to `-1'.</dd>
%%      <dt>`{error, {timeout, {[Exe | Args], OutLines}}}'</dt><dd>
%%          The command did not complete within the specified `Timeout'.<br/>
%%          `Exe' is the absolute path to the executable file, `Args' is the
%%          supplied parameters, and `OutLines' is a list of whatever output
%%          lines were collected from the command (in the order received)
%%          before it timed out.</dd>
%%      </dl>
%%
spawned_result(
    #rt_exec_token{port = Port, exe = Exe, args = Args} = _X, Form, Timeout)
        when (Form =:= rlines orelse Form =:= lines orelse Form =:= string)
        andalso ?is_ms_timeout(Timeout) ->
    ?LOG_DEBUG("X: ~0p F: ~0p T: ~0p", [_X, Form, Timeout]),
    Result = collect_result(Port, Form, Timeout),
    ?LOG_DEBUG("~s result: ~0p", [Exe, Result]),
    case Result of
        {timeout, Lines} ->
            {error, {timeout, {[Exe | Args], Lines}}};
        _ ->
            Result
    end;
spawned_result(CmdToken, Format, Timeout) ->
    erlang:error(badarg, [CmdToken, Format, Timeout]).

%% ===================================================================
%% Internal
%% ===================================================================

-type coll_lines() :: {rtt:cmd_rc(), rlines | lines, rtt:output_lines()}.
-type coll_string() :: {rtt:cmd_rc(), string, rtt:output_line()}.
-type coll_success() :: coll_lines() | coll_string().
-type port_accum() :: rtt:output_lines() | {port_cont(), rtt:output_lines()}.
-type port_cont() :: rtt:output_line() | rtt:output_lines() | list(port_cont()).
-type port_eol_tag() :: eol | noeol.
-type port_exit_status() :: {exit_status, rtt:cmd_rc()}.
-type port_rec_queue() :: list(port_exit_status() | port_timeout_msg() | binary()).
-type port_success_lines() :: {rtt:cmd_rc(), port_eol_tag(), rtt:output_lines()}.
-type port_timeout_lines() :: {timeout, rtt:output_lines()}.
-type port_timeout_msg() :: {timeout, term()}.

-spec cmd_line_elem(Elem :: string()) -> string().
%% @hidden
cmd_line_elem([]) ->
    [$", $"];
cmd_line_elem(Elem) ->
    case cmd_line_special(Elem) of
        true ->
            S1 = string:replace(Elem, [$\\], [$\\, $\\], all),
            S2 = string:replace(S1, [$"], [$\\, $"], all),
            [$" | S2] ++ [$"];
        _ ->
            Elem
    end.

-spec cmd_line_special(Elem :: string()) -> boolean().
%% @hidden
%% Returns `true' if Elem contains any character that *may* require it to be
%% escaped and/or quoted when used in a shell command line  - not all always
%% have special meaning, depending on context.
%% Clauses are ordered by ascending ascii value, let the compiler optimize it.
cmd_line_special([]) ->
    false;
cmd_line_special([Ch | _]) when Ch < $% ->
    true;
cmd_line_special([Ch | _]) when Ch > $% andalso Ch < $+ ->
    true;
cmd_line_special([$; | _]) ->
    true;
cmd_line_special([$< | _]) ->
    true;
cmd_line_special([$> | _]) ->
    true;
cmd_line_special([$? | _]) ->
    true;
cmd_line_special([Ch | _]) when Ch > $Z andalso Ch < $^ ->
    true;
cmd_line_special([$` | _]) ->
    true;
cmd_line_special([Ch | _]) when Ch > $z andalso Ch < 128 ->
    true;
cmd_line_special([_Ch | Rest]) ->
    cmd_line_special(Rest).

-spec collect_result(
    Port :: port(),
    Form :: rlines | lines,
    Timeout :: rtt:millisecs() )
        -> coll_lines() | port_timeout_lines();
    (
    Port :: port(),
    Form :: string,
    Timeout :: rtt:millisecs() )
        -> coll_string() | port_timeout_lines().
%% @hidden
collect_result(Port, Form, Timeout) ->
    collect_transform(handle_port(Port, Timeout), Form).

-spec collect_transform(
    PortResult :: port_success_lines() | port_timeout_lines(),
    Form :: rtt:cmd_out_format() )
        -> coll_success() | port_timeout_lines().
%% @hidden
%% Separated from collect_result/3 to use head pattern matching instead
%% of three levels of case clauses - let the compiler sort it out.
collect_transform({RC, _Tag, [] = Lines}, Form) ->
    {RC, Form, Lines};
collect_transform({RC, _Tag, Lines}, rlines = Form) ->
    {RC, Form, Lines};
collect_transform({RC, _Tag, Lines}, lines = Form) ->
    {RC, Form, lists:reverse(Lines)};
collect_transform({RC, eol, Lines}, string = Form) ->
    {RC, Form, lists:flatten([lists:join($\n, lists:reverse(Lines)), "\n"])};
collect_transform({RC, noeol, Lines}, string = Form) ->
    {RC, Form, lists:flatten(lists:join($\n, lists:reverse(Lines)))};
collect_transform({timeout, []} = Error, _Form) ->
    Error;
collect_transform({timeout = Err, Lines}, _Form) ->
    %% Per doc, lines returned in the order received, regardless of Form
    {Err, lists:reverse(Lines)};
%% Oops, check yer specs ...
collect_transform(PortResult, Form) ->
    erlang:error(badmatch, [PortResult, Form]).

-spec default_timeout() -> rtt:millisecs().
%% @hidden
-ifdef(TEST).
default_timeout() ->
    ?TEST_EXEC_TIMEOUT.
-else.
default_timeout() ->
    rt_config:get(rt_max_wait_time).
-endif. % TEST

-spec handle_port(
    Port :: port(),
    Timeout :: rtt:millisecs() )
        -> port_success_lines() | port_timeout_lines().
%% @hidden
handle_port(Port, Timeout) ->
    TimeoutMsg = {Port, ?COMMAND_TIMEOUT_MSG},
    TimeoutRef = erlang:send_after(Timeout, erlang:self(), TimeoutMsg),
    %% We'll later ensure the required {Port, eof} message is queued.
    Result = port_exit_status(Port, []),
    _ = erlang:cancel_timer(TimeoutRef),
    _ = (catch erlang:port_close(Port)),
    %% Make sure there's always at least one {Port, eof} message in the queue
    %% to terminate handle_port(Port, Accum), the subsequent flush will clean
    %% out any duplicates.
    erlang:self() ! {Port, eof},
    %% Now that the executable is finished, whether it wanted to or not,
    %% within the Timeout period, collect its output.
    {Tag, Lines} = handle_port_message(Port, []),
    %% Get rid of any stragglers
    port_flush(Port),
    case Result of
        {exit_status, RC} ->
            {RC, Tag, Lines};
        {timeout, _} ->
            %% On timeout, return whatever output we've gotten in the order
            %% received, since the error is likely to be logged as returned.
            {timeout, lists:reverse(Lines)}
    end.

-spec handle_port_message(
    Port :: port(),
    Accum :: port_accum() )
        -> {Tag :: port_eol_tag(), Out :: rtt:output_lines()}.
%% @hidden
handle_port_message(Port, Accum) ->
    receive
        {Port, {data, Data}} ->
            handle_line_data(Port, Data, Accum);
        {Port, eof} ->
            handle_line_eof(Accum)
    end.

-spec handle_line_data(
    Port :: port(),
    Data :: {port_eol_tag(), rtt:output_line()},
    Accum :: port_accum() )
        -> {Tag :: port_eol_tag(), Out :: rtt:output_lines()}.
%% @hidden
handle_line_data(Port, {eol, Data}, []) ->
    handle_port_message(Port, [Data]);
handle_line_data(Port, {eol, Data}, {Cont, Lines}) ->
    handle_port_message(Port, [lists:flatten([Cont, Data]) | Lines]);
handle_line_data(Port, {eol, Data}, Lines) ->
    handle_port_message(Port, [Data | Lines]);

handle_line_data(Port, {noeol, Data}, {Cont, Lines}) ->
    handle_port_message(Port, {[Cont, Data], Lines});
handle_line_data(Port, {noeol, Data}, Lines) ->
    handle_port_message(Port, {Data, Lines}).

-spec handle_line_eof(Accum :: port_accum())
        -> {Tag :: port_eol_tag(), Out :: rtt:output_lines()}.
%% @hidden
handle_line_eof({Cont, Lines}) ->
    {noeol, [lists:flatten(Cont) | Lines]};
handle_line_eof(Lines) ->
    {eol, Lines}.

-spec interact_run(
    Port :: port(),
    Ops :: rtt:interact_list(),
    Timeout :: rtt:millisecs() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} | port_timeout_msg().
%% @hidden
interact_run(Port, Ops, Timeout) ->
    TimeoutMsg = {Port, ?COMMAND_TIMEOUT_MSG},
    TimeoutRef = erlang:send_after(Timeout, erlang:self(), TimeoutMsg),
    Result = interact_seq(Ops, Port, []),
    _ = erlang:cancel_timer(TimeoutRef),
    _ = (catch erlang:port_close(Port)),
    port_flush(Port),
    Result.

-spec interact_seq(
    Ops :: rtt:interact_list(),
    Port :: port(),
    Queued :: port_rec_queue() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} | port_timeout_msg().
%% @hidden The interactive processing loop.
%%
%% There are a lot of conditions to consider, and in all cases where possible
%% they're matched in function heads - let the compiler sort them out.
%%
%% @end

%% We're out of operations, see if we have an exit code waiting ...
interact_seq([] = _O, Port, [] = Queue) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, Queue]),
    case port_exit_status(Port, Queue) of
        {exit_status, RC} ->
            {ok, RC};
        TimeoutMsg ->
            TimeoutMsg
    end;

%% No operations left, and a waiting exit code - this is the happy path.
interact_seq([] = _O, _Port, [{exit_status, RC} = _Q | _]) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, _Port, _Q]),
    {ok, RC};

%% Burn off the remaining outputs, maybe we'll get to an exit code ...
interact_seq([] = Ops, Port, [_Next | Queued]) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [Ops, Port, _Next]),
    interact_seq(Ops, Port, Queued);

%% Waiting for an exit, and there it is - another happy path.
interact_seq([{expect, exit, TimeoutRef}] = _O, _Port,
        [{exit_status, RC} = _Q | _]) when erlang:is_reference(TimeoutRef) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, _Port, _Q]),
    _ = erlang:cancel_timer(TimeoutRef),
    {ok, RC};
interact_seq([{expect, exit, _Timeout}] = _O,
        _Port, [{exit_status, RC} = _Q | _]) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, _Port, _Q]),
    {ok, RC};

%% The only thing we care about is an exit status, discard until we get one.
interact_seq([{expect, exit, _TimeoutRef}] = Ops, Port, [_Next | Queued]) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [Ops, Port, _Next]),
    interact_seq(Ops, Port, Queued);

%% Operations remain, but the execution's done.
interact_seq([{expect, _Pred, TimeoutRef} = _O | _], _Port,
        [{exit_status, RC} = _Q | _]) when erlang:is_reference(TimeoutRef) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, _Port, _Q]),
    _ = erlang:cancel_timer(TimeoutRef),
    {exit, RC};
interact_seq([_O | _], _Port, [{exit_status, RC} = _Q | _]) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, _Port, _Q]),
    {exit, RC};

%% Clear everything *except* non-discardable terminal message.
interact_seq([clear = _O | Ops], Port, Queued) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, ?LOG_HOL(Queued)]),
    interact_seq(Ops, Port, port_clear_keep_exit(Port, Queued));

%% Send to the executable's stdin, handling cases where it's no longer alive.
%% If the port's closed when we try to send, that's an error condition to be
%% reported with an '{exit, RC}' result (or a timeout, but that shouldn't
%% happen here). See the comment in interact/6 to find out why this is so
%% convoluted.
interact_seq([{send, Data} = _O | Ops], Port, Queued) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, ?LOG_HOL(Queued)]),
    case (catch erlang:port_command(Port, Data)) of
        true ->
            %% This doesn't mean we're out of the woods - there could be an
            %% 'EXIT' message due to the port crashing even though the data
            %% claims to have been sent.
            receive
                {'EXIT' = Trigger, Port, Reason} = _ExitMsg ->
                    ?LOG_DEBUG("Received ~0p", [_ExitMsg]),
                    {exit_status, RC} =
                        port_eof_exit_status(Port, {Trigger, Reason}),
                    {exit, RC}
            after
                %% Don't wait too long, this introduces a delay on success
                10 ->
                    interact_seq(Ops, Port, Queued)
            end;
        _Catch ->
            %% The only possibility here is that the port is closed, and
            %% port_command/2 guarantees that its 'EXIT' message has been
            %% delivered. Go get it, it can't not be there ...
            ?LOG_DEBUG("P: ~0p caught ~0p", [Port, _Catch]),
            case port_exit_status(Port, Queued) of
                {exit_status, RC} ->
                    {exit, RC};
                TimeoutMsg ->
                    TimeoutMsg
            end
    end;

%% It's not clear that there's a viable use case for the 'pause' operation
interact_seq([{pause, Millis} = _O | Ops], Port, Queued) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, ?LOG_HOL(Queued)]),
    timer:sleep(Millis),
    interact_seq(Ops, Port, Queued);

%% The first time we see an 'expect' with a numeric Timeout, schedule a timeout
%% message and replace Timeout with the timer reference for later cancellation.
interact_seq([{expect, Pred, Timeout} = _O | Ops], Port, Queued)
        when ?is_ms_timeout(Timeout) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, ?LOG_HOL(Queued)]),
    TimeoutMsg = {Port, {timeout, Pred}},
    TimeoutRef = erlang:send_after(Timeout, erlang:self(), TimeoutMsg),
    interact_seq([{expect, Pred, TimeoutRef} | Ops], Port, Queued);

%% Either get something into the data queue or we're done.
interact_seq([{expect, _Pred, _TimeoutRef} = _O | _] = Ops, Port, [] = _Q) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, _Q]),
    case port_next_record(Port, []) of
        {timeout, _} = TimeoutMsg ->
            TimeoutMsg;
        {line, Line} ->
            interact_seq(Ops, Port, [Line]);
        {exit_status, _RC} = ExitStatus ->
            interact_seq(Ops, Port, [ExitStatus])
    end;

%% See about matching a pattern ...
interact_seq([{expect, RE, TimeoutRef} = _O | RemOps] = AllOps,
        Port, [Next | Queued] = _Q) when ?is_compiled_regex(RE) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, Next]),
    case re:run(Next, RE, [{capture, none}]) of
        match ->
            _ = erlang:cancel_timer(TimeoutRef),
            interact_seq(RemOps, Port, Queued);
        _Nomatch ->
            interact_seq(AllOps, Port, Queued)
    end;
interact_seq([{expect, Match, TimeoutRef} = _O | RemOps] = AllOps,
        Port, [Next | Queued] = _Q) ->
    ?LOG_DEBUG("O: ~0p P: ~0p Q: ~0p", [_O, Port, Next]),
    case binary:match(Next, Match) of
        nomatch ->
            interact_seq(AllOps, Port, Queued);
        _Found ->
            _ = erlang:cancel_timer(TimeoutRef),
            interact_seq(RemOps, Port, Queued)
    end;

%% Oops, missed a pattern!
interact_seq(Ops, Port, Queued) ->
    Args = [Ops, Port, Queued],
    ?LOG_ALERT("Programming error, missed pattern: ~0p", [Args]),
    erlang:error(badarg, Args).

%%
%% End of the interactive processing loop.
%%

-spec opt_args(Args :: rtt:cmd_args(), Opts :: map()) -> map().
%% @hidden
opt_args([], Opts) ->
    Opts;
opt_args(Args, Opts) when erlang:is_list(Args) ->
    validate_args(Args),
    Opts#{args => Args};
opt_args(Args, _Opts) ->
    erlang:error(badarg, [args, Args]).

-spec opt_dir(Dir :: rtt:cmd_dir(), Opts :: map())
        -> map() | {error, {file:posix(), rtt:fs_path()}}.
%% @hidden
opt_dir(cwd, Opts) ->
    Opts;
opt_dir(pwd, Opts) ->
    Opts;
opt_dir(".", Opts) ->
    Opts;
opt_dir(Dir, Opts) when erlang:is_list(Dir) ->
    %% If we don't have permission to cd to Dir, open_port isn't going to give
    %% us a helpful error, so try it in a controlled environment so we can
    %% return something meaningful.
    %% Since we're going to try to cd to it, don't bother with filelib:is_dir,
    %% because file:set_cwd will give us the same error we'd manufacture.
    CWD = resolve_cwd(),
    case file:set_cwd(Dir) of
        ok ->
            GetDir = file:get_cwd(),
            ok = file:set_cwd(CWD),
            case GetDir of
                {ok, CWD} ->
                    %% Already in the specified directory!
                    Opts;
                {ok, Abs} ->
                    Opts#{cd => Abs};
                {error, GetErr} ->
                    %% WUT???
                    {error, {GetErr, Dir}}
            end;
        {error, CdErr} ->
            {error, {CdErr, Dir}}
    end;
opt_dir(Dir, _Opts) ->
    erlang:error(badarg, [dir, Dir]).

-spec opt_env(Env :: list(rtt:env_var()), Opts :: map()) -> map().
%% @hidden
opt_env([], Opts) ->
    Opts;
opt_env(Env, Opts) when erlang:is_list(Env) ->
    validate_env(Env),
    Opts#{env => Env};
opt_env(Env, _Opts) ->
    erlang:error(badarg, [env, Env]).

-spec opt_form(Form :: rtt:cmd_out_format(), Opts :: map()) -> map().
%% @hidden
opt_form(Form, Opts)
        when Form =:= rlines orelse Form =:= lines orelse Form =:= string ->
    Opts#{line => default_linebuf()};
opt_form(Form, _Opts) ->
    erlang:error(badarg, [form, Form]).

-spec port_exe(Cmd :: rtt:cmd_exe(), Opts :: map())
        -> {spawn_executable, rtt:fs_path()} |
            {error, {file:posix(), rtt:fs_path()}}.
%% @hidden
port_exe(Cmd, #{cd := Dir} = _Opts) when erlang:is_list(Cmd) ->
    case resolve_cmd(Cmd, Dir) of
        {error, What} ->
            {error, {What, Cmd}};
        AbsCmd ->
            {spawn_executable, AbsCmd}
    end;
port_exe(Cmd, _Opts) when erlang:is_list(Cmd) ->
    case resolve_cmd(Cmd) of
        {error, What} ->
            {error, {What, Cmd}};
        AbsCmd ->
            {spawn_executable, AbsCmd}
    end;
port_exe(Cmd, _Opts) ->
    erlang:error(badarg, [cmd, Cmd]).

-spec port_clear_keep_exit(Port :: port(), Queue :: port_rec_queue())
        -> port_rec_queue().
%% @hidden
%% Clears everything out of the Queue and mailbox EXCEPT the first non-
%% discardable message that would stop processing, if present.
port_clear_keep_exit(Port, [] = Queue) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, Queue]),
    %% Match everything except `{Port, {exit_status, RC}}', which we want to
    %% remain in the mailbox until it's explicitly retrieved.
    receive
        {Port, ?COMMAND_TIMEOUT_MSG = TimeoutMsg} ->
            [TimeoutMsg];
        {Port, eof = Trigger} ->
            [port_eof_exit_status(Port, Trigger)];
        {'EXIT' = Trigger, Port, Reason} ->
            [port_eof_exit_status(Port, {Trigger, Reason})];
        {Port, {data, _}} = _Msg ->
            ?LOG_DEBUG("Discarding ~0p", [_Msg]),
            port_clear_keep_exit(Port, Queue)
    after
        0 ->
            Queue
    end;
port_clear_keep_exit(Port, [{exit_status, _RC} = ExitStatus | _]) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, ExitStatus]),
    [ExitStatus];
port_clear_keep_exit(Port, [?COMMAND_TIMEOUT_MSG = TimeoutMsg | _]) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, TimeoutMsg]),
    [TimeoutMsg];
port_clear_keep_exit(Port, [_Next | Queue]) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, _Next]),
    port_clear_keep_exit(Port, Queue).

-spec port_eof_exit_status(Port :: port(), Trigger :: term() )
        -> port_exit_status().
%% @hidden
%% Collect or synthesize the port's exit status after receiving an indication
%% (the Trigger) that the port's execution has stopped.
%%
%% The delivery order of exit_status/eof is unspecified, but try to handle any
%% order - which may not include an exit_status message at all - sensibly. If
%% they ARE both delivered, it should be within a pretty short interval, BUT
%% if the system is under extreme load and messages aren't being delivered or
%% matched in a timely manner, give it some time before increasing the load
%% even more with a log entry.
port_eof_exit_status(Port, Trigger) ->
    ?LOG_DEBUG("P: ~0p T: ~0p", [Port, Trigger]),
    receive
        {Port, {exit_status, _RC} = ExitStatus} ->
            ExitStatus
    after
        ?EOF_WAIT_FOR_EXIT_STATUS ->
            ?LOG_ERROR(
                "Port ~0p missing 'exit_status' message after ~0p",
                [Port, Trigger]),
            {exit_status, -1}
    end.

-spec port_exit_status(Port :: port(), Queued :: list(binary() | tuple()) )
        -> port_exit_status() | port_timeout_msg().
%% @hidden
%% Collect the port's exit status, if available, or synthesize one.
%% This waits forever - don't call it unless you're sure it'll find something.
port_exit_status(Port, [] = _Q) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, _Q]),
    receive
        {Port, {exit_status, _RC} = ExitStatus} ->
            ExitStatus;
        {Port, eof = Trigger} ->
            port_eof_exit_status(Port, Trigger);
        {'EXIT' = Trigger, Port, Reason} ->
            port_eof_exit_status(Port, {Trigger, Reason});
        {Port, {timeout, _} = TimeoutMsg} ->
            TimeoutMsg
    end;
port_exit_status(Port, [{exit_status, _RC} = ExitStatus | _] = _Q) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, ExitStatus]),
    ExitStatus;
port_exit_status(Port, [?COMMAND_TIMEOUT_MSG = TimeoutMsg | _] = _Q) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, TimeoutMsg]),
    TimeoutMsg;
port_exit_status(Port, [_Next | Queued] = _Q) ->
    ?LOG_DEBUG("P: ~0p Q: ~0p", [Port, _Next]),
    port_exit_status(Port, Queued).

-spec port_flush(Port :: port()) -> ok.
%% @hidden
%% Clean out any remaining messages relating to Port. We match stuff that we
%% may never receive so that we don't have to worry about what spawn/monitor
%% options and patterns we may decide to use in the future.
port_flush(Port) ->
    receive
        {Port, _} = _M ->
            ?LOG_DEBUG(_M),
            port_flush(Port);
        {'EXIT', Port, _} = _M ->
            ?LOG_DEBUG(_M),
            port_flush(Port);
        {'DOWN', _, _, Port, _} = _M ->
            ?LOG_DEBUG(_M),
            port_flush(Port)
    after
        1 ->
            ok
    end.

-spec port_next_record(Port :: port(), Accum :: iolist())
        -> {line, binary()} | port_exit_status() | port_timeout_msg().
%% @hidden Get the next message to be processed.
%%
%%  *   EOF here refers to either a `{Port, eof}' or `{EXIT, Port, Reason}'
%%      message, as both are indicative of the same event - which is delivered
%%      depends on `open_port/2' options and `port_command/2,3' usage.
%%
%%  -   Data messages are always delivered before the EOF message.
%%  -   `{Port, {timeout, What}}' and/or `{Port, {exit_status, RC}}' may be
%%      received at any time, including before the last data message.
%%  -   We never want to extract an exit_status message here UNLESS we've
%%      first seen the EOF message, as it stops further processing of pending
%%      data messages.
%%  -   Managing the handling sequence sometimes involves appending an EOF
%%      message to the mailbox to replace something we extracted.
port_next_record(Port, [] = _A) ->
    ?LOG_DEBUG("P: ~0p A: ~0p", [Port, _A]),
    receive
        {Port, {timeout, _} = TimeoutMsg} ->
            TimeoutMsg;
        {Port, {data, {eol, Line}}} ->
            {line, Line};
        {Port, {data, {noeol, Data}}} ->
            port_next_record(Port, [Data]);
        {Port, eof = Trigger} ->
            port_eof_exit_status(Port, Trigger);
        {'EXIT' = Trigger, Port, Reason} ->
            port_eof_exit_status(Port, {Trigger, Reason})
    end;
port_next_record(Port, Accum) ->
    ?LOG_DEBUG("P: ~0p A: ~0p", [Port, Accum]),
    receive
        {Port, {timeout, _} = TimeoutMsg} ->
            TimeoutMsg;
        {Port, {data, {eol, Data}}} ->
            {line, erlang:list_to_binary([Accum, Data])};
        {Port, {data, {noeol, Data}}} ->
            port_next_record(Port, [Accum, Data]);
    %%
    %% Make sure EOF messages get re-handled later with an empty accumulator,
    %% as we don't want to discard previously received data.
    %%
        {Port, eof} = Msg ->
            erlang:self() ! Msg,
            {line, erlang:list_to_binary(Accum)};
        {'EXIT', Port, _Reason} = Msg ->
            erlang:self() ! Msg,
            {line, erlang:list_to_binary(Accum)}
    end.

-spec resolve_cwd() -> rtt:fs_path().
%% @hidden
resolve_cwd() ->
    case file:get_cwd() of
        {ok, CWD} ->
            CWD;
        {error, Posix} ->
            erlang:error(Posix, [get_cwd])
    end.

-spec resolve_executable(
    Path :: list(rtt:fs_path()),
    RelCmd :: rtt:cmd_exe() )
        -> rtt:cmd_exe() | {error, file:posix()}.
%% @hidden
resolve_executable(Path, RelCmd) ->
    resolve_executable(Path, RelCmd, undefined).

-spec resolve_executable(
    Path :: list(rtt:fs_path()),
    RelCmd :: rtt:cmd_exe(),
    FoundErr :: {error, file:posix()} | undefined )
        -> rtt:cmd_exe() | {error, file:posix()}.
%% @hidden
resolve_executable([], _RelCmd, undefined) ->
    {error, enoent};
resolve_executable([], _RelCmd, FoundErr) ->
    FoundErr;
resolve_executable([[$/ | _] = Dir | Dirs], RelCmd, FoundErr) ->
    AbsPath = filename:join(Dir, RelCmd),
    case resolve_exe_detail(AbsPath) of
        {error, enoent} ->
            resolve_executable(Dirs, RelCmd, FoundErr);
        {error, _} = Error ->
            case FoundErr of
                undefined ->
                    resolve_executable(Dirs, RelCmd, Error);
                _ ->
                    resolve_executable(Dirs, RelCmd, FoundErr)
            end;
        RetPath ->
            RetPath
    end;
%% Dir is not absolute, make it so ...
resolve_executable([Dir | Dirs], RelCmd, FoundErr) when erlang:is_list(Dir) ->
    resolve_executable(
        [filename:absname(resolve_dir(Dir)) | Dirs], RelCmd, FoundErr);
resolve_executable([Dir | _], RelCmd, _FoundErr) ->
    erlang:error(badarg, [Dir, RelCmd]).

-spec resolve_exe_detail(Cmd :: rtt:cmd_exe())
        -> rtt:cmd_exe() | {error, file:posix()}.
%% @hidden
resolve_exe_detail(AbsPath) ->
    case file:read_file_info(AbsPath, [{time, posix}, raw]) of
        %% Undocumented, but as of OTP 22 symlinks are followed, so the real
        %% type of the path is reported - i.e. type will never be 'symlink'.
        {ok, #file_info{type = Type, mode = Mode}} ->
            case Type of
                regular ->
                    %% This check is flawed, as it's only testing whether ANY
                    %% execute bit is set, which may not be applicable to the
                    %% current user, but we're not going to resolve all that.
                    if (Mode band 8#111) =/= 0 ->
                        AbsPath;
                    true ->
                        {error, eacces}
                    end;
                directory ->
                    {error, eisdir};
                _ ->
                    {error, eftype}
            end;
        Error ->
            Error
    end.

-spec validate_args(Args :: list(rtt:cmd_arg())) -> ok.
%% @hidden
validate_args([]) ->
    ok;
validate_args([Arg | Args]) when erlang:is_list(Arg) ->
    validate_args(Args);
validate_args(Args) ->
    erlang:error(badarg, [args, Args]).

-spec validate_env(Env :: list(rtt:env_var())) -> ok.
%% @hidden
validate_env([]) ->
    ok;
validate_env([{[_|_], Val} | Envs]) when erlang:is_list(Val) ->
    validate_env(Envs);
validate_env([{[_|_], false} | Envs]) ->
    validate_env(Envs);
validate_env(Env) ->
    erlang:error(badarg, [env, Env]).

-spec validate_interact_ops(
    OpsIn :: rtt:interact_list() )
        -> OpsOut :: rtt:interact_list() | no_return().
%% @hidden Validate and, where appropriate, modify the list of operations.
%% This serves as something of a precompiler, inserting, removing, and
%% modifying instructions for proper handling by interact_seq/3.
%% Any pattern violations result in a `badarg' exception.
validate_interact_ops(OpsIn) ->
    validate_interact_ops(OpsIn, []).

-spec validate_interact_ops(
    OpsIn :: rtt:interact_list(),
    RevOpsIn :: rtt:interact_list() )
        -> OpsOut :: rtt:interact_list() | no_return().
%% @hidden
validate_interact_ops([], RevOpsOut) ->
    lists:reverse(RevOpsOut);

%% `{expect, exit ...}' is only allowed as the last operation
validate_interact_ops([{expect, exit}, _Next | _] = Ops, _) ->
    erlang:error(badarg, [ops_after_exit, Ops]);
validate_interact_ops([{expect, exit, _}, _Next | _] = Ops, _) ->
    erlang:error(badarg, [ops_after_exit, Ops]);

%% `expect' always has a timeout
validate_interact_ops([{expect, Pred} | Ops], RevOpsIn) ->
    OpOut = {expect, Pred, ?DEFAULT_EXPECT_TIMEOUT},
    validate_interact_ops([OpOut | Ops], RevOpsIn);

%% Validate predicates, the compiler *should* optimize out the timeout
%% guard on recursion from above.
validate_interact_ops([{expect, Pred, Timeout} | Ops], RevOpsIn)
        when ?is_ms_timeout(Timeout) ->
    OpOut = {expect, validate_interact_pred(Pred), Timeout},
    validate_interact_ops(Ops, [OpOut | RevOpsIn]);

%% Validate `send' contents
validate_interact_ops([{send, Send} | Ops], RevOpsIn) ->
    OpOut = {send, validate_interact_send(Send)},
    validate_interact_ops(Ops, [OpOut | RevOpsIn]);

%% Filter out repeat `clear' operations
validate_interact_ops([clear | Ops], [clear | _] = RevOpsIn) ->
    validate_interact_ops(Ops, RevOpsIn);
validate_interact_ops([clear = Op | Ops], RevOpsIn) ->
    validate_interact_ops(Ops, [Op | RevOpsIn]);

%% `pause' always has a timeout
validate_interact_ops([pause | Ops], RevOpsIn) ->
    OpOut = {pause, ?DEFAULT_INTERACTIVE_PAUSE},
    validate_interact_ops(Ops, [OpOut | RevOpsIn]);
validate_interact_ops([{pause, Timeout} = Op | Ops], RevOpsIn)
        when ?is_ms_timeout(Timeout) ->
    validate_interact_ops(Ops, [Op | RevOpsIn]);

%% Unmatched operation
validate_interact_ops([Op | _], _RevOpsIn) ->
    erlang:error(badarg, [interact_op, Op]).

-spec validate_interact_pred(Pred :: rtt:interact_pred())
        -> rtt:interact_pred() | no_return().
%% @hidden
validate_interact_pred(exit = Pred) ->
    Pred;
validate_interact_pred(Pred) when ?is_compiled_regex(Pred) ->
    Pred;
validate_interact_pred({re, [_|_] = Str}) ->
    case re:compile(Str) of
        {ok, MP} ->
            MP;
        {error, What} ->
            erlang:error(What, [re, compile, Str])
    end;
validate_interact_pred({re, Bin}) when ?is_nonempty_binary(Bin) ->
    case re:compile(Bin) of
        {ok, MP} ->
            MP;
        {error, What} ->
            erlang:error(What, [re, compile, Bin])
    end;
validate_interact_pred([_|_] = Str) ->
    erlang:list_to_binary(Str);
validate_interact_pred(Bin) when ?is_nonempty_binary(Bin) ->
    Bin;
validate_interact_pred(Pred) ->
    erlang:error(badarg, [expect, Pred]).

-spec validate_interact_send(Send :: nonempty_string() | binary())
        -> binary() | no_return().
%% @hidden
validate_interact_send([_|_] = Send) ->
    erlang:list_to_binary(Send);
validate_interact_send(Send) when ?is_nonempty_binary(Send) ->
    Send;
validate_interact_send(Send) ->
    erlang:error(badarg, [send, Send]).

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

cmd_rc_test() ->
    ?assertMatch({0, []}, cmd("true", [])),
    ?assertMatch({N, []} when N /= 0, cmd("false", [])).

cmd_lines_test() ->
    Cmd = "printf",
    Fmt = "%s\n%s\n\n",
    Prms = ["line 1", "line 2"],
    Form = lines,
    Expect = Prms ++ [""],
    ?LOG_DEBUG("Expect: ~0p", [Expect]),
    ?assertMatch({0, Form, Expect}, cmd(Cmd, cwd, [Fmt | Prms], [], Form)).

cmd_rlines_test() ->
    Cmd = "printf",
    Fmt = "%s\n%s\n\n",
    Prms = ["line 1", "line 2"],
    Form = rlines,
    Expect = lists:reverse(Prms ++ [""]),
    ?LOG_DEBUG("Expect: ~0p", [Expect]),
    ?assertMatch({0, Form, Expect}, cmd(Cmd, cwd, [Fmt | Prms], [], Form)).

cmd_string_test() ->
    Cmd = "printf",
    Fmt = "%s\n%s\n\n",
    Prms = ["line 1", "line 2"],
    Form = string,
    Expect = lists:flatten(lists:join($\n, Prms ++ ["", ""])),
    ?LOG_DEBUG("Expect: ~0p", [Expect]),
    ?assertMatch({0, Form, Expect}, cmd(Cmd, cwd, [Fmt | Prms], [], Form)).

spawn_rc_test() ->
    T_Tok = spawn_cmd("true", []),
    F_Tok = spawn_cmd("false", []),
    ?assertMatch({0, []}, spawned_result(T_Tok)),
    ?assertMatch({N, []} when N /= 0, spawned_result(F_Tok)).

interact_exit_test() ->
    Ops = [
        {send, "q().\n"},
        {expect, exit}
    ],
    ?assertMatch({ok, 0},
        interact("erl", cwd, [], [], Ops, default_timeout())).

interact_noexit_test() ->
    Ops = [
        {send, "q().\n"}
    ],
    ?assertMatch({ok, 0},
        interact("erl", cwd, [], [], Ops, default_timeout())).

interact_abort_test() ->
    Ops = [
        {send, "q().\n"},
        {expect, "never arrives"}
    ],
    ?assertMatch({exit, 0}, interact("erl", [], Ops)).

interact_send_after_exit_test_() ->
    {timeout, 15000, fun test_interact_send_after_exit/0}.

test_interact_send_after_exit() ->
    Ops = [
        {expect, {re, "abort with \\^G|press Ctrl+G to abort"}},
        {send, "q().\n"},
        {pause, 5000},
        {send, "halt(5).\n"}
    ],
    ?assertMatch({exit, 0},
        interact("erl", cwd, [], [], Ops, default_timeout())).

interact_match_test() ->
    Prompt = case string:to_integer(erlang:system_info(otp_release)) of
        {Rel, _} when Rel > 25 ->
            "press Ctrl+G to abort";
        _ ->
            "abort with ^G"
    end,
    Ops = [
        {expect, Prompt},
        {send, "q().\n"},
        {expect, exit}
    ],
    ?assertMatch({ok, 0},
        interact("erl", cwd, [], [], Ops, default_timeout())).

interact_regex_test() ->
    Ops = [
        {expect, {re, "abort with \\^G|press Ctrl+G to abort"}},
        {send, "q().\n"},
        {expect, exit}
    ],
    ?assertMatch({ok, 0},
        interact("erl", cwd, [], [], Ops, default_timeout())).

interact_rc_test() ->
    Ops = [
        {send, "halt(3).\n"},
        {expect, exit}
    ],
    ?assertMatch({ok, 3}, interact("erl", [], Ops)).

interact_expect_timeout_test() ->
    Ops = [
        {send, "q().\n"},
        {expect, exit, 1}
    ],
    ?assertMatch({timeout, exit}, interact("erl", [], Ops)).

interact_command_timeout_test() ->
    Ops = [
        {send, "q().\n"},
        {expect, exit}
    ],
    Exe = resolve_cmd("erl"),
    ?assertMatch({timeout, Exe}, interact("erl", [], Ops, 1)).

interact_clear_test() ->
    Out = "42",
    Ops = [
        {pause, 10},
        %% printf will have exited before getting here
        %% discard the buffered output
        clear,
        {expect, Out}
    ],
    ?assertMatch({exit, 0}, interact("printf", [Out], Ops)).

-endif. % TEST
