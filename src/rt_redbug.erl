%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%% Enable and disable tracing from the riak_test command line,
%% add `--tracing` to the test run to execute any calls to
%% `rt_redbug:trace/2` that are in the test suites. The traces
%% are printed to the test.log
%%
%% -------------------------------------------------------------------
%%
%% @doc Operations for tests to use `redbug' tracing.
%%
%% Unless tracing the local node itself, the local `redbug' process acts as a
%% proxy to `redbug' running on a target node.
%% Either way, only ONE node can be traced at a time.
%%
%% To trace more than one node concurrently, use `rpc', which is not handled by
%% this module because there are numerous complications with doing so.
%%
%% Refer to the <a href="https://hexdocs.pm/redbug/">Redbug Documentation</a>
%%
%% A somewhat out-of-date overview of using `redbug' is at <a href=
%% "https://robertoaloi.github.io/erlang/profiling-erlang-applications-using-redbug"
%% >Profiling Erlang Apps using Redbug</a>
%%
%% @todo {@link set_tracing_applied/1} alters the global configuration!
%% Instead, it should ONLY alter the currently-running test's context.
%%
%% @end
-module(rt_redbug).

%% Public API
%% For use by tests
-export([
    default_trace_options/0,
    is_tracing_applied/0,
    set_tracing_applied/1,
    stop/0,
    trace/2, trace/3
]).

%% Public Types
-export_type([
    opt_list/0,
    opt_map/0,
    opts/0,
    rtp/0,
    rtps/0
]).

-type opt_list() :: list({atom(), term()}).
%% Equivalent to (un-exported) <a
%% href="https://hexdocs.pm/redbug/redbug.html#type-proplist"
%% >redbug:proplist()</a>

-type opt_map() :: #{atom() => term()}.
%% The `map' version of {@link opt_list()}

-type opts() :: opt_list() | opt_map().
%% Equivalent to (un-exported) <a
%% href="https://hexdocs.pm/redbug/redbug.html#type-options"
%% >redbug:options()</a>

-type rtp() :: atom() | 'send' | 'receive' | nonempty_string().
%% Equivalent to (un-exported) <a
%% href="https://hexdocs.pm/redbug/redbug.html#type-rtp"
%% >redbug:rtp()</a>

-type rtps() :: nonempty_list(rtp()).
%% A list of {@link rtp()}s

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ENABLED_KEY,    apply_traces).
-define(TRACING_KEY,    rt_redbug_tracing).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Explicitly sets `redbug' tracing capability on or off in the global
%%      configuration, overriding any prior configuration.
%%
%% Tracing may have previously been set by (in order of precedence):<ol>
%%  <li>This function</li>
%%  <li>The `riak_test --trace' command line option</li>
%%  <li>`{apply_traces, <boolean>}' in the `riak_test.config' file</li>
%% </ol>
%% @returns The previous setting, whether from this function or
%%      global `riak_test' configuration.
-spec set_tracing_applied(TracingApplied :: boolean())
        -> OldValue :: boolean().
set_tracing_applied(TracingApplied) when erlang:is_boolean(TracingApplied) ->
    set_tracing_applied(is_tracing_applied(), TracingApplied).

%% @doc Report whether tracing is enabled.
-spec is_tracing_applied() -> boolean() | rtt:std_error().
is_tracing_applied() ->
    application:get_env(riak_test, ?ENABLED_KEY, false).

%% @doc Apply traces to ONE node using redbug tracing and syntax, with
%%      default options.
%% @equiv trace(Node, TraceStrings, [])
-spec trace(Node :: node(), TraceStrings :: rtp() | rtps())
        -> ok | rtt:std_error().
trace(Node, TraceStrings) ->
    trace(Node, TraceStrings, []).

%% @doc Apply traces to ONE node using redbug tracing and syntax, with
%%      non-default options.
%%
%% To set a trace on one or more functions:```
%%      rt_redbug:trace(Node, "riak_kv_qry_compiler:compile", Options).
%%      rt_redbug:trace(Node, ["riak_kv_qry_compiler:compile",
%%          "riak_ql_parser:parse"], Options).'''
%%
%% Multiple traces can be set in one call. Calling the `rt_redbug:trace'
%% function a second time stops the traces started by the first call.
%%
%% Traces can be stopped by calling {@link stop/0. `rt_redbug:stop()'}.
%% This is not needed before starting a new trace, due to the above behavior,
%% but may be important to ensure trace output file stability.
%%
%% @returns
%%  `ok' on success, `false' if tracing is not enabled, or `{error, Reason}'.
%%
-spec trace(Node :: node(), TraceStrings :: rtp() | rtps(), Options :: opts())
        -> ok | false | rtt:std_error().
trace(Node, TraceStrings, Options) when erlang:is_atom(Node) ->
    case is_tracing_applied() of
        true ->
            ?LOG_DEBUG("redbug start -> stop", []),
            ?assertMatch(ok, stop()),
            Opts = apply_options_to_defaults(Node, Options),
            ?LOG_INFO("starting redbug with: ~0p ~0p", [TraceStrings, Opts]),
            RbResult = redbug:start(TraceStrings, Opts),
            case RbResult of
                {error, _}  ->
                    ?LOG_ERROR("redbug:start error: ~0p", [RbResult]),
                    RbResult;
                R when erlang:is_tuple(R) ->
                    store_target_node(Node),
                    ?LOG_INFO("redbug:start returned: ~0p", [RbResult]);
                _ ->
                    ?LOG_ERROR("redbug:start error: ~0p", [RbResult]),
                    {error, RbResult}
            end;
        _ ->
            ?LOG_WARNING("Redbug tracing is not enabled", []),
            false
    end.

%% @doc Return default trace options, primarily for reference.
%%
%% Options specified to {@link trace/2} supersede the defaults.
-spec default_trace_options() -> opt_map().
default_trace_options() ->
    #{
        %% default ct timeout of 30 minutes
        %% http://erlang.org/doc/apps/common_test/write_test_chapter.html#id77922
        time => (30 * 60 * 1000),
        %% raise the threshold for the number of traces that can be received by
        %% redbug before tracing is suspended
        msgs => 1000,
        %% print milliseconds
        print_msec => true,
        %% the disterl cookie
        cookie => rt_config:get(rt_cookie, riak)
    }.

%% @doc Stop all redbug tracing unconditionally.
%%
%% This function blocks until `redbug' has actually stopped all of its
%% local and remote processes, or until its default timeout has expired.
%% If it times out waiting then `{error, timeout}' is returned.
-spec stop() -> ok | rtt:std_error().
stop() ->
    Node = take_target_node(),
    Stop = redbug:stop(Node),
    ?LOG_DEBUG("redbug:stop(~s) -> ~0p", [Node, Stop]),
    case Stop of
        not_started ->
            ok;
        _ ->
            ?LOG_INFO("Redbug stopping ...", []),
            %% redbug:stop is asynchronous, wait until it unregisters itself.
            RegName = redbug:redbug_name(Node),
            WaitFun = fun() -> erlang:whereis(RegName) =:= undefined end,
            case rt:wait_until(WaitFun) of
                ok ->
                    ?LOG_INFO("Redbug stopped", []);
                R ->
                    ?LOG_ERROR("Redbug failed to stop: ~0p", [R]),
                    {error, timeout}
            end
    end.


%% ===================================================================
%% Internal
%% ===================================================================

-spec apply_options_to_defaults(Node :: node(), Options :: opts()) -> opt_map().
%% @hidden
apply_options_to_defaults(Node, Options)
        when erlang:is_atom(Node) andalso erlang:is_map(Options) ->
    maps:merge(default_trace_options(), Options#{target => Node});
apply_options_to_defaults(Node, [{K, _}|_] = Options) when erlang:is_atom(K) ->
    apply_options_to_defaults(Node, maps:from_list(Options));
apply_options_to_defaults(Node, []) when erlang:is_atom(Node) ->
    (default_trace_options())#{target => Node}.

%% Dialyzer will correctly point out that some heads will never match, but we
%% want to keep the pattern(s) in here to cover when we switch to storing the
%% configuration in test metadata.
-dialyzer({no_match, set_tracing_applied/2}).
-spec set_tracing_applied(
    OldValue :: boolean() | rtt:std_error(), NewValue :: boolean() )
        -> PrevValue :: boolean().
%% @hidden Implementation of set_tracing_applied/1.
%% `NewValue' has already been validated as a boolean.
set_tracing_applied(SameValue, SameValue) ->
    ?LOG_INFO(
        "redbug traces are ~s in the test", [tracing_status(SameValue)]),
    SameValue;
set_tracing_applied(OldValue, NewValue) when erlang:is_boolean(OldValue) ->
    application:set_env(riak_test, ?ENABLED_KEY, NewValue),
    ?LOG_INFO(
        "redbug traces have been ~s in the test", [tracing_status(NewValue)]),
    NewValue orelse ?assertMatch(ok, stop()),
    OldValue;
set_tracing_applied({error, Reason}, _NewValue) ->
    ?LOG_ERROR("Unable to retrieve test metadata: ~0p", [Reason]),
    erlang:error(Reason);
set_tracing_applied(BadValue, _NewValue) ->
    ?LOG_ERROR("Internal riak_test programming error,"
        " '~s' should never be ~0p", [?ENABLED_KEY, BadValue]),
    erlang:error(bug, [?ENABLED_KEY, BadValue]).

-spec store_target_node(Node :: node()) -> ok | rtt:std_error().
%% @hidden Sets `Node' as the target node in the environment.
%% It is an error to set the node when the value is already populated.
store_target_node(Node) ->
    application:set_env(riak_test, ?TRACING_KEY, Node).

-spec take_target_node() -> node().
%% @hidden Removes and returns the target node from the environment,
%%      or returns the current node if it isn't set.
take_target_node() ->
    case application:get_env(riak_test, ?TRACING_KEY) of
        {ok, Node} ->
            application:unset_env(riak_test, ?TRACING_KEY),
            Node;
        _ ->
            erlang:node()
    end.

%% @hidden
tracing_status(true) -> "enabled";
tracing_status(false) -> "disabled".
