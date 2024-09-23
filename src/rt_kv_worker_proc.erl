%% -------------------------------------------------------------------
%%
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
%% @doc
%% This module defines a named process (rt_kv_worker_proc), which is designed to work
%% in tandem with the riak_kv_worker intercept.
%%
%% Once started, this process will communicate with the riak_kv_worker, and will
%% be informed of when a worker starts and stops its work.  Users can install
%% callbacks into this process, which can maintain state information at the
%% start and stop points, and test various conditions during or after this process
%% has run.
%% @end
-module(rt_kv_worker_proc).

-export([
    add_intercept/1,
    start_link/0,
    start_link/1,
    stop/0,
    stop/1
]).

%% Spawned process entry
-export([
    start_loop/1
]).

-export_type([
    accum/0,
    node_pid/0,
    result/0,
    started_callback/0,
    completed_callback/0,
    opts/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SVC_NAME,           ?MODULE).
-define(SVC_MSG(Content),   {?SVC_NAME, Content}).
-define(DEFAULT_TIMEOUT,    60000).

-type accum()               :: term().
-type node_pid()            :: {node(), pid()}.
-type result()              :: ok | {error, term()}.
-type started_callback()    :: fun((node_pid(), accum()) -> accum()).
-type completed_callback()  :: fun((node_pid(), accum()) -> {result(), accum()}).
-type opts()                :: list(
    {started_callback,          started_callback()} |
    {completed_callback,        completed_callback()} |
    {num_expected_completions,  pos_integer()} |
    {accum,                     accum()} |
    {timeout,                   rtt:millisecs()}
).

-record(state, {
    rt_proc                     :: pid(),
    started_callback            :: started_callback(),
    completed_callback          :: completed_callback(),
    num_completions             :: non_neg_integer(),
    num_expected_completions    :: pos_integer() | undefined,
    current_work                :: sets:set(),
    accum                       :: accum(),
    timeout                     :: rtt:millisecs()
}).

%% @doc Add the requisite riak_kv_worker intercepts to the selected node/s.
-spec add_intercept(node() | [node()]) -> ok | [ok].
add_intercept(Node) when is_atom(Node) ->
    ?LOG_INFO("Adding handle_work_handoff_intercept to node ~0p", [Node]),
    rt_intercept:add(
        Node, {
            riak_kv_worker,
            [{{handle_work, 3}, handle_work_handoff_intercept}]
        }
    );
add_intercept(Nodes) ->
    [add_intercept(Node) || Node <- Nodes].

%% @doc Start the rt_kv_worker_proc
%% @equiv start_link([])
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start the rt_kv_worker_proc
%% @todo doc options
-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    StartedCallback = proplists:get_value(started_callback, Opts, fun no_op_started/2),
    CompletedCallback = proplists:get_value(completed_callback, Opts, fun no_op_completed/2),
    NumExpectedCompletions = proplists:get_value(num_expected_completions, Opts, undefined),
    Accum = proplists:get_value(accum, Opts, undefined),
    Timeout = case proplists:get_value(timeout, Opts) of
        undefined ->
            rt_config:get(rt_max_wait_time);
        Val ->
            Val
    end,
    RTProc = erlang:self(),
    State = #state{
        rt_proc = RTProc,
        started_callback = StartedCallback,
        completed_callback = CompletedCallback,
        num_completions = 0,
        num_expected_completions = NumExpectedCompletions,
        current_work = sets:new(),
        accum = Accum,
        timeout = Timeout
    },
    Pid = erlang:spawn_link(?MODULE, start_loop, [State]),
    receive
        {ok, Pid} = Result ->
            yes = global:register_name(?SVC_NAME, Pid),
            Result
    after
        ?DEFAULT_TIMEOUT ->
            erlang:exit(Pid, kill),
            {error, timeout}
    end.

%% @doc Stop the rt_kv_worker_proc and return the current app state
%% @equiv stop(60000)
-spec stop() -> accum() | {error, term()}.
stop() ->
    stop(?DEFAULT_TIMEOUT).

%% @doc Stop the rt_kv_worker_proc and return the current app state
-spec stop(Timeout :: rtt:millisecs()) -> accum() | {error, term()}.
stop(Timeout) ->
    _ = global:send(?SVC_NAME, ?SVC_MSG(stop)),
    receive
        ?SVC_MSG(Result) ->
            _ = global:unregister_name(?SVC_NAME),
            Result
    after
        Timeout ->
            {error, timeout}
    end.

%%
%% spawned process
%%

%% @hidden
start_loop(#state{rt_proc = RTProc} = State) ->
    RTProc ! {ok, erlang:self()},
    loop(State).

%% @hidden
loop(#state{
        rt_proc = RTProc,
        current_work = CurrentWork,
        started_callback = StartedCallback,
        completed_callback = CompletedCallback,
        num_completions = NumCompletions,
        num_expected_completions = NumExpectedCompletions,
        accum = Accum,
        timeout = Timeout
    } = State) ->
    receive
        {work_started, {Node, Ref}, Pid} ->
            Accum1 = StartedCallback({Node, Pid}, Accum),
            Pid ! {Ref, ok},
            loop(State#state{
                current_work = sets:add_element({Node, Pid}, CurrentWork),
                accum = Accum1
            });
        {work_completed, {Node, Ref}, Pid} ->
            NodePid = {Node, Pid},
            case sets:is_element(NodePid, CurrentWork) of
                true ->
                    {Result, Accum1} = CompletedCallback({Node, Pid}, Accum),
                    Pid ! {Ref, ok},
                    case Result of
                        ok ->
                            %% what to do next...
                            NewWork = sets:del_element(NodePid, CurrentWork),
                            case {NumCompletions + 1, sets:is_empty(NewWork)} of
                                {NumExpectedCompletions, true} ->
                                    exit_loop(RTProc, {ok, Accum1});
                                {NumExpectedCompletions, _} ->
                                    exit_loop(RTProc,
                                        {error, more_completions_than_expected});
                                _ ->
                                    loop(State#state{
                                        current_work = NewWork,
                                        num_completions = NumCompletions + 1,
                                        accum = Accum1}
                                    )
                            end;
                        _ ->
                            exit_loop(RTProc,
                                {error, {unexpected_result, Result}})
                    end;
                _ ->
                    exit_loop(RTProc, {error, {not_found, {Node, Pid}}})
            end;
        ?SVC_MSG(stop) ->
            exit_loop(RTProc, {ok, Accum})
    after
        Timeout ->
            exit_loop(RTProc, {error, timeout})
    end.

%% @hidden
exit_loop(Dest, Msg) ->
    Dest ! ?SVC_MSG(Msg),
    ok.

%%
%% default callbacks
%%

%% @hidden
-spec no_op_started(node_pid(), accum()) -> accum().
no_op_started(_NodePair, Accum) -> Accum.

%% @hidden
-spec no_op_completed(node_pid(), accum()) -> {result(), accum()}.
no_op_completed(_NodePair, Accum) -> {ok, Accum}.
