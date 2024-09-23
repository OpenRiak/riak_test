%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2022 Martin Sumner.
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
%% @doc Remote log manager/collector.
%%
%% Modules implementing this behavior are loaded on the remote node and
%% operate in the following sequence:
%% ```
%%      CallbackModule:connect(LogCollector, LogSourceID) ->
%%          configures the local logging service to forward logs via
%%          some implementation of ReceiveLocalLogEvent(...) below
%%
%%      ReceiveLocalLogEvent(...) ->
%%          gen_server:cast(LogCollector, {LogSourceID, FormattedLogLine})
%%
%%      CallbackModule:disconnect(LogCollector, LogSourceID) ->
%%          stops forwarding of log messages
%% '''
%% The `log_local/3' callback function is to be implemented as a facade over
%% the local logging service such that it behaves as:
%% ```
%%      CallbackModule:log_local(Level, Format, Args) ->
%%          LocalLogService:log(Level, Format, Args)
%% '''
%%
%% @end
-module(rt_logger).
-behaviour(gen_server).

-callback connect(
    LogCollector :: coll_ref(), LogSourceId :: term() ) -> ok | {error, term()}.
-callback disconnect(
    LogCollector :: coll_ref(), LogSourceId :: term() ) -> ok | {error, term()}.
-callback log_local(
    Level :: logger:level(), Format :: io:format(), Args :: list() ) -> ok.

%% riak_test API
-export([
    get_logs/1,
    log_to_node/4,
    plugin_logger/1,
    unplug_logger/1,

    %% application-ish interface
    start/0,
    stop/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-export_type([
    coll_ref/0,
    src_id/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-opaque coll_ref() :: pid() | {atom(), node()}.
-opaque src_id() :: node().

-ifdef(HIDE_FROM_EDOC).
-record(nrec, {
    conn    = false :: boolean(),
    mod     :: module(),
    logs    = [] :: rtt:log_lines()
}).
-type state() :: #{
    coll := coll_ref(),
    node := node(),
    v32 := rtt:vsn_rec(),
    node() => #nrec{}
}.
-endif. % HIDE_FROM_EDOC

-define(SERVER, ?MODULE).

%% ===================================================================
%% riak_test API
%% ===================================================================

-spec plugin_logger(Node :: node()) -> ok | rtt:std_error().
plugin_logger(Node) ->
    gen_server:call(?SERVER, {connect, Node}).

-spec unplug_logger(Node :: node()) -> ok.
unplug_logger(Node) ->
    gen_server:call(?SERVER, {disconnect, Node}).

-spec get_logs(Node :: node()) -> rtt:log_lines().
get_logs(Node) ->
    gen_server:call(?SERVER, {get_logs, Node}).

-spec log_to_node(
    Node :: node(),
    Level :: logger:level(),
    Format :: io:format(),
    Args :: list() )
        -> ok.
log_to_node(Node, Level, Format, Args) ->
    gen_server:call(?SERVER, {log_to_node, Node, Level, Format, Args}).

-spec start() -> ok | rtt:std_error().
%% @private
start() ->
    case gen_server:start({local, ?SERVER}, ?MODULE, [], []) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        Error ->
            Error
    end.

-spec stop() -> ok.
%% @private
stop() ->
    case erlang:whereis(?SERVER) of
        undefined ->
            ok;
        _Pid ->
            gen_server:stop(?SERVER)
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init(Ignored :: term()) -> {ok, state()}.
%% @private
init(_) ->
    {ok, #{
        coll => erlang:self(),
        node => erlang:node(),
        v32 => rt_vsn:new_version([3, 2, 0])
    }}.

-spec handle_call(Msg :: term(), From :: {pid(), term()}, State :: state())
        -> {reply, term(), state()}.
%% @private
handle_call({log_to_node, Node, Level, Format, Args}, _From, StateIn) ->
    case log_to_node(Node, Level, Format, Args, StateIn) of
        {ok, State} ->
            {reply, ok, State};
        Error ->
            {reply, Error, StateIn}
    end;
handle_call({get_logs, Node}, _From, State) ->
    case State of
        #{Node := #nrec{logs = [_|_] = Logs}} ->
            {reply, lists:reverse(Logs), State};
        _ ->
            {reply, [], State}
    end;
handle_call({connect, Node}, _From, State) ->
    %% We ALWAYS try to connect, regardless of the #nrec.conn flag, because
    %% we have no way of knowing whether the node has been restarted.
    case connect_node(Node, State) of
        {ok, StateOut} ->
            {reply, ok, StateOut};
        Error ->
            {reply, Error, State}
    end;
handle_call({disconnect, Node}, _From, State) ->
    %% Unlike connecting, we ONLY disconnect if we've previously connected.
    case State of
        #{coll := Collector, Node := #nrec{conn = true, mod = Mod} = NRec} ->
            _ = rpc:cast(Node, Mod, disconnect, [Collector, Node]),
            {reply, ok, State#{Node => NRec#nrec{conn = false}}};
        _ ->
            {reply, ok, State}
    end;
handle_call(Msg, From, State) ->
    ?LOG_NOTICE("Ignoring ~0p from ~0p", [Msg, From]),
    {reply, not_implemented, State}.

-spec handle_cast(Msg :: term(), State :: state()) -> {noreply, state()}.
%% @private
handle_cast({Node, LogLine}, State) ->
    case State of
        #{Node := #nrec{logs = Logs} = NRec} ->
            {noreply, State#{Node => NRec#nrec{logs = [LogLine | Logs]}}};
        _ ->
            ?LOG_WARNING(
                "Ignoring forwarded log from ~0p: ~s", [Node, LogLine]),
            {noreply, State}
    end;
handle_cast(Msg, State) ->
    ?LOG_NOTICE("Ignoring ~0p", [Msg]),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: state()) -> boolean().
%% @private
terminate(_Reason, #{coll := Collector} = State) ->
    FoldFun = fun
        (Node, #nrec{conn = true, mod = Mod}, _Ret) ->
            rpc:cast(Node, Mod, disconnect, [Collector, Node]);
        (_Key, _Val, Ret) ->
            Ret
    end,
    maps:fold(FoldFun, false, State).

%% ===================================================================
%% Internal
%% ===================================================================

-spec connect_node(Node :: node(), State :: state())
        -> {ok, state()} | rtt:std_error().
connect_node(Node, StateIn) ->
    case get_nrec(Node, StateIn) of
        {#nrec{} = NRec, State} ->
            Collector = maps:get(coll, State),
            case rpc_call(Node, NRec, connect, [Collector, Node]) of
                ok ->
                    {ok, State#{Node => NRec#nrec{conn = true}}};
                CallErr ->
                    CallErr
            end;
        Error ->
            Error
    end.

-spec log_to_node(
    Node :: node(),
    Level :: logger:level(),
    Format :: io:format(),
    Args :: list(),
    State :: state() )
        -> {ok, state()} | rtt:std_error().
log_to_node(Node, Level, Format, Args, StateIn) ->
    case get_nrec(Node, StateIn) of
        {#nrec{} = NRec, State} ->
            case rpc_call(Node, NRec, log_local, [Level, Format, Args]) of
                ok ->
                    {ok, State};
                CallErr ->
                    CallErr
            end;
        Error ->
            Error
    end.

-spec get_nrec(Node :: node(), State :: state())
        -> {#nrec{}, state()} | rtt:std_error().
get_nrec(Node, State) ->
    case State of
        #{Node := NR} ->
            {NR, State};
        _ ->
            case new_nrec(Node, State) of
                #nrec{} = NRec ->
                    {NRec, State#{Node => NRec}};
                Error ->
                    Error
            end
    end.

-spec new_nrec(Node :: node(), State :: state())
        -> #nrec{} | rtt:std_error().
new_nrec(Node, #{v32 := V32}) ->
    case node_vsn(Node) of
        {rtv, _} = VsnRec ->
            case rt_vsn:compare_versions(VsnRec, V32) of
                Old when Old < 0 ->
                    #nrec{mod = rt_remote_lager};
                _ ->
                    #nrec{mod = rt_remote_logger}
            end;
        Error ->
            Error
    end.

-spec node_vsn(Node :: node()) -> rtt:vsn_rec() | rtt:std_error().
node_vsn(Node) ->
    case rt:get_vsn_rec(rt:get_node_version(Node)) of
        {rtv, _} = VR ->
            VR;
        _ ->
            case rpc:call(Node, release_handler, which_releases, []) of
                [{_Name, _Vsn, _Apps, _Status} | _] = R ->
                    VList = [{S, V} || {_N, V, _A, S} <- R],
                    %% Returned list MUST contain at least 'permanent'
                    rt_vsn:parse_version(
                        proplists:get_value(current, VList,
                            proplists:get_value(permanent, VList)));
                {badrpc, NodeDown} ->
                    %% The only alternative is 'nodedown'
                    {error, NodeDown}
            end
    end.

-spec rpc_call(Node :: node(), NRec :: #nrec{}, Func :: atom(), Args :: list())
        -> term() | rtt:std_error().
rpc_call(Node, #nrec{mod = Mod}, Func, Args) ->
    case rpc:call(Node, Mod, Func, Args) of
        {badrpc, {'EXIT', {undef, _}}} ->
            {Mod, Bin, FN} = code:get_object_code(Mod),
            case rpc:call(Node, code, load_binary, [Mod, FN, Bin]) of
                {module, Mod} ->
                    rpc:call(Node, Mod, Func, Args);
                {badrpc, RpcLoad} ->
                    {error, RpcLoad};
                LoadErr ->
                    LoadErr
            end;
        {badrpc, RpcCall} ->
            {error, RpcCall};
        CallRes ->
            CallRes
    end.
