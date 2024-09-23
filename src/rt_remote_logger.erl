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
%% @doc Logger handler to execute on a remote Riak 3.2+ node, installed
%% via RPC by `rt_logger'.
%%
%% Other `rt_remote_*' modules support prior Riak versions.
%%
%% @end
-module(rt_remote_logger).
-behavior(rt_logger).

% rt_logger callbacks
-export([
    connect/2,
    disconnect/2,
    log_local/3
]).

% Logger handler
-export([
    log/2
]).

-define(HANDLER, riak_test_logger).

%% ===================================================================
%% rt_logger callbacks
%% ===================================================================

-spec connect(
    LogCollector :: rt_logger:coll_ref(), LogSourceId :: rt_logger:src_id() )
        -> ok | {error, term()}.
connect(LogCollector, LogSourceId) ->
    %% Same as rt_logger:formatter(true, false) but with single_line => true
    %% TODO: Consider using the {Name, Node} coll_ref() so we can rpc to get it.
    Formatter = {logger_formatter, #{
        single_line => true, legacy_header => false,
        template => [
            time, " [", level, "] ", {pid, [pid, "@"], []},
            {mfa, [mfa, ":"], []}, {line, [line, ":"], []}, " ", msg ]
    }},
    HConfig = #{
        config => #{dest => LogCollector, from => LogSourceId},
        formatter => Formatter
    },
    logger:add_handler(?HANDLER, ?MODULE, HConfig).

-spec disconnect(
    LogCollector :: rt_logger:coll_ref(), LogSourceId :: rt_logger:src_id() )
        -> ok | {error, term()}.
disconnect(_LogCollector, _LogSourceId) ->
    logger:remove_handler(?HANDLER).

-spec log_local(
    Level :: logger:level(), Format :: io:format(), Args :: list() ) -> ok.
log_local(Level, Format, Args) ->
    logger:log(Level, Format, Args).

%% ===================================================================
%% Logger handler callback
%% ===================================================================

-spec log(Event :: logger:log_event(), Config :: logger:handler_config())
        -> ok.
%% @private Send the formatted log back to the riak_test node.
log(Event, #{config := #{dest := Collector, from := Source},
        formatter := {FModule, FConfig}}) ->
    gen_server:cast(Collector, {Source, FModule:format(Event, FConfig)}).
