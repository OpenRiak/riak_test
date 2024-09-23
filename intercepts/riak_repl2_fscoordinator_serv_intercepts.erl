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
-module(riak_repl2_fscoordinator_serv_intercepts).

-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").
-define(M, riak_repl2_fscoordinator_serv_orig).

handle_protocol_msg_error(Msg, _) ->
    ?I_INFO("Intentionally crashing on ~p on node ~p", [Msg, node()]),
    erlang:error(intentional_intercept_error).
