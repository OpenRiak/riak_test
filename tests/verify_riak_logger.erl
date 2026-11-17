%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(verify_riak_logger).
-deprecated(module).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(UNIX_RW_R__R__, 8#100644).

confirm() ->
    ?LOG_INFO("Staring a node"),
    Nodes = [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),

    ?LOG_INFO("Stopping and restarting that node"),
    rt:stop(Node),

    rt:start(Node),
    ?LOG_INFO("Checking for log files"),

    HConfigs = rpc:call(Node, logger, get_handler_config, []),
    ?assert(erlang:is_list(HConfigs)),
    ?LOG_DEBUG("Log handlers:~n~p", [HConfigs]),

    Files = handler_files(HConfigs, []),

    ?LOG_INFO("Checking files on ~0p: ~0p", [Node, Files]),
    FileRecs = lists:map(
        fun(File) ->
            case rpc:call(Node, filelib, is_file, [File]) of
                true ->
                    case rpc:call(Node, file, read_file_info, [File]) of
                        {ok, FI} ->
                            {FI, File};
                        FIError ->
                            {FIError, File}
                    end;
                Nope ->
                    {Nope, File}
            end
        end, Files),
    %% match on the tuple to get the filename in any mismatch
    lists:foreach(
        fun(FileRec) ->
            ?assertMatch({#file_info{}, _}, FileRec)
        end, FileRecs),
    lists:foreach(
        fun({#file_info{mode = M}, FN}) ->
            ?assertMatch({?UNIX_RW_R__R__, _}, {?UNIX_RW_R__R__ band M, FN})
        end, FileRecs),

    pass.

handler_files([#{config := #{file := File}} | HConfigs], Files) ->
    handler_files(HConfigs, [File | Files]);
handler_files([_NotFileHandler | HConfigs], Files) ->
    handler_files(HConfigs, Files);
handler_files([], Files) ->
    Files.
