#!/usr/bin/env escript
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2022-2023 Workday, Inc.
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
%% Tests for the presence of the bug where Riak opens the PB port before it is
%% ready to accept requests in the target node.
%% The script performs enough retries that it can safely be started *before*
%% starting Riak - the real-world scenario we want to simulate.
%%
%% The script reports success or failure via its return code - printed output
%% is informative only. Statistics regarding how many times certain events
%% occurred are reported upon script completion.
%%
%% The paths provided on the command line MUST include compiled ebin
%% directories for riak-erlang-client, aka riakc, and its dependencies.
%%
-module(pb_listen_wait_until_kv_ready).

-mode(compile).
-compile([
    debug_info,
    no_auto_import,
    warn_export_vars,
    warn_unused_import,
    warnings_as_errors
]).

-export([
    main/1,
    silence_crashes/2
]).

%% Default to dev1
-define(TGT_HOST,   "127.0.0.1").
-define(TGT_PORT,   10017).

-define(MAXTRIES,   9000).
-define(RETRY_MS,   20).

-define(B,  <<"groceries">>).
-define(K,  <<"mine">>).
-define(V,  <<"eggs & bacon">>).

-define(INC_STAT(K,S),  K => (maps:get(K, S, 0) + 1)).

main([]) ->
    io:format(standard_error,
        "Usage: ~s <{riakc|dep}-ebin-dir> ...~n",
        [filename:basename(escript:script_name())]),
    erlang:halt(2);
main(Paths) ->
    try
        ok = silence_crashes(),
        ok = code:add_pathsz(Paths),
        case connect_and_put() of
            {ok, Stats} ->
                io:format("SUCCESS with ~0p~n", [Stats]),
                erlang:halt(0);
            {Result, Stats} ->
                io:format(standard_error,
                    "FAILED ~0p with ~0p~n", [Result, Stats])
        end
    catch
        Class:Reason:Stack ->
            io:format(standard_error,
                "~0p : ~0p :~n~p~n", [Class, Reason, Stack])
    end,
    erlang:halt(1).

connect_and_put() ->
    Rec = riakc_obj:new(?B, ?K, ?V),
    case connect_and_put(Rec, #{iter => 1}) of
        {ok = R, Stats} ->
            {maps:fold(fun map_result/3, R, Stats), Stats};
        Failed ->
            Failed
    end.

connect_and_put(_Rec, #{iter := Try} = Stats, IncStat) when Try > ?MAXTRIES ->
    {timeout, Stats#{?INC_STAT(IncStat, Stats)}};
connect_and_put(Rec, #{iter := Try} = Stats, IncStat) ->
    timer:sleep(?RETRY_MS),
    connect_and_put(Rec, Stats#{iter => (Try + 1), ?INC_STAT(IncStat, Stats)}).

connect_and_put(Rec, Stats) ->
    try riakc_pb_socket:start(?TGT_HOST, ?TGT_PORT) of
        {ok, Pid} ->
            case riakc_pb_socket:put(Pid, Rec) of
                ok ->
                    io:format("Done PUT ~0p:~0p => ~0p~n", [
                        riakc_obj:bucket(Rec), riakc_obj:key(Rec),
                        riakc_obj:get_update_value(Rec)]),
                    case riakc_pb_socket:get(Pid, ?B, ?K) of
                        {ok, Out} ->
                            io:format("Done GET ~0p:~0p => ~0p~n", [
                                riakc_obj:bucket(Out), riakc_obj:key(Out),
                                riakc_obj:get_value(Out)]),
                            {ok, Stats};
                        GetErr ->
                            {get, Stats#{error => GetErr}}
                    end;
                PutErr ->
                    connect_and_put(Rec, Stats, PutErr)
            end;
        {error, {tcp, econnrefused}} ->
            connect_and_put(Rec, Stats, refused);
        Unknown ->
            connect_and_put(Rec, Stats, Unknown)
    catch
        Class:Reason ->
            io:format(standard_error, "~b: ~0p : ~0p~n", [?LINE, Class, Reason]),
            connect_and_put(Rec, Stats, except)
    end.

map_result(_Key, _Val, error = Result) ->
    Result;
map_result(iter, _Val, Result) ->
    Result;
map_result(refused, _Val, Result) ->
    Result;
map_result(except, _Val, Result) ->
    Result;
%% Any other key is an error
map_result(_Key, _Val, _Result) ->
    error.

silence_crashes() ->
    logger:add_primary_filter(?MODULE, {fun silence_crashes/2, ?MODULE}).

silence_crashes(#{msg := {report, #{label := {proc_lib, crash}}}}, ?MODULE) ->
    stop;
silence_crashes(_Map, _Param) ->
    % io:format("~b: ~p ~p.~n", [?LINE, _Param, _Map]),
    ignore.
