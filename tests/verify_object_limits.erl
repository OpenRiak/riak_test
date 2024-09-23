%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% @doc Verifies Riak's warnings and caps for number of siblings
%%  and object size. Warnings end up in the logs, and hard caps can
%%  make requests fail.
-module(verify_object_limits).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"b">>).
-define(WARN_SIZE, 1000).
-define(MAX_SIZE, 10000).
-define(WARN_SIBLINGS,2).
-define(MAX_SIBLINGS,5).


confirm() ->
    [Node1] = rt:build_cluster(1, [{riak_kv, [
                        {ring_creation_size, 8},
                        {anti_entropy, {off,[]}},
                        {max_object_size, ?MAX_SIZE},
                        {warn_object_size, ?WARN_SIZE},
                        {max_siblings, ?MAX_SIBLINGS},
                        {warn_siblings, ?WARN_SIBLINGS}]}]),
    C = rt:pbc(Node1),

    %% Set up to grep logs to verify messages
    rt:setup_log_capture(Node1),

    % For the sibling test, we need the bucket to allow siblings
    ?LOG_INFO("Configuring bucket to allow siblings"),
    ?assertMatch(ok, riakc_pb_socket:set_bucket(C, ?BUCKET,
                                                [{allow_mult, true}])),
    verify_size_limits(C, Node1),
    verify_sibling_limits(C, Node1),
    ?LOG_NOTICE("Starting readrepair section of test"),
    verify_readrepair_ignore_max_size(C, Node1),
    verify_readrepair_ignore_max_sib(C, Node1),
    pass.

verify_size_limits(C, Node1) ->
    ?LOG_INFO("Verifying size limits"),
    Puts = [{1, ok},
            {10, ok},
            {50, ok},
            {?WARN_SIZE, warning},
            {?MAX_SIZE, error},
            {?MAX_SIZE*2, error}],
    lists:foreach(fun(SL) -> verify_size_limits(C, Node1, SL) end, Puts).

verify_size_limits(C, Node1, {N, X}) ->
    ?LOG_INFO("Checking put of size ~0p, expected ~0p", [N, X]),
    K = <<N:32/big-integer>>,
    V = <<0:(N)/integer-unit:8>>, % N zeroes bin
    O = riakc_obj:new(?BUCKET, K, V),
    % Verify behavior on write
    Res = riakc_pb_socket:put(C, O),
    ?LOG_INFO("Result : ~0p", [Res]),
    case X of
        ok ->
            ?assertMatch({N, ok}, {N, Res});
        error ->
            ?assertMatch({N, {error, _}}, {N, Res}),
            verify_size_write_error(Node1, K, N);
        warning ->
            verify_size_write_warning(Node1, K, N)
    end,
    % Now verify on read
    ?LOG_INFO("Now checking read of size ~0p, expected ~0p", [N, X]),
    ReadRes = riakc_pb_socket:get(C, ?BUCKET, K),
    case X of
        ok ->
            ?assertMatch({{ok, _}, N}, {ReadRes, N});
        warning ->
            ?assertMatch({{ok, _}, N}, {ReadRes, N}),
            verify_size_read_warning(Node1, K, N);
        error ->
            ?assertMatch({{error, _}, N}, {ReadRes, N})
    end.

verify_size_write_warning(Node, K, N) ->
    ?LOG_INFO("Looking for write warning for size ~0p", [N]),
    Pattern = io_lib:format("warning.*Writ.*~0p.*~0p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_read_warning(Node, K, N) ->
    ?LOG_INFO("Looking for read warning for size ~0p", [N]),
    Pattern = io_lib:format("warning.*Read.*~0p.*~0p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_write_error(Node, K, N) ->
    ?LOG_INFO("Looking for write error for size ~0p", [N]),
    Pattern = io_lib:format("error.*~0p.*~0p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_sibling_limits(C, Node1) ->
    K = <<"sibtest">>,
    O = riakc_obj:new(?BUCKET, K, <<"val">>),
    [?assertMatch(ok, riakc_pb_socket:put(C, O))
     || _ <- lists:seq(1, ?WARN_SIBLINGS+1)],
    P = io_lib:format("warning.*siblings.*~0p.*~0p.*(~0p)",
                      [?BUCKET, K, ?WARN_SIBLINGS+1]),
    Found = rt:expect_in_log(Node1, P),
    ?LOG_INFO("Looking for sibling warning: ~0p", [Found]),
    ?assertEqual(true, Found),
    % Generate error now
    [?assertMatch(ok, riakc_pb_socket:put(C, O))
     || _ <- lists:seq(?WARN_SIBLINGS+2, ?MAX_SIBLINGS)],
    Res = riakc_pb_socket:put(C, O),
    ?LOG_INFO("Result when too many siblings : ~0p", [Res]),
    ?assertMatch({error,_},  Res),
    ok.

verify_readrepair_ignore_max_size(C, Node1) ->
    % Add intercept to treat all vnode puts as readrepairs
    Intercept = {riak_kv_vnode, [{{put, 6}, put_as_readrepair},{{coord_put,6}, coord_put_as_readrepair}]},
    ok = rt_intercept:add(Node1, Intercept),
    % Do put with value greater than max size and confirm warning
    ?LOG_INFO("Checking readrepair put of size ~b, expecting ok result and log warning", [?MAX_SIZE*2]),
    K = <<"rrsizetest">>,
    V = <<0:(?MAX_SIZE*2)/integer-unit:8>>,
    O = riakc_obj:new(?BUCKET, K, V),
    ?assertMatch(ok, riakc_pb_socket:put(C, O)),
    verify_size_write_warning(Node1, K, ?MAX_SIZE*2),
    % Clean intercept
    ok = rt_intercept:clean(Node1, riak_kv_vnode),
    ok.

verify_readrepair_ignore_max_sib(C, Node1) ->
    ?LOG_INFO("Checking sibling warning on readrepair above max siblings=~b", [?MAX_SIBLINGS]),
    K = <<"rrsibtest">>,
    V = <<"sibtest">>,
    O = riakc_obj:new(?BUCKET, K, V),
    % Force sibling error
    [?assertMatch(ok, riakc_pb_socket:put(C, O))
     || _ <- lists:seq(1, ?MAX_SIBLINGS)],
    Res = riakc_pb_socket:put(C, O),
    ?LOG_INFO("Result when too many siblings : ~0p", [Res]),
    ?assertMatch({error,_},  Res),
    % Add intercept to spoof writes as readrepair
    Intercept = {riak_kv_vnode, [{{put, 6}, put_as_readrepair},{{coord_put,6}, coord_put_as_readrepair}]},
    ok = rt_intercept:add(Node1, Intercept),
    % Verify readrepair writes return ok and log warning
    ?LOG_INFO("Verifying succesful put above max_siblings with readrepair"),
    ?assertMatch(ok, riakc_pb_socket:put(C, O)),
    P = io_lib:format("warning.*siblings.*~0p.*~0p.*(~0p)",
                      [?BUCKET, K, ?MAX_SIBLINGS+1]),
    Found = rt:expect_in_log(Node1, P),
    ?LOG_INFO("Looking for sibling warning: ~0p", [Found]),
    ?assertEqual(true, Found),
    % Clean intercept
    ok = rt_intercept:clean(Node1, riak_kv_vnode),
    ok.



