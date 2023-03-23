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
%% Confirm that bitcask is reporting expiry stats as configured.
%%
-module(verify_bitcask_expiry_stats).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(EXPIRED_KEYS, <<"bitcask_expiry_1s_expired_keys">>).
-define(EXPIRED_BYTES, <<"bitcask_expiry_1s_expired_bytes">>).

confirm() ->
    lager:info("Overriding backend set in configuration"),
    lager:info("Multi backend with default settings (rt) to be used"),

    Config = [
      {riak_kv, [
            {storage_backend, riak_kv_multi_backend},
            {multi_backend_default, <<"mybitcask">>},
            {multi_backend, [
               {<<"mybitcask">>, riak_kv_bitcask_backend, [
                         {expiry_secs, 1},
                         {expiry_grace_time, 3},
                         {data_root, "$(platform_data_dir)/bitcask_expiry_1s"}
                ]}
        ]}
      ]}],

    Node = hd(rt:deploy_nodes(1, Config)),
    Client =  rt:pbc(Node),
    Bucket = <<"some_bucket">>,
    Key = <<"key1">>,
    ?assertMatch(ok, put(Client, Bucket, Key, <<"value">>)),

    KeyExpiresFun = fun() ->
        get(Client, Bucket, Key) =:= {error, notfound}
    end,
    rt:wait_until(KeyExpiresFun, 3, 1000),
    Stats = rt:get_stats(Node),
    %% 3 reflects the number of partitions from which the key was removed.
    ?assertMatch({?EXPIRED_KEYS, 3}, lists:keyfind(?EXPIRED_KEYS, 1, Stats)),
    ?assertMatch({?EXPIRED_BYTES, Bytes} when Bytes > 0,
        lists:keyfind(?EXPIRED_BYTES, 1, Stats)),

    pass.


%% @private
put(Client, Bucket, Key, Value) ->
    RObj = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Client, RObj, [{w,1}]).

%% @private
get(Client, Bucket, Key) ->
    riakc_pb_socket:get(Client, Bucket, Key).
