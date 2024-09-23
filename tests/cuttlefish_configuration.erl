%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(cuttlefish_configuration).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("stdlib/include/assert.hrl").

confirm() ->

    CuttlefishConf = [
        {"ring_size", "8"},
        {"leveldb.sync_on_write", "on"}
    ],

    [Node] = rt:deploy_nodes(1, {cuttlefish, CuttlefishConf}),
    {ok, RingSize} = rt:rpc_get_env(Node, [{riak_core, ring_creation_size}]),
    ?assertEqual(8, RingSize),

    %% test leveldb sync typo
    {ok, LevelDBSync} = rt:rpc_get_env(Node, [{eleveldb, sync}]),
    ?assertEqual(true, LevelDBSync),

    pass.
