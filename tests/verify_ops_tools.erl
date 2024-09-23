%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Martin Sumner.
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
-module(verify_ops_tools).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Very basic test to confirm that redbug and recon are loaded and available
%% to support troubleshooting

confirm() ->
    ?LOG_INFO("Spinning up test nodes"),
    Config = [{riak_core, [{ring_creation_size, 8}]},
                {riak_kv, [{anti_entropy, {off, []}}]}],

    [RootNode | _RestNodes] = rt:build_cluster(2, Config),
    rt:wait_for_service(RootNode, riak_kv),

    ?LOG_INFO("Calling redbug and recon - are they there?"),
    ?assertMatch(ok, rpc:call(RootNode, redbug, help, [])),
    ?LOG_INFO("Redbug present"),
    ?assertMatch(2,
        length(rpc:call(RootNode, recon, node_stats_list, [2, 2]))),
    ?LOG_INFO("Recon present"),

    pass.
