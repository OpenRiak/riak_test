%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_asis_put).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    %% 1. Deploy two nodes
    [Node1, Node2] = rt:deploy_nodes(2),
    %% 2. With PBC
    ?LOG_INFO("Put new object in ~0p via PBC.", [Node1]),
    PB1 = rt:pbc(Node1),
    PB2 = rt:pbc(Node2),
    Obj1 = riakc_obj:new(<<"verify_asis_put">>, <<"1">>, <<"test">>, "text/plain"),
    %%    a. put in node 1
    %%    b. fetch from node 1 for vclock
    {ok, Obj1a} = riakc_pb_socket:put(PB1, Obj1, [return_body]),
    %%    c. put asis in node 2
    %%    d. fetch from node 2, check vclock is same
    ?LOG_INFO("Put object asis in ~0p via PBC.", [Node2]),
    {ok, Obj1b} = riakc_pb_socket:put(PB2, Obj1a, [asis, return_body]),
    ?LOG_INFO("Check vclock equality after asis put (PBC)."),
    ?assert(riakc_obj:vclock(Obj1a) == riakc_obj:vclock(Obj1b)),

    %% 3. Repeat with HTTP, nodes reversed
    ?LOG_INFO("Put new object in ~0p via HTTP.", [Node2]),
    HTTP1 = rt:httpc(Node1),
    HTTP2 = rt:httpc(Node2),
    Obj2 = riakc_obj:new(<<"verify_asis_put">>, <<"2">>, <<"test">>, "text/plain"),
    %%    a. put in node 2
    %%    b. fetch from node 2 for vclock
    {ok, Obj2a} = rhc:put(HTTP2, Obj2, [return_body]),
    %%    c. put asis in node 1
    %%    d. fetch from node 1, check vclock is same
    ?LOG_INFO("Put object asis in ~0p via PBC.", [Node1]),
    {ok, Obj2b} = rhc:put(HTTP1, Obj2a, [asis, return_body]),
    ?LOG_INFO("Check vclock equality after asis put (HTTP)."),
    ?assert(riakc_obj:vclock(Obj2a) == riakc_obj:vclock(Obj2b)),

    pass.
