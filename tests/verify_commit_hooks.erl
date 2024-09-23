%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.
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
-module(verify_commit_hooks).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    [Node] = rt:deploy_nodes(1),
    ?LOG_INFO("Loading the hooks module into ~0p", [Node]),
    rt:load_modules_on_nodes([hooks], [Node]),

    ?LOG_INFO("Setting pid of test (~0p) in application environment of ~0p for postcommit hook", [self(), Node]),
    ?assertEqual(ok, rpc:call(Node, application, set_env, [riak_test, test_pid, self()])),

    ?LOG_INFO("Installing commit hooks on ~0p", [Node]),
    ?assertEqual(ok, rpc:call(Node, hooks, set_hooks, [])),

    ?LOG_INFO("Checking precommit atom failure reason."),
    HTTP = rt:httpc(Node),
    ?assertMatch({error, {ok, "500", _, _}},
                 rt:httpc_write(HTTP, <<"failatom">>, <<"key">>, <<"value">>)),

    ?LOG_INFO("Checking Bug 1145 - string failure reason"),
    ?assertMatch({error, {ok, "403", _, _}},
                 rt:httpc_write(HTTP, <<"failstr">>, <<"key">>, <<"value">>)),

    ?LOG_INFO("Checking Bug 1145 - binary failure reason"),
    ?assertMatch({error, {ok, "403", _, _}},
                 rt:httpc_write(HTTP, <<"failbin">>, <<"key">>, <<"value">>)),

    ?LOG_INFO("Checking that bucket without commit hooks passes."),
    ?assertEqual(ok, rt:httpc_write(HTTP, <<"fail">>, <<"key">>, <<"value">>)),

    ?LOG_INFO("Checking that bucket with passing precommit passes."),
    ?assertEqual(ok, rt:httpc_write(HTTP, <<"failkey">>, <<"key">>, <<"value">>)),

    ?LOG_INFO("Checking that bucket with failing precommit fails."),
    ?assertMatch({error, {ok, "403", _, _}},
                 rt:httpc_write(HTTP, <<"failkey">>, <<"fail">>, <<"value">>)),

    ?LOG_INFO("Checking fix for BZ1244 - riak_kv_wm_object makes call to riak_client:get/3 with invalid type for key"),
    %% riak_kv_wm_object:ensure_doc will return {error, not_found}, leading to 404.
    %% see https://github.com/basho/riak_kv/pull/237 for details of the fix.
    ?assertMatch({error, {ok, "404", _, _}},
                 rt:httpc_write(HTTP, <<"bz1244bucket">>, undefined, <<"value">>)),

    ?LOG_INFO("Checking that postcommit fires."),
    ?assertMatch(ok, rt:httpc_write(HTTP, <<"postcommit">>, <<"key">>, <<"value">>)),

    receive
        {wrote, _Bucket, _Key}=Msg ->
            ?assertEqual({wrote, <<"postcommit">>, <<"key">>}, Msg),
            pass
    after 2000
        ->
            ?LOG_ERROR("Postcommit did not send a message within 2 seconds!"),
            ?assert(false)
    end.
