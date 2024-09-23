%% -------------------------------------------------------------------
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
-module(verify_multibackend_head).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DEFAULT_RING_SIZE, 16).

-define(CONF,
        [{riak_kv,
          [
           {anti_entropy, {off, []}}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

confirm() ->
    ?LOG_INFO("Overriding backend set in configuration"),
    ?LOG_INFO("Multi backend with default settings (rt) to be used"),

    {BackendRef, HeadSupport} = get_backendref(),

    rt:set_backend(riak_kv_multi_backend),
    [Node1] = rt:deploy_nodes(1, ?CONF),

    Stats1 = get_stats(Node1),

    C = rt:httpc(Node1),
    PBC = rt:pbc(Node1),

    ?LOG_INFO("Setting up a bucket type to use backend and test"),
    Type = <<"backend">>,
    TypedBucket = <<"TB0">>,
    UnTypedBucket = <<"B0">>,
    BucketProps = [{backend, BackendRef}],
    rt:create_and_activate_bucket_type(Node1, Type, BucketProps),
    rt:wait_until_bucket_type_status(Type, active, [Node1]),
    rt:wait_until_bucket_props([Node1], {Type, TypedBucket}, BucketProps),
    rt:pbc_set_bucket_prop(PBC, UnTypedBucket, BucketProps),
    rt:wait_until_bucket_props([Node1], UnTypedBucket, BucketProps),
    ?LOG_INFO("Backend now setup for typed and untyped"),

    [rt:httpc_write(C, {Type, TypedBucket}, <<X>>, <<"sample_value">>)
        || X <- lists:seq(1, 3)],
    [rt:httpc_read(C, {Type, TypedBucket}, <<X>>)
        || X <- lists:seq(1, 3)],
    [rt:httpc_write(C, UnTypedBucket, <<X>>, <<"sample_value">>)
        || X <- lists:seq(4, 5)],
    [rt:httpc_read(C, UnTypedBucket, <<X>>)
        || X <- lists:seq(4, 5)],


    Stats2 = get_stats(Node1),

    ExpectedNodeStats =
        case HeadSupport of
            true ->
                [{<<"node_gets">>, 5},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 5},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 5},
                        % The five PUTS will require only HEADs
                    {<<"vnode_heads">>, 15},
                        % There is no reduction in the count of HEADs
                        % as HEADS before GETs
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 5},
                    {<<"vnode_heads_total">>, 15},
                    {<<"vnode_puts_total">>, 15}];
            false ->
                [{<<"node_gets">>, 5},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 5},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 15},
                    {<<"vnode_heads">>, 0},
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 15},
                    {<<"vnode_heads_total">>, 0},
                    {<<"vnode_puts_total">>, 15}]
        end,

    %% make sure the stats that were supposed to increment did
    verify_inc(Stats1, Stats2, ExpectedNodeStats),

    pass.


get_backendref() ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    ?LOG_INFO("Running with backend ~0p", [Backend]),
    {get_backendref(Backend), Backend == leveled}.

get_backendref(eleveldb) ->
    <<"eleveldb1">>;
get_backendref(bitcask) ->
    <<"bitcask1">>;
get_backendref(leveled) ->
    <<"leveled1">>;
get_backendref(memory) ->
    <<"memory1">>.


verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         ?LOG_INFO("~s: ~0p -> ~0p (expected ~0p)", [Key, Old, New, Old + Inc]),
         ?assertEqual(New, (Old + Inc))
     end || {Key, Inc} <- Keys].

get_stats(Node) ->
    rt:get_stats(Node, 2000).
