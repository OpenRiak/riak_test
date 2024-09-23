%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.
%% Copyright (c) 2022 Workday, Inc.
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
-module(verify_membackend).
-behavior(riak_test).

-export([confirm/0]).

%% invoked by name
-export([
    combo/1,
    max_memory/1,
    ttl/1
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% uncomment to check consistent puts
% -define(CHECK_CONSISTENT_PUTS, true).

-define(BUCKET, <<"ttl_test">>).

-ifdef(CHECK_CONSISTENT_PUTS).
-define(CHECK_PUT_CONSISTENT(Node), ok = check_put_consistent(Node)).
-else.
-define(CHECK_PUT_CONSISTENT(Node), ok).
-endif. % CHECK_CONSISTENT_PUTS

confirm() ->
    Tests = [ttl, max_memory, combo],
    [Res1, Res2] =
        [begin
             ?LOG_INFO("testing mode ~0p", [Mode]),
             put(mode, Mode),
             [begin
                  ?LOG_INFO("testing setting ~0p", [Test]),
                  ?MODULE:Test(Mode)
              end
              || Test <- Tests]
         end
         || Mode <- [regular, multi]],
    Res = Res1 ++ Res2,
    [ok] = lists:usort(Res),
    pass.


ttl(Mode) ->
    Conf = mkconf(ttl, Mode),
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    ok = check_leave_and_expiry(NodeA, NodeB),

    rt:clean_cluster([NodeA]),
    ok.

max_memory(Mode) ->
    Conf = mkconf(max_memory, Mode),
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    rt:join(NodeB, NodeA),

    ok = check_put_delete(NodeA),

    ?CHECK_PUT_CONSISTENT(NodeA),

    ok = check_eviction(NodeA),

    rt:clean_cluster([NodeA, NodeB]),

    ok.

combo(Mode) ->
    Conf = mkconf(combo, Mode),

    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    ok = check_leave_and_expiry(NodeA, NodeB),

    %% Make sure that expiry is updating used_memory correctly
    Pid = get_remote_vnode_pid(NodeA),
    {0, _} = get_used_space(NodeA, Pid),

    ok = check_put_delete(NodeA),

    ?CHECK_PUT_CONSISTENT(NodeA),

    ok = check_eviction(NodeA),

    rt:clean_cluster([NodeA]),

    ok.


check_leave_and_expiry(NodeA, NodeB) ->
    ?assertMatch([], rt:systest_write(NodeB, 1, 100, ?BUCKET, 2)),
    ?assertMatch([], rt:systest_read(NodeB, 1, 100, ?BUCKET, 2)),

    rt:join(NodeB, NodeA),

    ok = rt:wait_until_nodes_ready([NodeA, NodeB]),
    rt:wait_until_no_pending_changes([NodeA, NodeB]),

    rt:leave(NodeB),
    rt:wait_until_unpingable(NodeB),

    ?assertMatch([], rt:systest_read(NodeA, 1, 100, ?BUCKET, 2)),

    ?LOG_INFO("waiting for keys to expire"),
    timer:sleep(timer:seconds(210)),

    _ = rt:systest_read(NodeA, 1, 100, ?BUCKET, 2),
    timer:sleep(timer:seconds(5)),
    Res = rt:systest_read(NodeA, 1, 100, ?BUCKET, 2),

    ?assertMatch(100, length(Res)),
    ok.

check_eviction(Node) ->
    ?LOG_INFO("checking that values are evicted when memory limit "
               "is exceeded"),
    Size = 20000 * 8,
    Val = <<0:Size>>,

    ?assertMatch([], rt:systest_write(Node, 1, 500, ?BUCKET, 2, Val)),

    Res = length(rt:systest_read(Node, 1, 100, ?BUCKET, 2, Val)),

    %% this is a wider range than I'd like but the final outcome is
    %% somewhat hard to predict.  Just trying to verify that some
    %% memory limit somewhere is being honored and that values are
    %% being evicted.
    ?assertMatch(100, Res), % Or else memory_reclamation_issue

    {ok, C} = riak:client_connect(Node),

    [begin
         riak_client:delete(?BUCKET, <<N:32/integer>>, C),
         timer:sleep(100)
     end
     || N <- lists:seq(1, 500)],

    %% make sure all deletes propagate?
    timer:sleep(timer:seconds(10)),
    ok.

check_put_delete(Node) ->
    ?LOG_INFO("checking that used mem is reclaimed on delete"),
    Pid = get_remote_vnode_pid(Node),

    {MemBaseline, PutSize, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    ok = riak_client:delete(?BUCKET, <<Key:32/integer>>, C),

    timer:sleep(timer:seconds(5)),

    {Mem, _PutSize} = get_used_space(Node, Pid),

    %% The size of the encoded Riak Object varies a bit based on
    %% the included metadata, so we consult the riak_kv_memory_backend
    %% to determine the actual size of the object written
    ?assertMatch(PutSize, MemBaseline - Mem),
    ok.

-ifdef(CHECK_CONSISTENT_PUTS).
check_put_consistent(Node) ->
    ?LOG_INFO("checking that used mem doesn't change on re-put"),
    Pid = get_remote_vnode_pid(Node),

    {MemBaseline, _PutSize, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    %% Write a slightly larger object than before
    ok = riak_client:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>), C),

    {ok, _} = riak_client:get(?BUCKET, <<Key:32/integer>>, C),

    timer:sleep(timer:seconds(2)),

    {Mem, _} = get_used_space(Node, Pid),

    case abs(Mem - MemBaseline) < 3 of
        true ->
            ok;
        _ ->
            %% funky way to capture the values
            ?assertEqual(consistency_failure, {Mem, MemBaseline})
    end.
-endif. % CHECK_CONSISTENT_PUTS

%% @doc Keep putting objects until the memory used changes
%% Also return the size of the object after it's been encoded
%% and send to the memory backend.
-spec(put_until_changed(pid(), node(), term()) -> {integer(), integer(), term()}).
put_until_changed(Pid, Node, Key) ->
    {ok, C} = riak:client_connect(Node),
    {UsedSpace, _} = get_used_space(Node, Pid),

    riak_client:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>), C),

    timer:sleep(500),

    {UsedSpace1, PutObjSize} = get_used_space(Node, Pid),
    case UsedSpace < UsedSpace1 of
        true ->
            {UsedSpace1, PutObjSize, Key};
        _ ->
            put_until_changed(Pid, Node, Key+1)
    end.

mkconf(Test, Mode) ->
    MembConfig = case Test of
        ttl ->
            [{ttl, 200}];
        max_memory ->
            [{max_memory, 1}];
        combo ->
            [{max_memory, 1},
                {ttl, 200}]
    end,
    case Mode of
        regular ->
            %% only memory supports TTL
            rt:set_backend(memory),
            [
                {riak_core, [
                    {ring_creation_size, 4}
                ]},
                {riak_kv, [
                    {anti_entropy, {off, []}},
                    {delete_mode, immediate},
                    {memory_backend, MembConfig}
                ]}
            ];
        multi ->
            rt:set_backend(multi),
            [
                {riak_core, [
                    {ring_creation_size, 4}
                ]},
                {riak_kv, [
                    {anti_entropy, {off, []}},
                    {delete_mode, immediate},
                    {multi_backend_default, <<"memb">>},
                    {multi_backend, [
                        {<<"memb">>, riak_kv_memory_backend, MembConfig}
                    ]}
                ]}
            ]
    end.

get_remote_vnode_pid(Node) ->
    [{_,_,VNode}|_] = rpc:call(Node, riak_core_vnode_manager,
                               all_vnodes, [riak_kv_vnode]),
    VNode.

%% @doc Interrogate the VNode to determine the memory used
-spec(get_used_space(node(), pid()) -> {integer(), integer()}).
get_used_space(Node, VNodePid) ->
    {_Mod, State} = riak_core_vnode:get_modstate(VNodePid),
    {Mod, ModState} = riak_kv_vnode:get_modstate(State),
    Status = case Mod of
        riak_kv_memory_backend ->
            erpc:call(
                Node,
                riak_kv_memory_backend,
                status,
                [ModState]);
        riak_kv_multi_backend ->
            [{_Name, Stat}] =
                erpc:call(
                    Node,
                    riak_kv_multi_backend,
                    status,
                    [ModState]
                ),
            Stat;
        _Else ->
            ?LOG_ERROR("didn't understand backend ~0p", [Mod]),
            throw(boom)
    end,
    Mem = proplists:get_value(used_memory, Status),
    PutObjSize = proplists:get_value(put_obj_size, Status),
    ?LOG_INFO("got ~0p used memory, ~0p put object size", [Mem, PutObjSize]),
    {Mem, PutObjSize}.
