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
-module(overload).
-behavior(riak_test).

-export([confirm/0]).

%% MFA test invocations
-export([
    test_fsm_protection/2,
    test_no_overload_protection/2,
    test_vnode_protection/2
]).

%% RPC calls back into this module
-export([
    remote_suspend_and_overload/0,
    remote_suspend_vnode/1,
    remote_suspend_vnode_proxy/1,
    remote_vnode_gets_in_queue/1,
    remote_vnode_queues_empty/0
]).

%% Lots of unused functions, easier than commenting them out
-compile(nowarn_unused_function).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-cover_modules([riak_kv_vnode,
                riak_kv_ensemble_backend,
                riak_core_vnode_proxy]).

-define(NUM_REQUESTS, 200).
-define(THRESHOLD, 100).
-define(LIST_KEYS_RETRIES, 1000).
-define(GET_RETRIES, 1000).
-define(BUCKET, <<"test">>).
-define(VALUE, <<"overload_test_value">>).
-define(NORMAL_TYPE, <<"normal_type">>).
-define(CONSISTENT_TYPE, <<"consistent_type">>).
-define(WRITE_ONCE_TYPE, <<"write_once_type">>).
-define(NORMAL_BUCKET, {?NORMAL_TYPE, ?BUCKET}).
-define(CONSISTENT_BUCKET, {?CONSISTENT_TYPE, ?BUCKET}).
-define(WRITE_ONCE_BUCKET, {?WRITE_ONCE_TYPE, ?BUCKET}).

%% This record contains the default values for config settings if they were not set
%% in the advanced.config file - because setting something to `undefined` is not the same
%% as not setting it at all, we need to make sure to overwrite with defaults for each test,
%% not just set things back to `undefined`. Also, makes the tests re-orderable as they always
%% set everything they need, and don't depend on a previous test to make changes.
-record(config, {
    vnode_overload_threshold = 10000,
    vnode_check_interval = 5000,
    vnode_check_request_interval = 2500,
    fsm_limit=undefined}).

default_config() ->
    default_config(#config{}).

default_config(#config{
    vnode_overload_threshold=VnodeOverloadThreshold,
    vnode_check_interval = VnodeCheckInterval,
    vnode_check_request_interval = VnodeCheckRequestInterval,
    fsm_limit = FsmLimit
}) ->
    [{riak_core, [{ring_creation_size, 8},
        {default_bucket_props,
            [
                {n_val, 5},
                {allow_mult, true},
                {dvv_enabled, true}
            ]},
        {vnode_management_timer, 1000},
        {enable_health_checks, false},
        {enable_consensus, true},
        {vnode_overload_threshold, VnodeOverloadThreshold},
        {vnode_check_interval, VnodeCheckInterval},
        {vnode_check_request_interval, VnodeCheckRequestInterval}]},
        {riak_kv, [{fsm_limit, FsmLimit},
            {storage_backend, riak_kv_eleveldb_backend},
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_tick, 100},
            {anti_entropy, {on, []}},
            {anti_entropy_timeout, 5000}]},
        {riak_api, [{pb_backlog, 1024}]}].

confirm() ->
    [Node1 | _] = Nodes = setup(),

    ok = create_bucket_type(Nodes, ?NORMAL_TYPE, [{n_val, 3}]),
    ok = create_bucket_type(Nodes, ?CONSISTENT_TYPE, [{consistent, true}, {n_val, 5}]),
    ok = create_bucket_type(Nodes, ?WRITE_ONCE_TYPE, [{write_once, true}, {n_val, 1}]),

    Key = generate_key(),
    ?LOG_INFO("Generated overload test key ~0p", [Key]),

    NormalBKV = {?NORMAL_BUCKET, Key, ?VALUE},
    ConsistentBKV = {?CONSISTENT_BUCKET, Key, ?VALUE},
    WriteOnceBKV = {?WRITE_ONCE_BUCKET, Key, ?VALUE},

    write_once(Node1, NormalBKV),
    write_once(Node1, ConsistentBKV),
    write_once(Node1, WriteOnceBKV),

    PBC = rt:pbc(Node1),
    {ok, _NormalResult} = riakc_pb_socket:get(PBC, ?NORMAL_BUCKET, Key),
    {ok, _ConsistentResult} = riakc_pb_socket:get(PBC, ?CONSISTENT_BUCKET, Key),
    {ok, _WriteOnceResult} = riakc_pb_socket:get(PBC, ?WRITE_ONCE_BUCKET, Key),

    Tests = [test_no_overload_protection,
             test_vnode_protection,
             test_fsm_protection],

    [begin
         ?LOG_INFO("Starting Test ~0p for ~0p", [Test, BKV]),
         ok = erlang:apply(?MODULE, Test, [Nodes, BKV])
     end || Test <- Tests,
            BKV <- [NormalBKV,
                    ConsistentBKV,
                    WriteOnceBKV]],

    %% Test cover queries doesn't depend on bucket/keyvalue, just run it once
    test_cover_queries_overload(Nodes),
    pass.

generate_key() ->
    N = rand:uniform(500),

    Part1 = <<"overload_test_key_">>,
    Part2 = integer_to_binary(N),

    <<Part1/binary, Part2/binary>>.

setup() ->
    ensemble_util:build_cluster(5, default_config(), 5).

test_no_overload_protection(_Nodes, {?CONSISTENT_BUCKET, _, _}) ->
    ok;
test_no_overload_protection(Nodes, BKV) ->
    ?LOG_INFO("Setting default configuration for no overload protection test."),
    rt:pmap(fun(Node) ->
        rt:update_app_config(Node, default_config())
    end, Nodes),
    ?LOG_INFO("Testing with no overload protection"),
    ProcFun = build_predicate_eq(test_no_overload_protection, ?NUM_REQUESTS,
                                  "ProcFun", "Procs"),
    QueueFun = build_predicate_eq(test_no_overload_protection, ?NUM_REQUESTS,
                                  "QueueFun", "Queue Size"),
    verify_test_results(run_test(Nodes, BKV), BKV, ProcFun, QueueFun).

verify_test_results({_NumProcs, QueueLen}, {?CONSISTENT_BUCKET, _, _}, _ProcFun, QueueFun) ->
    ?assert(QueueFun(QueueLen));
verify_test_results({NumProcs, QueueLen}, _BKV, ProcFun, QueueFun) ->
    ?assert(ProcFun(NumProcs)),
    ?assert(QueueFun(QueueLen)).

test_vnode_protection(Nodes, BKV) ->
    %% Setting check_interval to one ensures that process_info is called
    %% to check the queue length on each vnode send.
    %% This allows us to artificially raise vnode queue lengths with dummy
    %% messages instead of having to go through the vnode path for coverage
    %% query overload testing.
    ?LOG_INFO("Testing with vnode queue protection enabled"),
    ?LOG_INFO("Setting vnode overload threshold to ~b", [?THRESHOLD]),
    ?LOG_INFO("Setting vnode check interval to 1"),
    Config = default_config(#config{vnode_overload_threshold=?THRESHOLD, vnode_check_interval=1}),
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),
    ProcFun = build_predicate_lt(test_vnode_protection, (?NUM_REQUESTS+1), "ProcFun", "Procs"),
    QueueFun = build_predicate_lte(test_vnode_protection, (?THRESHOLD+1), "QueueFun", "QueueSize"),
    verify_test_results(run_test(Nodes, BKV), BKV, ProcFun, QueueFun),

    [Node1 | _] = Nodes,
    CheckInterval = ?THRESHOLD div 2,
    Dropped = read_until_success(Node1),
    ?LOG_INFO("Unnecessary dropped requests: ~b", [Dropped]),
    ?assert(Dropped =< CheckInterval),

    Victim = get_victim(Node1, BKV),

    ?LOG_INFO("Suspending vnode proxy for ~0p", [Victim]),
    Pid = suspend_vnode_proxy(Victim),
    ProcFun2 = build_predicate_gte("test_vnode_protection after suspend",
                                   (?NUM_REQUESTS), "ProcFun", "Procs"),
    QueueFun2 = build_predicate_lte("test_vnode_protection after suspend",
                                   (?THRESHOLD+1), "QueueFun", "QueueSize"),
    verify_test_results(run_test(Nodes, BKV), BKV, ProcFun2, QueueFun2),
    Pid ! resume,
    ok.


%% Don't check consistent gets, as they don't use the FSM
test_fsm_protection(_, {?CONSISTENT_BUCKET, _, _}) ->
    ok;

%% Don't check on fast path either.
test_fsm_protection(_, {?WRITE_ONCE_BUCKET, _, _}) ->
    ok;

test_fsm_protection(Nodes, BKV) ->
    ?LOG_INFO("Testing with coordinator protection enabled"),
    ?LOG_INFO("Setting FSM limit to ~b", [?THRESHOLD]),
    %% Set FSM limit and reset other changes from previous tests.
    Config = default_config(#config{fsm_limit=?THRESHOLD}),
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),
    Node1 = hd(Nodes),

    rt:wait_for_cluster_service(Nodes, riak_kv),
    rt:load_modules_on_nodes([?MODULE], Nodes),
    {ok, ExpectedFsms} = get_calculated_sj_limit(Node1, riak_kv_get_fsm_sj, 1),

    %% We expect exactly ExpectedFsms, but because of a race in SideJob we sometimes get 1 more
    %% Adding 2 (the highest observed race to date) to the lte predicate to handle the occasional case.
    %% Once SideJob is fixed we should remove it (Basho #2219).
    ProcFun = build_predicate_lte(test_fsm_protection, (ExpectedFsms+2),
                                 "ProcFun", "Procs"),
    QueueFun = build_predicate_lt(test_fsm_protection, (?NUM_REQUESTS),
                                  "QueueFun", "QueueSize"),
    verify_test_results(run_test(Nodes, BKV), BKV, ProcFun, QueueFun),

    ok.

get_calculated_sj_limit(Node, ResourceName) ->
    get_calculated_sj_limit(Node, ResourceName, 5).

get_calculated_sj_limit(Node, ResourceName, Retries) when Retries > 0 ->
    CallResult = rpc:call(Node, erlang, apply, [fun() -> ResourceName:width() * ResourceName:worker_limit() end, []]),
    Result = case CallResult of
        {badrpc, Reason} ->
            ?LOG_INFO("Failed to retrieve sidejob limit from ~0p for ~0p: ~0p", [Node, ResourceName, Reason]),
            timer:sleep(1000),
            get_calculated_sj_limit(Node, ResourceName, Retries-1);
        R -> {ok, R}
    end,
    Result;

get_calculated_sj_limit(Node, ResourceName, Retries) when Retries == 0 ->
    {error, io_lib:format("Failed to retrieve sidejob limit from ~0p for resource ~0p. Giving up.", [Node, ResourceName])}.

test_cover_queries_overload(Nodes) ->
    ?LOG_INFO("Testing cover queries with vnode queue protection enabled"),
    ?LOG_INFO("Setting vnode overload threshold to ~b", [?THRESHOLD]),
    ?LOG_INFO("Setting vnode check interval to 1"),

    Config = default_config(#config{vnode_overload_threshold=?THRESHOLD,
                           vnode_check_request_interval=2,
                           vnode_check_interval=1}),
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),

    [rt:wait_for_service(Node, riak_kv) || Node <- Nodes],
    rt:load_modules_on_nodes([?MODULE], Nodes),

    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    Pids = [begin
                ?LOG_INFO("Suspending all kv vnodes on ~0p", [N]),
                suspend_and_overload_all_kv_vnodes(N)
            end || N <- [Node2, Node3, Node4, Node5]],

    [?assertEqual({error, <<"mailbox_overload">>}, KeysRes) ||
        KeysRes <- [list_keys(Node1) || _ <- lists:seq(1, 3)]],

    ?LOG_INFO("list_keys correctly handled overload"),

    [?assertEqual({error, mailbox_overload}, BucketsRes) ||
        BucketsRes <- [list_buckets(Node1) || _ <- lists:seq(1, 3)]],
    ?LOG_INFO("list_buckets correctly handled overload"),

    ?LOG_INFO("Resuming all kv vnodes"),
    [resume_all_vnodes(Pid) || Pid <- Pids],

    ?LOG_INFO("Waiting for vnode queues to empty"),
    wait_for_all_vnode_queues_empty(Node2).

run_test(Nodes, BKV) ->
    [Node1 | _RestNodes] = Nodes,
    rt:wait_for_cluster_service(Nodes, riak_kv),
    ?LOG_INFO("Sleeping for 5s to let process count stablize"),
    timer:sleep(5000),
    rt:load_modules_on_nodes([?MODULE], Nodes),
    overload_proxy:start_link(),
    rt_intercept:add(Node1, {riak_kv_get_fsm, [{{start, 4}, count_start_4}]}),

    Victim = get_victim(Node1, BKV),
    ?LOG_INFO("Suspending vnode ~0p/~0p",
               [element(1, Victim), element(2, Victim)]),
    Suspended = suspend_vnode(Victim),

    NumProcs1 = overload_proxy:get_count(),

    ?LOG_INFO("Initial process count on ~0p: ~b", [Node1, NumProcs1]),
    ?LOG_INFO("Sending ~b read requests", [?NUM_REQUESTS]),
    Reads = spawn_reads(Node1, BKV, ?NUM_REQUESTS),

    rt:wait_until(fun() ->
                          overload_proxy:is_settled(20)
                  end, 5, 500),
    NumProcs2 = overload_proxy:get_count(),
    ?LOG_INFO("Final process count on ~0p: ~b", [Node1, NumProcs2]),

    QueueLen = vnode_gets_in_queue(Victim),
    ?LOG_INFO("Final vnode queue length for ~0p: ~b",
               [Victim, QueueLen]),

    resume_vnode(Suspended),
    rt:wait_until(fun() ->
                          vnode_gets_in_queue(Victim) =:= 0
                  end),
    kill_pids(Reads),
    overload_proxy:stop(),
    {NumProcs2 - NumProcs1, QueueLen}.

get_victim(Node, {Bucket, Key, _}) ->
    Hash = riak_core_util:chash_std_keyfun({Bucket, Key}),
    PL = riak_core_ring:preflist(Hash, rt:get_ring(Node)),
    hd(PL).

ring_manager_check_fun(Node) ->
    fun() ->
            case rpc:call(Node, riak_core_ring_manager, get_chash_bin, []) of
                {ok, _R} ->
                    true;
                _ ->
                    false
            end
    end.

create_bucket_type(Nodes, Type, Props) ->
    ?LOG_INFO("Create bucket type ~0p, wait for propagation", [Type]),
    rt:create_and_activate_bucket_type(hd(Nodes), Type, Props),
    rt:wait_until_bucket_type_status(Type, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {Type, <<"bucket">>}, Props),
    ok.

node_overload_check(Pid) ->
    fun() ->
            Pid ! {verify_overload, self()},
            receive
                true ->
                    true;
                _ ->
                    false
            end
    end.

list_keys(Node) ->
    ?LOG_INFO("Connecting to node ~0p", [Node]),
    Pid = rt:pbc(Node),
    ?LOG_INFO("Listing keys on node ~0p", [Node]),
    Res = riakc_pb_socket:list_keys(Pid, {<<"normal_type">>, ?BUCKET}, 20000),
    ?LOG_INFO("List keys on node ~0p completed with result ~0p", [Node, Res]),
    riakc_pb_socket:stop(Pid),
    Res.

list_buckets(Node) ->
    {ok, C} = riak:client_connect(Node),
    riak_client:list_buckets(30000, C).

wait_for_all_vnode_queues_empty(Node) ->
    rt:wait_until(Node, fun(N) ->
                                vnode_queues_empty(N)
                        end).

vnode_queues_empty(Node) ->
    rpc:call(Node, ?MODULE, remote_vnode_queues_empty, []).

remote_vnode_queues_empty() ->
    lists:all(fun({_, _, Pid}) ->
                      {message_queue_len, Len} =
                          process_info(Pid, message_queue_len),
                      Len =:= 0
              end, riak_core_vnode_manager:all_vnodes()).

write_once(Node, {Bucket, Key, Value}) ->
    ?LOG_INFO("Writing to node ~0p", [Node]),
    PBC = rt:pbc(Node, [{auto_reconnect, true}, {queue_if_disconnected, true}]),
    lager:info(
        "Write response for Bucket ~p ~p",
        [Bucket, rt:pbc_write(PBC, Bucket, Key, Value)]
    ),
    riakc_pb_socket:stop(PBC).

read_until_success(Node) ->
    {ok, C} = riak:client_connect(Node),
    read_until_success(C, 0).

read_until_success(C, Count) ->
    case riak_client:get(<<"dummy">>, <<"dummy">>, C) of
        {error, mailbox_overload} ->
            read_until_success(C, Count+1);
        _ ->
            Count
    end.

spawn_reads(Node, {Bucket, Key, _}, Num) ->
    Self = self(),
    Pids = [begin
                Pid = spawn(fun() ->
                        rt:wait_until(pb_get_fun(Node, Bucket, Key, Self), ?GET_RETRIES, ?GET_RETRIES)
                            end),
                timer:sleep(1), % slow down just a bit to prevent thundering herd from overwhelming the node,
                Pid
            end || _ <- lists:seq(1, Num)],
    [ receive {sent, Pid} -> ok end || Pid <- Pids ],
    Pids.

pb_get_fun(Node, Bucket, Key, TestPid) ->
    fun() ->
            PBC = rt:pbc(Node),
            Result = case catch riakc_pb_socket:get(PBC, Bucket, Key) of
                         {error, <<"overload">>} ->
                             ?LOG_DEBUG("overload detected in pb_get, continuing..."),
                             true;
                         %% we expect timeouts in this test as we've shut down a vnode - return true in this case
                         {error, timeout} ->
                             ?LOG_DEBUG("timeout detected in pb_get, continuing..."),
                             true;
                         {error, <<"timeout">>} ->
                             ?LOG_DEBUG("timeout detected in pb_get, continuing..."),
                             true;
                         {ok, Res} ->
                             ?LOG_DEBUG("riakc_pb_socket:get(~0p, ~0p, ~0p) succeeded, Res:~0p", [PBC, Bucket, Key, Res]),
                             true;
                         {error, Type} ->
                             ?LOG_ERROR("riakc_pb_socket threw error ~0p reading {~0p, ~0p}, retrying...", [Type, Bucket, Key]),
                             false;
                         {'EXIT', Type} ->
                             ?LOG_INFO("riakc_pb_socket threw error ~0p reading {~0p, ~0p}, retrying...", [Type, Bucket, Key]),
                             false
                     end,
            case Result of
                true -> TestPid ! {sent, self()};
                false -> false
            end,

            riakc_pb_socket:stop(PBC),
            Result
    end.

kill_pids(Pids) ->
    [exit(Pid, kill) || Pid <- Pids].

suspend_and_overload_all_kv_vnodes(Node) ->
    ?LOG_INFO("Suspending vnodes on ~0p", [Node]),
    Pid = rpc:call(Node, ?MODULE, remote_suspend_and_overload, []),
    Pid ! {overload, self()},
    receive {overloaded, Pid} ->
        ?LOG_INFO("Received overloaded message from ~0p", [Pid]),
        Pid
    end,
    rt:wait_until(node_overload_check(Pid)),
    Pid.

remote_suspend_and_overload() ->
    spawn(fun() ->
                  Vnodes = riak_core_vnode_manager:all_vnodes(),
                  [begin
                       ?LOG_INFO("Suspending vnode pid: ~0p", [Pid]),
                       erlang:suspend_process(Pid)
                   end || {riak_kv_vnode, _, Pid} <- Vnodes],
                  wait_for_input(Vnodes)
          end).

wait_for_input(Vnodes) ->
    receive
        {overload, From} ->
            ?LOG_INFO("Overloading vnodes."),
            [overload(Vnodes, Pid) || {riak_kv_vnode, _, Pid} <- Vnodes],
            ?LOG_INFO("Sending overloaded message back to test."),
            From ! {overloaded, self()},
            wait_for_input(Vnodes);
        {verify_overload, From} ->
            OverloadCheck = verify_overload(Vnodes),
            From ! OverloadCheck,
            wait_for_input(Vnodes);
        resume ->
            ?LOG_INFO("Resuming vnodes"),
            [erlang:resume_process(Pid) || {riak_kv_vnode, _, Pid}
                                               <- Vnodes]
    end.

verify_overload(Vnodes) ->
    MessageLists = [element(2, process_info(Pid, messages)) ||
                       {riak_kv_vnode, _, Pid} <- Vnodes],
    OverloadMsgCounts = lists:foldl(fun overload_msg_counter/2, [], MessageLists),
    lists:all(fun(X) -> X >= ?NUM_REQUESTS end, OverloadMsgCounts).

overload_msg_counter(Messages, Acc) ->
    Count = lists:foldl(fun count_overload_messages/2, 0, Messages),
    [Count | Acc].

count_overload_messages(Message, Count) ->
    case Message of
        {set_concurrency_limit, some_lock, 1} ->
            Count + 1;
        _ ->
            Count
    end.

overload(Vnodes, Pid) ->
    %% The actual message doesn't matter. This one just has the least
    %% side effects.
    [Pid ! {set_concurrency_limit, some_lock, 1} ||
        _ <- lists:seq(1, ?NUM_REQUESTS)],
    %% Need to send 1 message through the proxy to get to overloaded state
    {Mod, Idx, _} = lists:keyfind(Pid, 3, Vnodes),
    ProxyPid = whereis(riak_core_vnode_proxy:reg_name(Mod, Idx)),
    ProxyPid ! junk.



suspend_vnode({Idx, Node}) ->
    suspend_vnode(Node, Idx).

suspend_vnode(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_suspend_vnode, [Idx], infinity).

remote_suspend_vnode(Idx) ->
    spawn(fun() ->
                  {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
                  erlang:suspend_process(Pid),
                  receive resume ->
                          erlang:resume_process(Pid)
                  end
          end).

suspend_vnode_proxy({Idx, Node}) ->
    suspend_vnode_proxy(Node, Idx).

suspend_vnode_proxy(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_suspend_vnode_proxy, [Idx], infinity).

remote_suspend_vnode_proxy(Idx) ->
    spawn(fun() ->
                  Name = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
                  Pid = whereis(Name),
                  erlang:suspend_process(Pid, []),
                  receive resume ->
                          erlang:resume_process(Pid)
                  end
          end).

resume_all_vnodes(Pid) ->
    Pid ! resume.

resume_vnode(Pid) ->
    Pid ! resume.

process_count(Node) ->
    rpc:call(Node, erlang, system_info, [process_count]).

vnode_gets_in_queue({Idx, Node}) ->
    vnode_gets_in_queue(Node, Idx).

vnode_gets_in_queue(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_vnode_gets_in_queue, [Idx]).

dropped_stat(Node) ->
    Stats = rpc:call(Node, riak_core_stat, get_stats, []),
    proplists:get_value(dropped_vnode_requests_total, Stats).

get_fsm_active_stat(Node) ->
    Stats = rpc:call(Node, riak_kv_stat, get_stats, []),
    proplists:get_value(node_get_fsm_active, Stats).

run_count(Node) ->
    timer:sleep(500),
    ?LOG_INFO("fsm count:~0p", [get_num_running_gen_fsm(Node)]),
    run_count(Node).

get_num_running_gen_fsm(Node) ->
    Procs = rpc:call(Node, erlang, processes, []),
    ProcInfo = [ rpc:call(Node, erlang, process_info, [P]) || P <- Procs, P /= undefined ],

    InitCalls = [ [ proplists:get_value(initial_call, Proc) ] || Proc <- ProcInfo, Proc /= undefined ],
    FsmList = [ proplists:lookup(riak_kv_get_fsm, Call) || Call <- InitCalls ],
    length(proplists:lookup_all(riak_kv_get_fsm, FsmList)).

remote_vnode_gets_in_queue(Idx) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
    {messages, AllMessages} = process_info(Pid, messages),
    GetMessages = lists:filter(fun is_get_req/1, AllMessages),
    ?LOG_INFO("Get requests (~0p): ~b", [Idx, length(GetMessages)]),
    length(GetMessages).

%% This is not the greatest thing ever, since we're coupling this test pretty
%% tightly to the internal representation of get requests in riak_kv...can't
%% really figure out a better way to do this, though.
is_get_req({'$gen_event', {riak_vnode_req_v1, _, _, Req}}) ->
    case element(1, Req) of
        riak_kv_get_req_v1 ->
            true;
        riak_kv_head_req_v1 ->
            % May also now be a head request once we start testing 2.2.7
            true;
        _ ->
            false
    end;
is_get_req(_) ->
    false.

%% In tests that do not expect work to be shed, we want to confirm that
%% at least ?NUM_REQUESTS (queue entries) are handled.
build_predicate_gte(Test, Metric, Label, ValueLabel) ->
    fun (X) ->
            ?LOG_INFO("in test ~0p ~0p, ~0p:~0p, expected no overload, Metric:>=~0p",
                       [Test, Label, ValueLabel, X, Metric]),
            X >= Metric
    end.

%% In tests that do not expect work to be shed, we want to confirm that
%% exactly ?NUM_REQUESTS (processes entries) are handled.
build_predicate_eq(Test, Metric, Label, ValueLabel) ->
    fun (X) ->
        ?LOG_INFO("in test ~0p ~0p, ~0p:~0p, expected no overload, Metric:==~0p",
            [Test, Label, ValueLabel, X, Metric]),
        X == Metric
    end.

%% In tests that expect work to be shed due to overload, the success
%% condition is simply that the number of (fsms|queue entries) is
%% less than ?NUM_REQUESTS.
build_predicate_lt(Test, Metric, Label, ValueLabel) ->
    fun (X) ->
            ?LOG_INFO("in test ~0p ~0p, ~0p:~0p, expected overload, Metric:<~0p",
                       [Test, Label, ValueLabel, X, Metric]),
            X < Metric
    end.

%% In tests that expect work to be shed due to overload, the success
%% condition is simply that the number of (fsms|queue entries) is
%% less than ?NUM_REQUESTS.
build_predicate_lte(Test, Metric, Label, ValueLabel) ->
    fun (X) ->
        ?LOG_INFO("in test ~0p ~0p, ~0p:~0p, expected overload, Metric:=<~0p",
            [Test, Label, ValueLabel, X, Metric]),
        X =< Metric
    end.
