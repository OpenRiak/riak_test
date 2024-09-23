%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
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
%% Start a Riak node and do not wait for riak_kv to come up.
%% The protobuf port should only open when the riak_kv service is ready.
%%
%% We test scenarios that are expected to
%%  - connect prematurely and fail
%%  - wait for riak_kv to be up and succeed
%%
%% Implementation Notes:
%%
%%  Timing is critical, and attempts to start the Riak node before trying to
%%  connect to it proved unreliable, at best.
%%
%%  Test scenarios are run in a separate agent process to
%%  - allow the agent process to start trying to connect before the Riak node
%%    is started
%%  - allow the controlling process to impose timeout limitations on the
%%    test scenario
%%  - handle what may be quite a few 'EXIT' messages due to connection
%%    failures/retries in riakc_pb_socket
%%
-module(pb_listen_wait_until_kv_ready).
-behavior(riak_test).

-export([
    confirm/0,          %% riak_test behavior
    silence_crashes/2,  %% logger hook
    agent_proc/1        %% spawned agent process
]).

-include_lib("kernel/include/logger.hrl").
% -include_lib("stdlib/include/assert.hrl").

%% First version with riak_api_pb_listener:wait_for_listener_ready.
%% ToDo: Update when the feature is in a published Riak release.
-define(MIN_FEATURE_VSN, [3, 0, 20]).

-define(BASE_CONFIG, [
    {riak_core, [
        {ring_creation_size, 8},
        %% turbo mode
        {vnode_inactivity_timeout, 1000},
        {vnode_management_timer, 100},
        {handoff_concurrency, 8},
        %% default
        {default_bucket_props, [{notfound_ok, false}]}
    ]}
]).

-define(NO_SERVICES,    []).

-define(CWR_MAX_TRIES,  9999).
-define(CWR_RETRY_MS,   20).

-type test_result() :: pass| fail.
-type test_error() :: {error, term()} | {atom(), {error, term()}}.
-type test_counter() :: refused | except | test_error().
-type test_stats() :: #{
    tries := non_neg_integer(),
    result => test_result(),
    delay => rtt:millisecs(),
    test_counter() => pos_integer()
}.
-record(run, {
    node :: node(),
    ip :: inet:ip_address(),
    port :: inet:port_number(),
    keyint :: pos_integer()
}).
-record(ret, {
    result :: test_result(),
    stats :: test_stats(),
    run :: #run{}
}).

-spec confirm() -> test_result() | no_return().
confirm() ->
    %% No crash logs, please.
    ok = silence_crashes(),
    %% Don't let stray exits take down the test.
    erlang:process_flag(trap_exit, true),
    %% Ensure the first call to results() doesn't return 'undefined'.
    results([]),
    Owner = erlang:self(),
    Agent = start_agent(Owner),

    FeatureVsn = rt_vsn:new_version(?MIN_FEATURE_VSN),
    CfgVersions = rt_vsn:configured_versions(),
    %% To confirm the test itself, use a version before
    %% wait_for_listener_ready/1 was implemented in riak_api_pb_listener
    case rt_vsn:find_version_before(FeatureVsn, CfgVersions) of
        {OldTag, OldVsn} ->
            ?LOG_INFO("Testing for failure with version "
                ++ term_fmt(OldTag) ++ " (~s)",
                [OldTag, rt_vsn:version_to_string(OldVsn)]),
            OldNode = deploy_node(OldTag),
            {ok, {OldIP, OldPort}} = rt:get_pb_conn_info(OldNode),
            connect_write_read(Agent, OldNode, OldIP, OldPort, ?LINE, fail),
            stop_node(OldNode, ?LINE);
        _ ->
            ok
    end,

    %% Test against 'current' in a single node
    {CurTag, CurVsn} = rt_vsn:find_version_at_least(FeatureVsn, CfgVersions),
    ?LOG_INFO(
        "Testing for success with version " ++ term_fmt(CurTag) ++ " (~s)",
        [CurTag, rt_vsn:version_to_string(CurVsn)]),
    Node = deploy_node(CurTag),
    {ok, {IP, PbPort}} = rt:get_pb_conn_info(Node),
    connect_write_read(Agent, Node, IP, PbPort, ?LINE, pass),
    stop_node(Node, ?LINE),

    stop_agent(Owner, Agent),
    consume_exits(0, ?LINE),

    ?LOG_INFO("RESULTS: ~0p", [lists:sort(results())]),
    test_result().

term_fmt(Term) when erlang:is_atom(Term) ->
    "'~s'";
term_fmt(_Term) ->
    "~0p".

-spec connect_write_read(
    Agent :: pid(), Node :: node(), IP :: inet:ip_address(),
    PbPort :: inet:port_number(), KeyInt :: pos_integer(), Expect :: term())
        -> ok.
%%
%% @hidden
%% @doc Restarts Node, runs the test scenario, reports and records result.
%%
%% @param Agent     The agent process.
%% @param Node      The target node.
%% @param IP        IP address of the target node.
%% @param PbPort    Protobuf listening port on the target node.
%% @param KeyInt    A unique integer identifying the test scenario, use ?LINE.
%% @param Expect    The test result that indicates scenario success.
%%
connect_write_read(Agent, Node, IP, PbPort, KeyInt, Expect) ->
    Owner = erlang:self(),
    Run = #run{node = Node, ip = IP, port = PbPort, keyint = KeyInt},
    TTO = rt_config:get(rt_max_wait_time),
    stop_node(Node, KeyInt),
    Agent ! {Owner, Run},
    receive
        {Agent, {running, Run}} ->
            ok
    after
        5000 ->
            erlang:error(timeout, [Agent, run])
    end,
    start_node(Node, KeyInt),
    #ret{result = Result, stats = Stats} = receive
        {Agent, #ret{run = Run} = Ret} ->
            Ret
    after
        TTO ->
            erlang:error(timeout, [Agent, result])
    end,
    case Result of
        Expect ->
            Fmt = "Scenario SUCCESS, expected result " ++ term_fmt(Result),
            ?LOG_INFO(Fmt, [Result]);
        _ ->
            Fmt = lists:flatten([
                "Scenario FAILURE, expect/actual: ",
                term_fmt(Expect), "/", term_fmt(Result)]),
            ?LOG_ERROR(Fmt, [Expect, Result])
    end,
    ?LOG_INFO("Stats: ~0p", [Stats]),
    result({KeyInt, (Result =:= Expect)}),
    consume_exits(0, ?LINE),
    ok.

-spec connect_write_read(#run{}) -> test_stats().
connect_write_read(#run{ip = IP, port = Port, keyint = KeyInt}) ->
    I = erlang:integer_to_list(KeyInt),
    B = erlang:list_to_binary(["Bucket-", I]),
    K = erlang:list_to_binary(["Key-", I]),
    V = erlang:list_to_binary(["Value-", I]),
    Rec = riakc_obj:new(B, K, V),
    Stats = #{tries => 1, result => pass, delay => ?CWR_RETRY_MS},
    connect_write_read(Rec, IP, Port, Stats).

connect_write_read(_Rec, _IP, _Port,
        #{tries := Try} = Stats, IncStat) when Try >= ?CWR_MAX_TRIES ->
    {timeout, increment_stat(IncStat, Stats)};
connect_write_read(Rec, IP, Port,
        #{tries := Try, delay := Delay} = Stats, IncStat) ->
    timer:sleep(Delay),
    connect_write_read(Rec, IP, Port,
        increment_stat(IncStat, Stats#{tries => (Try + 1)})).

connect_write_read(Rec, IP, Port, Stats) ->
    try riakc_pb_socket:start_link(IP, Port) of
        {ok, Pid} ->
            case riakc_pb_socket:put(Pid, Rec) of
                ok ->
                    B = riakc_obj:bucket(Rec),
                    K = riakc_obj:key(Rec),
                    ?LOG_INFO("PUT ~0p:~0p => ~0p", [
                        B, K, riakc_obj:get_update_value(Rec)]),
                    Result = case riakc_pb_socket:get(Pid, B, K) of
                        {ok, Out} ->
                            ?LOG_INFO("GET ~0p:~0p => ~0p", [
                                riakc_obj:bucket(Out), riakc_obj:key(Out),
                                riakc_obj:get_value(Out)]),
                            Stats;
                        GetErr ->
                            increment_stat({get, GetErr}, Stats)
                    end,
                    riakc_pb_socket:stop(Pid),
                    Result;
                PutErr ->
                    riakc_pb_socket:stop(Pid),
                    connect_write_read(Rec, IP, Port, Stats, PutErr)
            end;
        {error, {tcp, econnrefused}} ->
            connect_write_read(Rec, IP, Port, Stats, refused);
        Unexpected ->
            connect_write_read(Rec, IP, Port, Stats, Unexpected)
    catch
        Class:Reason ->
            ?LOG_ERROR("~0p : ~0p", [Class, Reason]),
            connect_write_read(Rec, IP, Port, Stats, except)
    end.

%% @hidden Drain 'EXIT' messages
consume_exits(Count, Line) ->
    receive
        {'EXIT', _From, _Reason} ->
            consume_exits((Count + 1), Line)
    after
        10 ->
            ?LOG_INFO("~s:~b: consumed ~b EXIT(s)", [?MODULE, Line, Count])
    end.

%%
%% Agent process
%%

-spec start_agent(Owner :: pid()) -> pid() | no_return().
start_agent(Owner) ->
    Agent = erlang:spawn_link(?MODULE, agent_proc, [Owner]),
    Agent ! {Owner, start},
    receive
        {Agent, ready} ->
            Agent
    after
        5000 ->
            erlang:error(timeout, [Agent, start])
    end.

-spec stop_agent(Owner :: pid(), Agent :: pid()) -> ok.
stop_agent(Owner, Agent) ->
    Agent ! {Owner, quit},
    ok.

-spec agent_proc(Owner :: pid()) -> no_return().
%% @private Agent process entry
agent_proc(Owner) ->
    erlang:process_flag(trap_exit, true),
    Agent = erlang:self(),
    receive
        {Owner, start} ->
            Owner ! {Agent, ready}
    after
        5000 ->
            erlang:exit(timeout)
    end,
    agent_loop(Owner, Agent).

-spec agent_loop(Owner :: pid(), Agent :: pid()) -> no_return().
%% @hidden Agent process message handler
agent_loop(Owner, Agent) ->
    receive
        {Owner, quit} ->
            erlang:exit(normal);
        {Owner, #run{} = Run} ->
            Owner ! {Agent, {running, Run}},
            TestStats = connect_write_read(Run),
            {Result, Stats} = maps:take(result, TestStats),
            Owner ! {Agent, #ret{result = Result, stats = Stats, run = Run}},
            consume_exits(0, ?LINE)
    end,
    agent_loop(Owner, Agent).

%%
%% Node operations, all return their Node parameter for nesting.
%%

deploy_node(VsnTag) ->
    [Node] = rt:deploy_nodes([{VsnTag, ?BASE_CONFIG}], ?NO_SERVICES),
    Node.

start_node(Node, Line) ->
    ?LOG_INFO("~s:~b starting node ~w", [?MODULE, Line, Node]),
    rt:start(Node),
    rt:log_to_nodes([Node], "~s:~b START NODE", [?MODULE, Line]),
    Node.

stop_node(Node, Line) ->
    ?LOG_INFO("~s:~b stopping node ~w", [?MODULE, Line, Node]),
    rt:log_to_nodes([Node], "~s:~b STOP NODE", [?MODULE, Line]),
    _ = rt:stop(Node),
    Node.

%%
%% Results tracking
%%

result(Result) ->
    results([Result | results()]).

test_result() ->
    test_result(results()).

test_result([]) ->
    pass;
test_result([{_Line, true} | Results]) ->
    test_result(Results);
test_result(_) ->
    fail.

results() ->
    erlang:get(scenario_results).

results(Results) ->
    erlang:put(scenario_results, Results).

%%
%% stats
%%

%% @hidden
%% When the key being incremented is not an atom, it's an error tuple or
%% maybe a binary, so the overall result will be 'fail'. If the result is
%% already 'fail' we can just update the stat counter, but if it's 'pass'
%% we need to consider the type of Key and maybe update it.
increment_stat(Key, Stats) when erlang:is_atom(Key) ->
    increment_map_counter(Key, Stats);
%% Key is an error counter but result is already 'fail'
increment_stat(Key, #{result := fail} = Stats) ->
    increment_map_counter(Key, Stats);
%% Current result => 'pass' and Key is an error counter
increment_stat(Key, Stats) ->
    increment_map_counter(Key, Stats#{result => fail}).

%% @hidden Should be inlined away.
increment_map_counter(Key, Map) ->
    maps:update_with(Key, fun(V) -> (V + 1) end, 1, Map).

%%
%% no crash logs
%%

silence_crashes() ->
    logger:add_primary_filter(?MODULE, {fun silence_crashes/2, ?MODULE}).

silence_crashes(#{msg := {report, #{label := {proc_lib, crash}}}}, ?MODULE) ->
    stop;
silence_crashes(Map, Param) ->
    ?LOG_INFO("~0p ~0p", [Param, Map]),
    ignore.
