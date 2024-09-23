%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Russell Brown.
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
%%% @doc
%%% riak_test for vnode capability controlling HEAD request use in get fsm
%%% @end
-module(verify_head_capability).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"test-bucket">>).

%% Number of K/V pairs that will be written.
%% This *probably* affects the pre-stop sleep for redbug, below.
-define(NUM_RECS, 50).

%% First version with get/put fsm Head capability
-define(MIN_HEAD_VSN, [2, 9, 0]).

-define(TRACE_FUNC, "riak_kv_vnode:head/3").
-define(TRACE_FILES, ["vhc_mixed.trace", "vhc_current.trace"]).

confirm() ->
    %% Create a mixed cluster of current and previous
    %% Create a PB client
    %% Do some puts so we have some data
    %% do GETs, they should all be _gets_ at the vnode
    %% Upgrade nodes to current
    %% Do some GETS
    %% check that HEADs happen after cap is negotiated

    MinHeadVsn = rt_vsn:new_version(?MIN_HEAD_VSN),
    [{_HdTag, HdVsn} | _] = CfgVersions = rt_vsn:configured_versions(),
    %% Confirm that we have a version new enough for the test
    ?assert(rt_vsn:compare_versions(HdVsn, MinHeadVsn) >= 0),
    %% NewTag == 'current' if 'current' meets the criteria
    {NewTag, NewVsn} = rt_vsn:find_version_at_least(MinHeadVsn, CfgVersions),
    NewStr = rt_vsn:version_to_string(NewVsn),

    %% We'll only run the mixed cluster tests if we found an old enough
    %% version. At the end of this case, return Nodes as a suitable cluster
    %% for the next applicable test phase.
    {OldEnough, Nodes} = case
            rt_vsn:find_version_before(MinHeadVsn, CfgVersions) of
        {OldTag, OldVsn} ->
            OldStr = rt_vsn:version_to_string(OldVsn),
            ?LOG_INFO(
                "Version ~s (~s) found not supporting fsm Head requests",
                [OldStr, OldTag]),
            ?LOG_INFO(
                "Building mixed cluster of version ~s (~s) and ~s (~s) nodes",
                [OldStr, OldTag, NewStr, NewTag]),
            {true, rt:build_cluster([OldTag, OldTag, NewTag, NewTag])};
        _ ->
            ?LOG_INFO("No version found not supporting fsm Head requests"),
            ?LOG_INFO(
                "Building cluster of version ~s (~s) nodes",
                [NewStr, NewTag]),
            {false, rt:build_cluster(lists:duplicate(4, NewTag))}
    end,

    %% Ensure redbug tracing is enabled regardless of r_t configuration
    rt_redbug:set_tracing_applied(true),

    %% Write ?NUM_RECS records.
    %% Write to old and new nodes, in case it's a mixed cluster.
    ?LOG_INFO("Writing ~b records", [?NUM_RECS]),
    ?assertMatch([], rt:systest_write(
        lists:nth(2, Nodes), 1, (?NUM_RECS div 2), ?BUCKET, 2)),
    ?assertMatch([], rt:systest_write(
        lists:nth(3, Nodes), ((?NUM_RECS div 2) + 1), ?NUM_RECS, ?BUCKET, 2)),

    ScratchDir = rt_config:get(rt_scratch_dir),
    [TraceFile1, TraceFile2] = TraceFiles =
        [filename:join(ScratchDir, FN) || FN <- ?TRACE_FILES],
    %% Make sure the trace files don't exist from some previous run!
    delete_files(TraceFiles),

    if OldEnough ->

        ?LOG_INFO("STARTING MIXED CLUSTER TRACE"),

        %% Pick ONE node to trace, which is the one we'll read on
        TraceNode1 =  rt_util:random_element(Nodes),
        ?LOG_INFO("Tracing on node ~0p", [TraceNode1]),
        ?assertMatch(ok, rt_redbug:trace(TraceNode1, ?TRACE_FUNC,
            #{arity => true, print_file => TraceFile1})),

        ReadRes1 = rt:systest_read(TraceNode1, 1, ?NUM_RECS, ?BUCKET, 2),

        ?assertMatch(ok, rt_redbug:stop()),

        ?assertMatch(0, head_cnt(TraceFile1)),
        ?assertMatch([], ReadRes1),

        ?LOG_INFO("upgrade all to ~s", [NewTag]),

        [OldNode1, OldNode2 | _] = Nodes,
        rt:upgrade(OldNode1, NewTag),
        rt:upgrade(OldNode2, NewTag);

    true ->    % ! OldEnough
        ok
    end,

    lists:foreach(
        fun(Node) ->
            ?assertMatch(ok,
                rt:wait_until_capability(Node, {riak_kv, get_request_type}, head))
        end, Nodes),

    ?LOG_INFO("STARTING HOMOGENEOUS CLUSTER TRACE"),

    %% Pick ONE node to trace, which is the one we'll read on
    TraceNode2 = rt_util:random_element(Nodes),
    ?LOG_INFO("Tracing on node ~0p", [TraceNode2]),
    ?assertMatch(ok, rt_redbug:trace(TraceNode2, ?TRACE_FUNC,
        #{arity => true, print_file => TraceFile2})),

    ReadRes2 = rt:systest_read(TraceNode2, 1, ?NUM_RECS, ?BUCKET, 2),

    ?assertMatch(ok, rt_redbug:stop()),

    %% one per read (should we count the handle_commands instead?)
    ?assertMatch(?NUM_RECS, head_cnt(TraceFile2)),
    ?assertMatch([], ReadRes2),

    %% Only delete trace files on success
    delete_files(TraceFiles),
    pass.


head_cnt(File) ->
    ?LOG_INFO("checking ~s", [File]),
    %% redbug doesn't create the file until it has something to write to it,
    %% so accommodate non-existence
    case file:read_file(File) of
        {ok, FileData} ->
            Pattern = "\\b" ?TRACE_FUNC "\\b",
            count_matches(re:run(FileData, Pattern, [global]));
        Error ->
            ?assertMatch({error, enoent}, Error),
            0
    end.

count_matches(nomatch) ->
    0;
count_matches({match, Matches}) ->
    length(Matches).

delete_files(Files) ->
    lists:foreach(fun file:delete/1, Files).
