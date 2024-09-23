%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
-module(http_bucket_types).
-behavior(riak_test).

-export([confirm/0]).

%% MFA callbacks
-export([mapred_modfun/3, mapred_modfun_type/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(SUMVALUE_MAPRED,
            [{map,
                {modfun, riak_kv_mapreduce, map_object_value},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_string_to_integer},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_sum},
                undefined, true}]).

-define(COUNT_MAPRED,
            [{map,
                {modfun, riak_kv_mapreduce, map_object_value},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_count_inputs},
                undefined, true}]).

-define(WAIT(E), ?assertEqual(ok, rt:wait_until(fun() -> (E) end))).

confirm() ->
    application:start(ibrowse),
    ?LOG_INFO("Deploy some nodes"),
    Nodes = rt:build_cluster(4, [], [
                                     {riak_core, [{default_bucket_props,
                                                   [
                                                       {n_val, 2},
                                                       {allow_mult, true},
                                                       {dvv_enabled, true}
                                                   ]}]}]),
    Node = hd(Nodes),

    RMD = riak_test_runner:metadata(),
    HaveIndexes = case proplists:get_value(backend, RMD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    RHC = rt:httpc(Node),
    ?LOG_INFO("default type get/put test"),
    %% write explicitly to the default type
    ok = rhc:put(RHC, riakc_obj:new({<<"default">>, <<"bucket">>},
                               <<"key">>, <<"value">>)),

    %% read from the default bucket implicitly
    {ok, O1} = rhc:get(RHC, <<"bucket">>, <<"key">>),
    %% read from the default bucket explicitly
    {ok, O2} = rhc:get(RHC, {<<"default">>, <<"bucket">>}, <<"key">>),

    %% same object, but slightly different presentation
    ?assertEqual(riakc_obj:key(O1), riakc_obj:key(O2)),
    ?assertEqual(riakc_obj:get_value(O1), riakc_obj:get_value(O2)),
    ?assertEqual(riakc_obj:only_bucket(O1), riakc_obj:only_bucket(O2)),
    ?assertEqual(riakc_obj:vclock(O1), riakc_obj:vclock(O2)),
    ?assertEqual(undefined, riakc_obj:bucket_type(O1)),
    ?assertEqual(<<"default">>, riakc_obj:bucket_type(O2)),

    %% write implicitly to the default bucket
    ok = rhc:put(RHC, riakc_obj:update_value(O1, <<"newvalue">>)),

    %% read from the default bucket explicitly
    {ok, O3} = rhc:get(RHC, {<<"default">>, <<"bucket">>}, <<"key">>),

    ?assertEqual(<<"newvalue">>, riakc_obj:get_value(O3)),

    ?LOG_INFO("list_keys test"),
    %% list keys
    ?WAIT({ok, [<<"key">>]} == rhc:list_keys(RHC, <<"bucket">>)),
    ?WAIT({ok, [<<"key">>]} == rhc:list_keys(RHC, {<<"default">>, <<"bucket">>})),

    ?LOG_INFO("list_buckets test"),
    %% list buckets
    ?WAIT({ok, [<<"bucket">>]} == rhc:list_buckets(RHC)),
    ?WAIT({ok, [<<"bucket">>]} == rhc:list_buckets(RHC, <<"default">>)),

    timer:sleep(5000),
    ?LOG_INFO("default type delete test"),
    %% delete explicitly via the default bucket
    ok = rhc:delete_obj(RHC, O3),

    %% read from the default bucket implicitly
    {error, {notfound, VC}} = rhc:get(RHC, <<"bucket">>, <<"key">>, [deletedvclock]),
    %% read from the default bucket explicitly
    {error, {notfound, VC}} = rhc:get(RHC, {<<"default">>, <<"bucket">>}, <<"key">>,
                               [deletedvclock]),

    %% write it again, being nice to siblings
    O3a = riakc_obj:new({<<"default">>, <<"bucket">>},
                        <<"key">>, <<"newestvalue">>),
    ok = rhc:put(RHC, riakc_obj:set_vclock(O3a, VC)),

    {ok, O4} = rhc:get(RHC, {<<"default">>, <<"bucket">>}, <<"key">>,
                       [deletedvclock]),

    %% delete explicitly via the default bucket
    ok = rhc:delete_obj(RHC, O4),

    %% read from the default bucket implicitly
    {error, notfound} = rhc:get(RHC, <<"bucket">>, <<"key">>),
    %% read from the default bucket explicitly
    {error, notfound} = rhc:get(RHC, {<<"default">>, <<"bucket">>}, <<"key">>),

    timer:sleep(5000), %% wait for delete_mode 3s to expire

    %% now there shoyld be no buckets or keys to be listed...
    %%
    %% list keys
    ?WAIT({ok, []} == rhc:list_keys(RHC, <<"bucket">>)),
    ?WAIT({ok, []} == rhc:list_keys(RHC, {<<"default">>, <<"bucket">>})),

    %% list buckets
    ?WAIT({ok, []} == rhc:list_buckets(RHC)),
    ?WAIT({ok, []} == rhc:list_buckets(RHC, <<"default">>)),

    ?LOG_INFO("custom type get/put test"),
    %% create a new type
    ok = rt:create_activate_and_wait_for_bucket_type(Nodes, <<"mytype">>, [{n_val,3}]),

    ?LOG_INFO("doing put"),
    ok = rhc:put(RHC, riakc_obj:new({<<"mytype">>, <<"bucket">>},
                               <<"key">>, <<"newestvalue">>)),

    ?LOG_INFO("doing get"),
    {ok, O5} = rhc:get(RHC, {<<"mytype">>, <<"bucket">>}, <<"key">>),

    ?assertEqual(<<"newestvalue">>, riakc_obj:get_value(O5)),

    ?LOG_INFO("doing get"),
    %% this type is NOT aliased to the default buckey
    {error, notfound} = rhc:get(RHC, <<"bucket">>, <<"key">>),

    ?LOG_INFO("custom type list_keys test"),
    ?WAIT({ok, []} == rhc:list_keys(RHC, <<"bucket">>)),
    ?WAIT({ok, [<<"key">>]} == rhc:list_keys(RHC, {<<"mytype">>, <<"bucket">>})),

    ?LOG_INFO("custom type list_buckets test"),
    %% list buckets
    ?WAIT({ok, []} == rhc:list_buckets(RHC)),
    ?WAIT({ok, [<<"bucket">>]} == rhc:list_buckets(RHC, <<"mytype">>)),

    ?LOG_INFO("UTF-8 type get/put test"),
    %% こんにちは - konnichiwa (Japanese)
    UnicodeTypeBin = unicode:characters_to_binary([12371,12435,12395,12385,12399], utf8),
    %% سلام - Salam (Arabic)
    UnicodeBucketBin = unicode:characters_to_binary([1587,1604,1575,1605], utf8),

    UCBBin = {UnicodeTypeBin, UnicodeBucketBin},

    ok = rt:create_activate_and_wait_for_bucket_type(Nodes, UnicodeTypeBin, [{n_val,3}]),

    ?LOG_INFO("doing put"),
    ok = rhc:put(RHC, riakc_obj:new(UCBBin,
                                    <<"key">>, <<"unicode">>)),

    ?LOG_INFO("doing get"),
    {ok, O6} = rhc:get(RHC, UCBBin, <<"key">>),

    ?assertEqual(<<"unicode">>, riakc_obj:get_value(O6)),

    ?LOG_INFO("unicode type list_keys test"),
    ?WAIT({ok, [<<"key">>]}== rhc:list_keys(RHC, UCBBin)),

    ?LOG_INFO("unicode type list_buckets test"),
    %% list buckets

    %% This is a rather awkward representation, but it's what rhc is
    %% currently giving us. Curl gives us
    %% {"buckets":["\u0633\u0644\u0627\u0645"]} to illustrate where
    %% the values are coming from, and those are indeed the correct
    %% hexadecimal values for the UTF-8 representation of the bucket
    %% name
    ?WAIT({ok, [<<"0633064406270645">>]} == rhc:list_buckets(RHC, UnicodeTypeBin)),

    ?LOG_INFO("bucket properties tests"),
    rhc:set_bucket(RHC, {<<"default">>, <<"mybucket">>},
                   [{n_val, 5}]),
    {ok, BProps} = rhc:get_bucket(RHC, <<"mybucket">>),
    ?assertEqual(5, proplists:get_value(n_val, BProps)),

    rhc:reset_bucket(RHC, {<<"default">>, <<"mybucket">>}),

    {ok, BProps1} = rhc:get_bucket(RHC, <<"mybucket">>),
    ?assertEqual(2, proplists:get_value(n_val, BProps1)),

    rhc:set_bucket(RHC, {<<"mytype">>, <<"mybucket">>},
                   [{n_val, 5}]),
    {ok, BProps2} = rhc:get_bucket(RHC, <<"mybucket">>),
    %% the default in the app.config is set to 2...
    ?assertEqual(2, proplists:get_value(n_val, BProps2)),

    {ok, BProps3} = rhc:get_bucket(RHC, {<<"mytype">>,
                                         <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps3)),

    rhc:reset_bucket(RHC, {<<"mytype">>, <<"mybucket">>}),

    {ok, BProps4} = rhc:get_bucket(RHC, {<<"mytype">>,
                                         <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps4)),

    ?LOG_INFO("bucket type properties test"),

    rhc:set_bucket_type(RHC, <<"mytype">>,
                        [{n_val, 5}]),

    {ok, BProps5} = rhc:get_bucket_type(RHC, <<"mytype">>),

    ?assertEqual(5, proplists:get_value(n_val, BProps5)),

    %% check that the bucket inherits from its type
    {ok, BProps6} = rhc:get_bucket(RHC, {<<"mytype">>,
                                         <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps6)),

    rhc:set_bucket_type(RHC, <<"mytype">>, [{n_val, 3}]),

    {ok, BProps7} = rhc:get_bucket_type(RHC, <<"mytype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps7)),

    %% make sure a regular bucket under the default type reflects app.config
    {ok, BProps8} = rhc:get_bucket(RHC, {<<"default">>,
                                         <<"mybucket">>}),
    ?assertEqual(2, proplists:get_value(n_val, BProps8)),

    %% make sure the type we previously created is NOT affected
    {ok, BProps9} = rhc:get_bucket_type(RHC, <<"mytype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps9)),

    %% make sure a bucket under that type is also not affected
    {ok, BProps10} = rhc:get_bucket(RHC, {<<"mytype">>,
                                          <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps10)),

    %% make sure a newly created type is not affected either
    %% create a new type
    ok = rt:create_activate_and_wait_for_bucket_type(Nodes, <<"mynewtype">>, []),

    {ok, BProps11} = rhc:get_bucket_type(RHC, <<"mynewtype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps11)),

    %% 2i tests

    case HaveIndexes of
        false -> ok;
        true ->
            Obj01 = riakc_obj:new(<<"test">>, <<"JRD">>, <<"John Robert Doe, 25">>),
            Obj02 = riakc_obj:new({<<"mytype">>, <<"test">>}, <<"JRD">>, <<"Jane Rachel Doe, 21">>),

            Obj1 = riakc_obj:update_metadata(Obj01,
                                             riakc_obj:set_secondary_index(
                                               riakc_obj:get_update_metadata(Obj01),
                                               [{{integer_index, "age"},
                                                 [25]},{{binary_index, "name"},
                                                        [<<"John">>, <<"Robert">>
                                                        ,<<"Doe">>]}])),

            Obj2 = riakc_obj:update_metadata(Obj02,
                                             riakc_obj:set_secondary_index(
                                               riakc_obj:get_update_metadata(Obj02),
                                               [{{integer_index, "age"},
                                                 [21]},{{binary_index, "name"},
                                                        [<<"Jane">>, <<"Rachel">>
                                                        ,<<"Doe">>]}])),

            ok = rhc:put(RHC, Obj1),
            ok = rhc:put(RHC, Obj2),

            ?assertMatch({ok, {index_results_v1, [<<"JRD">>], _, _}}, rhc:get_index(RHC, <<"test">>,
                                                                                    {binary_index,
                                                                                     "name"},
                                                                                    <<"John">>)),

            ?assertMatch({ok, {index_results_v1, [], _, _}}, rhc:get_index(RHC, <<"test">>,
                                                                           {binary_index,
                                                                            "name"},
                                                                           <<"Jane">>)),

            ?assertMatch({ok, {index_results_v1, [<<"JRD">>], _, _}}, rhc:get_index(RHC,
                                                                                    {<<"mytype">>,
                                                                                     <<"test">>},
                                                                                    {binary_index,
                                                                                     "name"},
                                                                                    <<"Jane">>))
    end,


    Store = fun(Bucket, {K,V, BI, II}) ->
                    O=riakc_obj:new(Bucket, K),
                    MD=riakc_obj:add_secondary_index(dict:new(),
                                                     {{binary_index, "b_idx"},
                                                      [BI]}),
                    MD2=riakc_obj:add_secondary_index(MD, {{integer_index,
                                                            "i_idx"}, [II]}),
                    OTwo=riakc_obj:update_metadata(O,MD2),
                    ok = rhc:put(RHC,riakc_obj:update_value(OTwo, V, "application/json"))
            end,

    [Store(<<"MRbucket">>, KV) || KV <- [
                                         {<<"foo">>, <<"2">>, <<"a">>, 4},
                                         {<<"bar">>, <<"3">>, <<"b">>, 7},
                                         {<<"baz">>, <<"4">>, <<"a">>, 4}]],

    ?assertEqual({ok, [{2, [9]}]},
                 rhc:mapred_bucket(RHC, <<"MRbucket">>,
                                   ?SUMVALUE_MAPRED)),

    [Store({<<"mytype">>, <<"MRbucket">>}, KV) || KV <- [
                                                         {<<"foo">>, <<"2">>, <<"a">>, 4},
                                                         {<<"bar">>, <<"3">>, <<"b">>, 7},
                                                         {<<"baz">>, <<"4">>, <<"a">>, 4},
                                                         {<<"bam">>, <<"5">>, <<"a">>, 3}]],

    ?assertEqual({ok, [{2, [14]}]},
                 rhc:mapred_bucket(RHC, {<<"mytype">>, <<"MRbucket">>},
                                   ?SUMVALUE_MAPRED)),

    ?assertEqual({ok, [{1, [3]}]},
                 rhc:mapred(RHC,
                            [{<<"MRbucket">>, <<"foo">>},
                             {<<"MRbucket">>, <<"bar">>},
                             {<<"MRbucket">>, <<"baz">>}],
                            ?COUNT_MAPRED)),

    ?assertEqual({ok, [{1, [4]}]},
                 rhc:mapred(RHC,
                                [{{{<<"mytype">>, <<"MRbucket">>}, <<"foo">>},
                                  undefined},
                                 {{{<<"mytype">>, <<"MRbucket">>}, <<"bar">>},
                                  undefined},
                                 {{{<<"mytype">>, <<"MRbucket">>}, <<"baz">>},
                                  undefined},
                                 {{{<<"mytype">>, <<"MRbucket">>}, <<"bam">>},
                                 undefined}],
                                ?COUNT_MAPRED)),

    case HaveIndexes of
        false -> ok;
        true ->
            {ok, [{1, Results}]} = rhc:mapred(RHC,
                                                {index,<<"MRbucket">>,{integer_index,
                                                                        "i_idx"},3,5},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results)),

            {ok, [{1, Results1}]} = rhc:mapred(RHC,
                                                {index,{<<"mytype">>,
                                                        <<"MRbucket">>},{integer_index,
                                                                        "i_idx"},3,5},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>, <<"5">>], lists:sort(Results1)),

            {ok, [{1, Results2}]} = rhc:mapred(RHC,
                                                {index,<<"MRbucket">>,{binary_index,
                                                                        "b_idx"}, <<"a">>},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results2)),

            {ok, [{1, Results3}]} = rhc:mapred(RHC,
                                                {index,{<<"mytype">>,
                                                        <<"MRbucket">>},{binary_index,
                                                                        "b_idx"}, <<"a">>},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>, <<"5">>], lists:sort(Results3)),
            ok
    end,

    %% load this module on all the nodes
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    %% do a modfun mapred using the function from this module
    ?assertEqual({ok, [{2, [2]}]},
                 rhc:mapred_bucket(RHC, {modfun, ?MODULE,
                                                    mapred_modfun, []},
                                       ?SUMVALUE_MAPRED)),

    %% do a modfun mapred using the function from this module
    ?assertEqual({ok, [{2, [5]}]},
                 rhc:mapred_bucket(RHC, {modfun, ?MODULE,
                                                    mapred_modfun_type, []},
                                       ?SUMVALUE_MAPRED)),


    rt:clean_cluster(Nodes),
    rt_redbug:set_tracing_applied(true),
    ?LOG_INFO("Deploy a single node cluster"),
    ?LOG_INFO("Can only monitor one node with redbug"),
    [SingleNode]
        = rt:build_cluster(1,
                            [],
                            [{riak_core,
                                [{default_bucket_props,
                                    [{n_val, 3},
                                        {allow_mult, true},
                                        {dvv_enabled, true}
                                    ]}]}]),

    ?LOG_INFO("Create a bucket type with selective sync enabled"),
    ok =
        rt:create_activate_and_wait_for_bucket_type(
            [SingleNode], <<"sync_backend">>, [{sync_on_write,backend}, {n_val,3}]),
    ok =
        rt:create_activate_and_wait_for_bucket_type(
            [SingleNode], <<"sync_one">>, [{sync_on_write,one}, {n_val,3}]),
    ok =
        rt:create_activate_and_wait_for_bucket_type(
            [SingleNode], <<"sync_all">>, [{sync_on_write,all}, {n_val,3}]),

    ScratchDir = rt_config:get(rt_scratch_dir),
    [OneTrcFile, AllTrcFile, BackendTrcFile] = TraceFiles =
        [filename:join(ScratchDir, FN) || FN <-
            ["hbt_one.trace", "hbt_all.trace", "hbt_backend.trace"]],
    %% Make sure the trace files don't exist from some previous run!
    delete_files(TraceFiles),

    ?LOG_INFO("STARTING TRACE"),
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    TraceFun = flushputfun(Backend),

    redbug_start(TraceFun, OneTrcFile, SingleNode),
    ?LOG_INFO("doing put"),
    ok = rhc:put(RHC,
        riakc_obj:new({<<"sync_one">>, <<"b_sync_one">>}, <<"key">>, <<"newestvalue">>)),
    ?LOG_INFO("doing get"),
    {ok, _} = rhc:get(RHC, {<<"sync_one">>, <<"b_sync_one">>}, <<"key">>),

    redbug_stop(),
    ?assertMatch(1, flushput_cnt(TraceFun, OneTrcFile)),


    redbug_start(TraceFun, BackendTrcFile, SingleNode),
    ?LOG_INFO("doing put"),
    ok = rhc:put(RHC,
        riakc_obj:new({<<"sync_backend">>, <<"b_sync_backend">>}, <<"key">>, <<"newestvalue">>)),
    ?LOG_INFO("doing get"),
    {ok, _} = rhc:get(RHC, {<<"sync_backend">>, <<"b_sync_backend">>}, <<"key">>),

    redbug_stop(),
    ?assertMatch(0, flushput_cnt(TraceFun, BackendTrcFile)),

    redbug_start(TraceFun, AllTrcFile, SingleNode),
    ?LOG_INFO("doing put"),
    ok = rhc:put(RHC,
        riakc_obj:new({<<"sync_all">>, <<"b_sync_all">>}, <<"key">>, <<"newestvalue">>)),
    ?LOG_INFO("doing get"),
    {ok, _} = rhc:get(RHC, {<<"sync_all">>, <<"b_sync_all">>}, <<"key">>),

    redbug_stop(),
    ?assertMatch(3, flushput_cnt(TraceFun, AllTrcFile)),

    %% Only delete trace files on success
    delete_files(TraceFiles),
    pass.

redbug_start(TraceFun, TrcFile, Node) ->
    ?LOG_INFO("TracingFun ~s on ~s", [TraceFun, Node]),
    ?assertMatch(ok, rt_redbug:trace(Node, TraceFun,
        #{arity => true, print_file => TrcFile})).

redbug_stop() ->
    ?assertMatch(ok, rt_redbug:stop()).

mapred_modfun(Pipe, _Args, _Timeout) ->
    riak_pipe:queue_work(Pipe, {{<<"MRbucket">>, <<"foo">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).

mapred_modfun_type(Pipe, _Args, _Timeout) ->
    riak_pipe:queue_work(Pipe, {{{<<"mytype">>, <<"MRbucket">>}, <<"bam">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).

flushputfun(leveled) ->
    "riak_kv_leveled_backend:flush_put/5";
flushputfun(eleveldb) ->
    "riak_kv_eleveldb_backend:flush_put/5";
flushputfun(bitcask) ->
    "riak_kv_bitcask_backend:flush_put/5";
flushputfun(undefined) ->
    flushputfun(bitcask).

flushput_cnt(TraceFun, File) ->
    ?LOG_INFO("checking ~0p", [File]),
    %% redbug doesn't create the file until it has something to write to it,
    %% so accommodate non-existence
    case file:read_file(File) of
        {ok, FileData} ->
            Pattern = "\\b" ++ TraceFun ++ "\\b",
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
