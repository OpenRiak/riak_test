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
%% @doc
%% Confirm that setting sync_on_write when using multi-backend does not
%% cause a crash.

-module(verify_multibackend_synconwrite).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(TRACE_FUNC, "riak_kv_multi_backend:flush_put/5").

-define(CONF(BackendRef, RingSize, MemoryBackend),
    [
        {
            riak_kv,
            [
                {anti_entropy, {off, []}},
                {storage_backend, riak_kv_multi_backend},
                {multi_backend_default, BackendRef},
                {
                    multi_backend,
                    [
                        {
                            <<"eleveldb1">>,
                            riak_kv_eleveldb_backend,
                            [{data_root, "$(platform_data_dir)/eleveldb1"}]
                        },
                        {
                            <<"leveled1">>,
                            riak_kv_leveled_backend,
                            [{data_root, "$(platform_data_dir)/leveled1"}]
                        },
                        {
                            <<"bitcask1">>,
                            riak_kv_bitcask_backend,
                            [{data_root, "$(platform_data_dir)/bitcask1"}]
                        },
                        {
                            <<"memory1">>,
                            MemoryBackend,
                                % Memory backend may use other backend so
                                % that all backends use sync_on_write
                            [{data_root, "$(platform_data_dir)/memory1"}]
                        }
                    ]
                }
            ]
        },
        {riak_core,
            [
                {ring_creation_size,        RingSize},
                {handoff_concurrency,       max(8, RingSize div 16)},
                {forced_ownership_handoff,  max(8, RingSize div 16)},
                {vnode_inactivity_timeout,  4000},
                {vnode_management_timer,    4000}
            ]
        }
    ]
).

confirm() ->
    ?LOG_INFO("Overriding backend set in configuration"),
    ?LOG_INFO("Backend will now be default backend in multi-backend"),

    BackendRef = get_backendref(),
    rt_redbug:set_tracing_applied(true),
    ScratchDir = rt_config:get(rt_scratch_dir),
    TraceFile1 = filename:join(ScratchDir, "all_trace"),
    TraceFile2 = filename:join(ScratchDir, "allbutone_trace"),

    ?LOG_INFO("Configure so all backends support sync_on_write"),
    rt:set_backend(riak_kv_multi_backend),
    [Node1] =
        rt:deploy_nodes(
            1,
            ?CONF(BackendRef, ?DEFAULT_RING_SIZE, riak_kv_bitcask_backend)
        ),
    pass =
        test_write_read(
            BackendRef,
            Node1,
            <<"TestAllSupportSync">>,
            TraceFile1
        ),
    
    rt:clean_cluster([Node1]),
    
    ?LOG_INFO("Configure with some backends not supporting sync_on_write"),
    [Node2] =
        rt:deploy_nodes(
            1,
            ?CONF(BackendRef, ?DEFAULT_RING_SIZE, riak_kv_memory_backend)
        ),
    pass =
        test_write_read(
            BackendRef,
            Node2,
            <<"TestAllButOneSupportSync">>,
            TraceFile2
        ),

    ?LOG_INFO("Tidy-up trace files"),
    ok = delete_files([TraceFile1, TraceFile2]),
    pass
    .


test_write_read(BackendRef, Node, TypedBucket, TraceFile) ->
    ?LOG_INFO("Setting up a bucket type to use backend ~0p", [BackendRef]),
    Type = <<"backend">>,
    BucketProps = [{backend, BackendRef}, {sync_on_write, one}],
    rt:create_and_activate_bucket_type(Node, Type, BucketProps),
    rt:wait_until_bucket_type_status(Type, active, [Node]),
    rt:wait_until_bucket_props([Node], {Type, TypedBucket}, BucketProps),

    rt_redbug:trace(
        Node,
        ?TRACE_FUNC,
        #{arity => true, print_file => TraceFile}
    ),

    ?LOG_INFO("Write and read some values"),
    {ok, C} = riak:client_connect(Node),
    CVB = <<"SampleValue">>,
    lists:foreach(
        fun(I) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [I])),
            Obj =
                riak_object:new(
                    {Type, TypedBucket},
                    Key,
                    <<I:32/integer, CVB/binary>>
                ),
            ok = riak_client:put(Obj, C)
        end,
        lists:seq(1, 1000)
    ),

    ?LOG_INFO("Check one flush_put for each write (as sync_on_write = one)"),
    1000 = fun_count(TraceFile),

    lists:foreach(
        fun(I) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [I])),
            ExpectedVal = <<I:32/integer, CVB/binary>>,
            {ok, Obj} = riak_client:get({Type, TypedBucket}, Key, C),
            ExpectedVal = riak_object:get_value(Obj)
        end,
        lists:seq(1, 1000)
    ),

    pass.


get_backendref() ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    ?LOG_INFO("Running with backend ~0p", [Backend]),
    get_backendref(Backend).

get_backendref(eleveldb) ->
    <<"eleveldb1">>;
get_backendref(bitcask) ->
    <<"bitcask1">>;
get_backendref(leveled) ->
    <<"leveled1">>;
get_backendref(memory) ->
    <<"memory1">>.

fun_count(File) ->
    ?LOG_INFO("Checking trace file ~s", [File]),
    {ok, FileData} = file:read_file(File),
    count_matches(re:run(FileData, "\\b" ?TRACE_FUNC "\\b", [global])).

count_matches(nomatch) ->
    0;
count_matches({match, Matches}) ->
    length(Matches).

delete_files(Files) ->
    lists:foreach(fun file:delete/1, Files).