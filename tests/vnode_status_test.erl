%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026  TI Tokyo.
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
-module(vnode_status_test).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BACKENDS, [riak_kv_bitcask_backend,
                   riak_kv_eleveldb_backend,
                   riak_kv_leveled_backend,
                   riak_kv_memory_backend,
                   riak_kv_multi_backend,
                   riak_kv_multi_prefix_backend
                  ]).

-define(EXPECTED_KEYS_IN_VNODE_STATUS_RESPONSES,
        [[key_count, status],
         [compactions, files_size_mb, fixed_indexes, level, read_block_error, read_mb, time, write_mb],
         [fetch_count_by_level, get_body_time, get_sample_count, head_rsp_time, head_sample_count,
          journal_last_compaction_duration,
          journal_last_compaction_max,
          journal_last_compaction_mean,
          journal_last_compaction_runlength,
          journal_last_compaction_score,
          journal_last_compaction_time,
          ledger_cache_size,
          level_files_count,
          n_active_journal_files,
          penciller_inmem_cache_size,
          penciller_last_merge_time,
          penciller_work_backlog_status,
          put_ink_time,
          put_mem_time,
          put_prep_time,
          put_sample_count
         ],
         [data_table_status, index_table_status],
         [name],
         [name]  %% an arbitrary name of a bitcask backend
         %% (a single bitcask backend, as configured by default)
        ]).

confirm() ->

    Nodes = rtdev:deploy_nodes(
              [{current, [{riak_kv, [{storage_backend, B}]}]} || B <- ?BACKENDS]),
    ok = rt:wait_until_nodes_ready(Nodes),

    [vnode_status_test(A) || A <- lists:zip(Nodes, ?EXPECTED_KEYS_IN_VNODE_STATUS_RESPONSES)],

    pass.

vnode_status_test({Node, Keys}) ->
    ?LOG_INFO("Test vnode-status is good json on ~s", [Node]),
    rt:wait_until(
      fun() ->
              {ok, {ExitCode, Output}} = rt:admin(Node, ["vnode-status"], [return_exit_code]),
              ?assertEqual(0, ExitCode),
              [Json, "ok\n"] = string:split(Output, "\n"),
              {struct, [{_, A}]} = mochijson2:decode(Json),
              A /= []
      end),
    {ok, {ExitCode, Output}} = rt:admin(Node, ["vnode-status"], [return_exit_code]),
    ?assertEqual(0, ExitCode),
    [Json, "ok\n"] = string:split(Output, "\n"),
    {struct, SS} = mochijson2:decode(Json),
    {struct, PS} = proplists:get_value(atom_to_binary(Node), SS),
    {struct, PL} = proplists:get_value(<<"0">>, PS),
    {_, BS} = proplists:get_value(<<"backend_status">>, PL),
    [begin
         case lists:keyfind(atom_to_binary(K), 1, BS) of
             false ->
                 ?LOG_ERROR("Key ~s not found in ~p", [K, BS]),
                 ?assert(false);
             _ ->
                 ok
         end
     end || K <- Keys].
