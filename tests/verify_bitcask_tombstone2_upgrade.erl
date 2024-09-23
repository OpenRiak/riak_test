%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
% @doc Verify that upgrading Riak with Bitcask to 2.0 or later will trigger
% an upgrade mechanism that will end up merging all existing bitcask files.
% This is necessary so that old style tombstones are reaped, which might
% otherwise stay around for a very long time. This version writes tombstones
% that can be safely dropped during a merge. Bitcask could resurrect old
% values easily when reaping tombstones during a partial merge if a
% restart happened later.
-module(verify_bitcask_tombstone2_upgrade).
-behaviour(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    Backend = proplists:get_value(backend, TestMetaData),
    ?LOG_INFO("Running with backend (this better be Bitcask!) ~0p", [Backend]),
    ?assertEqual({backend, bitcask}, {backend, Backend}),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, legacy),
    % Configure for fast merge checks
    Config = [{riak_kv, [{bitcask_merge_check_interval, 1000}]},
              {riak_core, [{ring_creation_size, 128}]},
              {bitcask, [{max_file_size, 100}]}],
    Nodes = rt:build_cluster([{OldVsn, Config}]),
    verify_bitcask_tombstone2_upgrade(Nodes),
    pass.

% Expects nodes running a version of Riak < 2.0 using Bitcask
verify_bitcask_tombstone2_upgrade(Nodes) ->
    ?LOG_INFO("Write some data, write it good"),
    write_some_data(Nodes),
    ?LOG_INFO("Collect the list of bitcask files created"),
    BitcaskFiles = list_bitcask_files(Nodes),
    NodeFiles = [[ {Node, Idx, F} || {Idx, F} <- Files, F /= []] || {Node, Files} <- BitcaskFiles],
    ?LOG_INFO("files are ~0p", [NodeFiles]),
    ?LOG_INFO("Now update the node to the current version"),
    [rt:upgrade(Node, current) || Node <- Nodes],
    ?LOG_INFO("And wait until all the old files have been merged, the version upgrade finished"),
    ?assertEqual(ok, rt:wait_until(upgrade_complete_fun(BitcaskFiles))),
    ?LOG_INFO("And that is that").

write_some_data([Node1 | _]) ->
    rt:pbc_systest_write(Node1, 10000).

list_bitcask_files(Nodes) ->
    [{Node, list_node_bitcask_files(Node)} || Node <- Nodes].

list_node_bitcask_files(Node) ->
    % Gather partitions owned, list *.bitcask.data on each.
    Partitions = rt:partitions_for_node(Node),
    {ok, DataDir} = rt:rpc_get_env(Node, [{bitcask, data_root}]),
    [begin
         IdxStr = integer_to_list(Idx),
         IdxDir = filename:join(DataDir, IdxStr),
         BitcaskPattern = filename:join([IdxDir, "*.bitcask.data"]),
         Paths = rpc:call(Node, filelib, wildcard, [BitcaskPattern]),
         ?assert(is_list(Paths)),
         Files = [filename:basename(Path) || Path <- Paths],
         {IdxDir, Files}
     end || Idx <- Partitions].

upgrade_complete_fun(BitcaskFiles) ->
    fun() ->
            upgrade_complete(BitcaskFiles)
    end.

upgrade_complete(BitcaskFiles) ->
    all(true, [upgrade_complete(Node, PFiles)
               || {Node, PFiles} <- BitcaskFiles]).

upgrade_complete(Node, PartitionFiles) ->
    all(true,[upgrade_complete(Node, IdxDir, Files)
              || {IdxDir, Files} <- PartitionFiles]).

upgrade_complete(Node, IdxDir, Files) ->
    %% Check we have version.txt, no upgrade.txt, no merge.txt
    MergeFile = filename:join(IdxDir, "merge.txt"),
    UpgradeFile = filename:join(IdxDir, "upgrade.txt"),
    VsnFile = filename:join(IdxDir, "version.txt"),

    file_exists(Node, VsnFile) andalso
        not file_exists(Node, UpgradeFile) andalso
        not file_exists(Node, MergeFile) andalso
        tombstones_merged(Node, IdxDir, Files).

file_exists(Node, Path) ->
    case rpc:call(Node, filelib, is_regular, [Path]) of
        {badrpc, Reason} ->
            throw({can_not_check_file, Node, Path, Reason});
        Result ->
            Result
    end.

all(Val, L) ->
    lists:all(fun(E) -> E == Val end, L).

tombstones_merged(Node, IdxDir, Files) ->
    %% this is less efficient (code and typing/reading) but at least
    %% we get some _reason_ for waiting/failure rather than a silently
    %% hung test

    lists:foldl(fun(File, Acc) ->
                        Fname = filename:join(IdxDir, File),
                        case file_exists(Node, Fname) of
                            false ->
                                Acc;
                            true ->
                                ?LOG_DEBUG("~0p unmerged", [{Node, Fname}]),
                                false
                        end
                end,
                true,
                Files).
