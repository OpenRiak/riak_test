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
%% 
%% Check that the metadata_version can be switched between v0 and v1

-module(verify_metadata_version_change).


-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(SNK_WORKERS, 4).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).

-define(REPL_SLEEP, 2048).

-define(BATCH_SIZE, 2000).

-define(NGR_INIT_DELAY, 5000).
-define(DELAY_COUNT, 35).
-define(DELAY_WAIT, 2000).
    % The ?DELAY_COUNT * ?DELAY_WAIT >. 65s.  This is because a sink worker
    % will delay for 65s when it hits an error, and an error can be caused by
    % for all peers restarting all the source nodes.

-define(INDEX_ENTRIES, 2).
-define(FIELD_LIST,
    ["bin1", "bin2", "bin3", "bin4", "bin5", "bin6", "bin7", "bin8"]
).


-define(CONFIG(RingSize, NVal, SrcQueueDefns, MDV), [
    {riak_core, [
        {ring_creation_size, RingSize},
        {handoff_concurrency, 8},
        {forced_ownership_handoff, 8},
        {vnode_inactivity_timeout, 4000},
        {vnode_management_timer, 2000},
        {default_bucket_props, [
            {n_val, NVal},
            {allow_mult, true},
            {dvv_enabled, true}
        ]}
    ]},
    {riak_kv, [
        {anti_entropy, {off, []}},
        {tictacaae_active, active},
        {tictacaae_parallelstore, leveled_ko},
        % if backend not leveled will use parallel key-ordered store
        {tictacaae_rebuildwait, 4},
        {tictacaae_rebuilddelay, 3600},
        {tictacaae_exchangetick, 120 * 1000},
        {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
        {ttaaefs_maxresults, 128},
        {delete_mode, keep},
        {replrtq_enablesrc, true},
        {replrtq_srcqueue, SrcQueueDefns},
        {ngr_initial_timeout, ?NGR_INIT_DELAY},
        {metadata_version, MDV}
    ]}
]).

-define(SNK_CONFIG(ClusterName, PeerList), [
    {riak_kv, [
        {replrtq_enablesink, true},
        {replrtq_sinkqueue, ClusterName},
        {replrtq_sinkpeers, PeerList},
        {replrtq_sinkworkers, ?SNK_WORKERS}
    ]}
]).


confirm() ->
    ClusterASrcQ = "cluster_b:any",
    ClusterBSrcQ = "cluster_a:any",

    [ClusterA, ClusterB] =
        rt:deploy_clusters(
            [
                {5, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ, v0)},
                {1, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ, v1)}
            ]
        ),

    ?LOG_INFO("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfigHTTP =
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":http"
        end,
    reset_peer_config(FoldToPeerConfigHTTP, ClusterA, ClusterB),

    ?LOG_INFO("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB
    ),
    rt:wait_until_transfers_complete(ClusterA),
    rt:wait_until_no_pending_changes(ClusterA),

    ?LOG_INFO("Two replicating clusters configured - active v0 standby v1"),

    metadata_version_change_test(ClusterA, ClusterB).


reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB) ->
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end,
        ClusterA
    ),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end,
        ClusterB
    ),
    
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB).

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_index(N) ->
    list_to_binary(io_lib:format("I~8..0B", [N])).

to_meta(N) ->
    list_to_binary(io_lib:format("M~8..0B", [N])).

metadata_version_change_test(ClusterA, ClusterB) ->
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),

    InitLoadClient = rt:pbc(NodeA),
    InitHTTPClient = rt:httpc(NodeA),

    ?LOG_INFO("Load some initial data in cluster A"),

    _LoadInitData = 
        lists:foreach(
            fun(I) ->
                write_data(
                    InitLoadClient,
                    riakc_pb_socket,
                    I
                )
            end,
            lists:seq(1, ?BATCH_SIZE)
        ),
    
    AReadsInit =
        read_data(
            InitLoadClient,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE,
            false
        ),
    AReadsInitHTTP =
        read_data(
            InitHTTPClient,
            rhc,
            1,
            ?BATCH_SIZE,
            false
        ),

    timer:sleep(?REPL_SLEEP),

    StandbyReader = rt:pbc(NodeB),

    BReadsInit =
        read_data(
            StandbyReader,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE,
            true
        ),
    
    ?LOG_INFO("Check A and B agree on initial data"),

    ?assert(AReadsInit == BReadsInit),
    ?assertMatch(ok, compare_pb_http(BReadsInit, AReadsInitHTTP)),

    riakc_pb_socket:stop(InitLoadClient),

    ?LOG_INFO("Reconfigure some A nodes to use V1 metadata"),

    ClusterAPart = lists:sublist(ClusterA, length(ClusterA) div 2),
    lists:foreach(
        fun(N) ->
            rt:set_advanced_conf(N,[{riak_kv, [{metadata_version, v1}]}])
        end,
        ClusterAPart
    ),

    rt:wait_until_transfers_complete(ClusterA),
    rt:wait_until_no_pending_changes(ClusterA),

    R2LoadClient = rt:pbc(NodeA),
    R3LoadClient = rt:pbc(lists:last(ClusterA)),
    R4LoadClient = rt:httpc(NodeA),
    R5LoadClient = rt:httpc(lists:last(ClusterA)),

    AReadsPartial_R2 =
        read_data(
            R2LoadClient,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE,
            false
        ),
    AReadsPartial_R5 =
        read_data(
            R5LoadClient,
            rhc,
            1,
            ?BATCH_SIZE,
            false
        ),

    ?assert(BReadsInit == AReadsPartial_R2),
    ?assertMatch(ok, compare_pb_http(BReadsInit, AReadsPartial_R5)),

    ?LOG_INFO("Load data into A - both v1 and v0 nodes - both http and pb"),

    _LoadR2Data = 
        lists:foreach(
            fun(I) ->
                write_data(
                    R2LoadClient,
                    riakc_pb_socket,
                    I
                )
            end,
            lists:seq(?BATCH_SIZE + 1, 2 * ?BATCH_SIZE)
        ),
    _LoadR3Data = 
        lists:foreach(
            fun(I) ->
                write_data(
                    R3LoadClient,
                    riakc_pb_socket,
                    I
                )
            end,
            lists:seq((2 * ?BATCH_SIZE) + 1, 3 * ?BATCH_SIZE)
        ),
    _LoadR4Data = 
        lists:foreach(
            fun(I) ->
                write_data(
                    R4LoadClient,
                    rhc,
                    I
                )
            end,
            lists:seq((3 * ?BATCH_SIZE) + 1, 4 * ?BATCH_SIZE)
        ),
    _LoadR5Data = 
        lists:foreach(
            fun(I) ->
                write_data(
                    R5LoadClient,
                    rhc,
                    I
                )
            end,
            lists:seq((4 * ?BATCH_SIZE) + 1, 5 * ?BATCH_SIZE)
        ),
    
    ?LOG_INFO("Check A and B agree on all data"),

    AReadsR2_toR5 =
        read_data(
            R2LoadClient,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE * 5,
            false
        ),
    AReadsR3_toR5 =
        read_data(
            R3LoadClient,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE * 5,
            false
        ),
    AReadsR4_toR5 =
        read_data(
            R4LoadClient,
            rhc,
            1,
            ?BATCH_SIZE * 5,
            false
        ),
    AReadsR5_toR5 =
        read_data(
            R5LoadClient,
            rhc,
            1,
            ?BATCH_SIZE * 5,
            false
        ),
    BReads_toR5 =
        read_data(
            StandbyReader,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE * 5,
            true
        ),
    ?assert(BReads_toR5 == AReadsR2_toR5),
    ?assert(BReads_toR5 == AReadsR3_toR5),
    ?assertMatch(ok, compare_pb_http(BReads_toR5, AReadsR4_toR5)),
    ?assertMatch(ok, compare_pb_http(BReads_toR5, AReadsR5_toR5)),

    riakc_pb_socket:stop(R2LoadClient),
    riakc_pb_socket:stop(R3LoadClient),

    ?LOG_INFO("Reconfigure all A nodes to use V1 metadata"),

    lists:foreach(
        fun(N) ->
            rt:set_advanced_conf(N,[{riak_kv, [{metadata_version, v1}]}])
        end,
        ClusterA
    ),
    rt:wait_until_transfers_complete(ClusterA),
    rt:wait_until_no_pending_changes(ClusterA),

    R6LoadClient = rt:pbc(NodeA),

    ?LOG_INFO("Load some more data - why not?"),

    _LoadR6Data = 
        lists:foreach(
            fun(I) ->
                write_data(
                    R6LoadClient,
                    riakc_pb_socket,
                    I
                )
            end,
            lists:seq((5 * ?BATCH_SIZE + 1), 6 * ?BATCH_SIZE)
        ),

    ?LOG_INFO("Check A and B agree on all data"),
    
    AReadsR6_toR6 =
        read_data(
            R6LoadClient,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE * 6,
            false
        ),
    BReads_toR6 =
        read_data(
            StandbyReader,
            riakc_pb_socket,
            1,
            ?BATCH_SIZE * 6,
            true
        ),
    ?assert(BReads_toR6 == AReadsR6_toR6),

    riakc_pb_socket:stop(R6LoadClient),

    pass.

read_data(Client, ClientMod, Start, End, MaybeWait) ->
    read_data(Client, ClientMod, Start, End, MaybeWait, [], 0).

read_data(_Client, _ClientMod, End, End, _MaybeWait, Acc, DelayCount) ->
    case DelayCount of
        N when N < ?DELAY_COUNT ->
            lists:reverse(Acc);
        N ->
            {error, {too_many_waits, N}}
    end;
read_data(Client, ClientMod, Start, End, MaybeWait, Acc, DC) ->
    K = to_key(Start),
    case {MaybeWait, ClientMod:get(Client, ?TEST_BUCKET, K)} of
        {_, {ok, Obj}} ->
            read_data(
                Client, ClientMod, Start + 1, End, MaybeWait, [Obj|Acc], DC);
        {true, {error, notfound}} ->
            ?LOG_INFO("Wait for replication of ~0p", [K]),
            timer:sleep(?DELAY_WAIT),
            read_data(Client, ClientMod, Start, End, MaybeWait, Acc, DC + 1)
    end.

write_data(Client, ClientMod, I) ->
    K = to_key(I),
    Obj =
        riakc_obj:new(?TEST_BUCKET, K, <<I:32/integer, (?COMMMON_VAL_INIT)/binary>>),
    MD0 = riakc_obj:get_metadata(Obj),
    FieldList = lists:sublist(?FIELD_LIST, ?INDEX_ENTRIES),
    MD1 =
        lists:foldl(
            fun(IdxName, MDAcc) ->
                riakc_obj:set_secondary_index(
                    MDAcc,
                    {{binary_index, IdxName},
                    [to_index(I), to_index(I + 1)]}
                )
            end,
            MD0,
            FieldList
        ),
    MD2 =
        lists:foldl(
            fun(MDEntry, MDAcc) ->
                riakc_obj:set_user_metadata_entry(MDAcc, MDEntry)
            end,
            MD1,
            [{<<"K1">>, to_meta(I)}, {<<"K2">>, to_meta(I + 1)}]
        ),
    ok = ClientMod:put(Client, riakc_obj:update_metadata(Obj, MD2)).

compare_pb_http([], []) ->
    ok;
compare_pb_http([PBo|RestPB], [HTTPo|RestHTTP]) ->
    KMatch = riakc_obj:key(PBo) == riakc_obj:key(HTTPo),
    VMatch = riakc_obj:get_value(PBo) == riakc_obj:get_value(HTTPo),
    ClockMatch = riakc_obj:vclock(PBo) == riakc_obj:vclock(HTTPo),
    PBumd =
        sets:from_list(
            riakc_obj:get_user_metadata_entries(
                riakc_obj:get_metadata(PBo)
            ),
            [{version, 2}]
        ),
    HTTPumd =
        sets:from_list(
            riakc_obj:get_user_metadata_entries(
                riakc_obj:get_metadata(HTTPo)
            ),
            [{version, 2}]
        ),
    PB2i =
        sets:from_list(
            riakc_obj:get_secondary_indexes(
                riakc_obj:get_metadata(PBo)
            ),
            [{version, 2}]
        ),
    HTTP2i = 
        sets:from_list(
            riakc_obj:get_secondary_indexes(
                riakc_obj:get_metadata(HTTPo)
            ),
            [{version, 2}]
        ),
    UMDMatch = sets:intersection(PBumd, HTTPumd) == sets:union(PBumd, HTTPumd),
    IdxMatch = sets:intersection(PB2i, HTTP2i) == sets:union(PB2i, HTTP2i),
    AllMatch =
        KMatch andalso
        VMatch andalso
        ClockMatch andalso
        UMDMatch andalso
        IdxMatch,
    case AllMatch of
        true ->
            compare_pb_http(RestPB, RestHTTP);
        false ->
            ?LOG_WARNING(
                "Compare fail key ~w value ~w clock ~w umd ~w Idx ~w",
                [KMatch, VMatch, ClockMatch, UMDMatch, IdxMatch]
            ),
            ?LOG_INFO("PB ~0p", [PBo]),
            ?LOG_INFO("HTTP ~0p", [HTTPo]),
            {error, compare_fail}
    end.
    