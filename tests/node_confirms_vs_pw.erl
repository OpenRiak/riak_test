%% -------------------------------------------------------------------
%%% @copyright (C) 2017, NHS Digital
%%% @doc
%%% riak_test for pw vs node_confirms behaviour.
%%%
%%% when using w=3, pw=2 in the attempt to prevent data loss by writing
%%% primary nodes only to ensure the write goes to more than one physical
%%% node, one ends up rejecting writes in the case of more than one node
%%% going down.
%%% node_confirms solves this issue by writing to both primary and
%%% fallback nodes, ensuring that the writes are to different physical nodes.
%%%
%%% This test demonstrates that of writing to a bucket with pw=2 when 2 nodes
%%% from the preflist are down will be rejected, whereas the same situation with
%%% node_confirms=2 returns a successful write.
%%% Finally, it demonstrates that write to a bucket with a node_confirms value
%%% that cannot be met will be rejected.
%%%
%%% @end
-module(node_confirms_vs_pw).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(RING_SIZE, 32).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core,
                [{default_bucket_props,
                    [{allow_mult, true}, {dvv_enabled, true}]},
                    {ring_creation_size, ?RING_SIZE},
                    {vnode_inactivity_timeout, 12000},
                    {forced_ownership_handoff, 8},
                    {handoff_concurrency, 8}
                ]
            },
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    [Node1|_] = Cluster = rt:build_cluster(5, Conf),

    % Get preflist, we need to find two primary nodes to stop for this bucket/key
    PL = rt:get_preflist(Node1, ?BUCKET, ?KEY),
    ?LOG_INFO("Got preflist"),
    ?LOG_INFO("Preflist ~0p", [PL]),
    [FirstNode | OtherPrimaries] = [Node || {{_Idx, Node}, Type} <- PL,
                                    Type == primary],
    ?LOG_INFO("Other Primaries ~0p", [OtherPrimaries]),

    [rt:stop_and_wait(N) || N <- OtherPrimaries],
    ?LOG_INFO("Killed 2 primaries"),
    %% Wait for prelist to change to 1 primary, 2 fallbacks
    wait_for_new_preflist(FirstNode, 1, 2),

    Client = rt:httpc(FirstNode),

    %% Now write test for pw=2, node_confirms=0. Should fail, as only one primary available
    ?LOG_INFO("Change bucket properties to pw:2 node_confirms:0"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 2}, {'node_confirms', 0}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 2}, {'node_confirms', 0}]),
    ?LOG_INFO("Attempting to write key"),
    %% Write key and confirm error pw=2 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"PW-value unsatisfied: 1/2\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now write test for pw=0, node_confirms=2. Should pass, as three physical nodes available
    ?LOG_INFO("Change bucket properties to pw:0 node_confirms:2"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 2}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 2}]),
    %% Write key
    ?LOG_INFO("Attempting to write key"),
    %% write key and confirm success
    ?assertMatch(ok, rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),
    ?LOG_INFO("Read back key with node_confirms = 2"),
    {ok, ObjNC2} = rhc:get(Client, ?BUCKET, ?KEY),
    ?assertMatch(<<"12345">>, riakc_obj:get_value(ObjNC2)),

    %% Negative tests
    %% Now write test for pw=0, node_confirms=4. Should fail, as node_confirms should not be greater than n_val (3)
    ?LOG_INFO("Change bucket properties to pw:0 node_confirms:4"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 4}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 4}]),
    %% Write key
    ?LOG_INFO("Attempting to write key"),
    %% Write key and confirm error invalid pw/node_confirms
    ?assertMatch({error, {ok,"400",_,<<"Specified w/dw/pw/node_confirms values invalid for bucket n value of 3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now stop another node and write test for pw=0, node_confirms=3. Should fail, as only two physical nodes available
    PL2 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    ?LOG_INFO("Got preflist"),
    ?LOG_INFO("Preflist ~0p", [PL2]),

    ?LOG_INFO("Change bucket properties to pw:0 node_confirms:3"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 3}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 3}]),
    Others = [Node || {{_Idx, Node}, _Type} <- PL2, Node /= FirstNode],
    OtherFailedNode = lists:last(Others),
    rt:stop_and_wait(OtherFailedNode),
    wait_for_new_preflist(FirstNode, PL2),
    PL3 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    ?LOG_INFO("Preflist ~0p", [PL3]),

    %% How many nodes are up?
    RemainingUpNodes = [UpNode2 || {{_Idx, UpNode2}, _Type} <- PL3],
    UpNodeCount = length(lists:usort(RemainingUpNodes)),
    {PriCount3, _FallCount3} = primary_and_fallback_counts(PL3),
    ?LOG_INFO("Left with ~w nodes ~w and ~w primary vnodes",
                [UpNodeCount, RemainingUpNodes, PriCount3]),

    ?LOG_INFO("Attempting to write key"),
    %% Write key and confirm error node_confirms=3 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"node_confirms-value unsatisfied: 2/3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12346">>)),

    ?LOG_INFO("Setting pw and node_confirms to reflect current status"),
    rpc:call(FirstNode,
                riak_core_bucket, set_bucket,
                [?BUCKET,
                    [{'pw', PriCount3}, {'node_confirms', UpNodeCount}]]),
    rt:wait_until_bucket_props([FirstNode],
                                ?BUCKET,
                                    [{'pw', PriCount3},
                                        {'node_confirms', UpNodeCount}]),

    ?LOG_INFO("Read back key"),
    ?LOG_INFO("Although previous put failed, it will have been co-ordinated"),
    ?LOG_INFO("We can expect to see siblings - both PUTs should be present"),
    ?LOG_INFO("Success occurs as enough nodes and primaries are up"),
    {ok, ObjS2} = rhc:get(Client, ?BUCKET, ?KEY),
    ?assertMatch([<<"12345">>, <<"12346">>],
                    lists:usort(riakc_obj:get_values(ObjS2))),
    ?LOG_INFO("Incrementing node_confirms to cause failure"),
    rpc:call(FirstNode,
                riak_core_bucket, set_bucket,
                [?BUCKET,
                    [{'node_confirms', UpNodeCount + 1}]]),
    rt:wait_until_bucket_props([FirstNode],
                                ?BUCKET,
                                    [{'node_confirms', UpNodeCount + 1}]),
    {error, {ok, "500", _Headers, Error}} = rhc:get(Client, ?BUCKET, ?KEY),
    ?LOG_INFO("Error ~0p now returned", [Error]),
    ExpectedError =
        list_to_binary(
            io_lib:format("Error:\n{insufficient_nodes,~w,need,~w}\n",
                            [UpNodeCount, UpNodeCount + 1])),
    ?assertMatch(ExpectedError, Error),

    ?LOG_INFO("Restart all nodes"),
    lists:foreach(fun rt:start_and_wait/1, [OtherFailedNode|OtherPrimaries]),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Cluster),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Cluster),
    {ok, ObjS3} = rhc:get(Client, ?BUCKET, ?KEY),
    ?assertMatch([<<"12345">>, <<"12346">>],
                    lists:usort(riakc_obj:get_values(ObjS3))),

    ?LOG_INFO("Test when setting notfound_ok and with one node down"),
    ?LOG_INFO("Should be able to confirm with node_confirms=3 if notfound_ok"),
    ?LOG_INFO("But if not notfound_ok, should not be able to read initially"),
    ?LOG_INFO("And this state will be corrected by read repair"),
    {C0, _PbC0, FailedNode0} =
        setup_notfound_test(Node1, <<"notfound_key0">>, false),

    ?LOG_INFO("Read should fail first time if notfound_ok is false"),
    {error, {ok, "500", _AltHeaders, AltError}} =
        rhc:get(C0, ?BUCKET, <<"notfound_key0">>),
    ?LOG_INFO("Error ~0p now returned", [AltError]),
    AltExpectedError =
        list_to_binary(
            io_lib:format("Error:\n{insufficient_nodes,~w,need,~w}\n",
                            [2, 3])),
    ?assertMatch(AltExpectedError, AltError),

    CheckAfterRepair =
        fun() ->
            case rhc:get(C0, ?BUCKET, <<"notfound_key0">>) of
                {ok, ObjS5} ->
                    ?assertMatch(<<"ABCDE">>, riakc_obj:get_value(ObjS5)),
                    true;
                _ ->
                    false
            end
        end,
    ?LOG_INFO("Because of read repair - read will soon succeed"),
    rt:wait_until(CheckAfterRepair, 10, 1000),

    ?LOG_INFO("Restart all nodes"),
    lists:foreach(fun rt:start_and_wait/1, [FailedNode0]),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Cluster),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Cluster),

    {C1, _PbC1, FailedNode1} =
        setup_notfound_test(Node1, <<"notfound_key1">>, true),

    ?LOG_INFO("Read should succeed first time if notfound_ok is true"),
    {ok, ObjS5} = rhc:get(C1, ?BUCKET, <<"notfound_key1">>),
    ?assertMatch(<<"ABCDE">>, riakc_obj:get_value(ObjS5)),

    ?LOG_INFO("Restart all nodes"),
    lists:foreach(fun rt:start_and_wait/1, [FailedNode1]),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Cluster),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Cluster),

    ?LOG_INFO("Repeat test, this time overriding node_confirms using option"),
    {C2, _PbC2, FailedNode2} =
        setup_notfound_test(Node1, <<"notfound_key2">>, false),

    ?LOG_INFO("Read should succeed first time if override node_confirms"),
    {ok, ObjS6} =
        rhc:get(C2, ?BUCKET, <<"notfound_key2">>, [{node_confirms, 1}]),
    ?assertMatch(<<"ABCDE">>, riakc_obj:get_value(ObjS6)),

    ?LOG_INFO("Restart all nodes"),
    lists:foreach(fun rt:start_and_wait/1, [FailedNode2]),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Cluster),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Cluster),

    ?LOG_INFO("Repeat test, this time overriding in PB Client"),
    {_C3, PbC3, _FailedNode3} =
        setup_notfound_test(Node1, <<"notfound_key3">>, false),

    {ok, ObjS7} =
        riakc_pb_socket:get(PbC3,
                            ?BUCKET,
                            <<"notfound_key3">>,
                            [{node_confirms, 1}]),
    ?assertMatch(<<"ABCDE">>, riakc_obj:get_value(ObjS7)),

    pass.


setup_notfound_test(Node1, Key, NotFoundOK) ->
    ?LOG_INFO("Testing with notfound_ok ~w", [NotFoundOK]),
    AltPL1 = rt:get_preflist(Node1, ?BUCKET, Key),
    [PriA, _PriB, PriC] =
        [Node || {{_Idx, Node}, Type} <- AltPL1, Type == primary],
    HttpC = rt:httpc(PriA),
    PbC = rt:pbc(PriA),

    ?LOG_INFO("Write new object in re-formed cluster"),
    rt:httpc_write(HttpC, ?BUCKET, Key, <<"ABCDE">>),

    ?LOG_INFO("Stop a single primary"),
    rt:stop_and_wait(PriC),
    ?LOG_INFO("Target_n_val should be 4"),
    ?LOG_INFO("So new preflist should be on 3 nodes"),
    AltPL2 = rt:get_preflist(PriA, ?BUCKET, Key),
    ?assertMatch(3, length(
                        lists:usort(
                            lists:map(fun({{_Idx, AltN}, _Status}) -> AltN end,
                                        AltPL2)))),
    ?LOG_INFO("Setting pw to 2 and node_confirms to 3"),
    rpc:call(PriA,
                riak_core_bucket, set_bucket,
                [?BUCKET,
                    [{'pw', 2},
                        {'node_confirms', 3},
                        {'notfound_ok', NotFoundOK}]]),
    rt:wait_until_bucket_props([PriA],
                                ?BUCKET,
                                [{'pw', 2},
                                {'node_confirms', 3},
                                {'notfound_ok', NotFoundOK}]),
    {HttpC, PbC, PriC}.


primary_and_fallback_counts(PL) ->
    lists:foldl(fun({{_, _}, primary}, {P, F}) ->
                        {P+1, F};
                   ({{_, _}, fallback}, {P, F}) ->
                        {P, F+1}
                end,
                {0, 0},
PL).

partition_compare(OldPL, NewPL) ->
    Old = [Part || {{_, Part}, _} <- OldPL],
    New = [Part || {{_, Part}, _} <- NewPL],
    Old == New.

wait_for_new_preflist(FirstNode, Primaries, Fallbacks) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
                          primary_and_fallback_counts(NewPL) == {Primaries, Fallbacks}
    end).

wait_for_new_preflist(FirstNode, OldPL) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
                          not partition_compare(OldPL, NewPL)
    end).

