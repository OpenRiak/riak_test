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
-module(query_json_queue).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(RING_SIZE, 16).
-define(DEFAULT_BUCKET_PROPS, [{allow_mult, true}, {dvv_enabled, true}]).
-define(BTYPE, <<"Type1">>).
-define(BNAME, <<"Bucket1">>).

-define(PEOPLE_INDEX, "peoplefinder_bin").
-define(FAMILY_INDEX, "familyname_bin").
-define(GIVEN_INDEX, "givenname_bin").
-define(POSTCODE_INDEX, "postcode_bin").
-define(REPORT_INDEX, "healthreport_bin").
-define(PEOPLE_TERMS, ["19650501|SMITH|ANNE.MARIE.ANNE-MARIE|LS9_0TW"]).
-define(FAMILY_TERMS, ["SMITH|19650501|19895060499999999", "JONES|19650501|19650501198950604"]).
-define(GIVEN_TERMS, ["ANNE|19650501|1965050199999999", "MARIE|19650501|1965050199999999", "ANNE-MARIE|19650501|1965050199999999"]).
-define(POSTCODE_TERMS, ["LS9_0TW|19650501|1990080199999999", "LS9_1GH|19650501|1965050119900801"]).
-define(REPORT_TERMS, ["SHA0001GP00000119650501FYNNY"]).

-define(VERSION, "HTTP/1.1").

-define(CONFIG(RingSize),
    [
        {
            riak_kv, 
                [
                    {anti_entropy, {off, []}},
                    {log_index_fsm, true},
                    {tictacaae, passive}
                ]
        },
        {
            riak_core,
                [
                    {ring_creation_size,        RingSize},
                    {default_bucket_props,      ?DEFAULT_BUCKET_PROPS},
                    {handoff_concurrency,       max(8, RingSize div 16)},
                    {forced_ownership_handoff,  max(8, RingSize div 16)},
                    {vnode_inactivity_timeout,  8000},
                    {vnode_management_timer,    4000}
                ]
        }
    ]
).

confirm() ->
    case proplists:get_value(backend, riak_test_runner:metadata()) of
        leveled ->
            ObjectCount = 1000,
            Nodes = rt:build_cluster(4, ?CONFIG(?RING_SIZE)),
            ok = setup_data(Nodes, ObjectCount),
            ok = confirm_errors(Nodes),
            ok = test_peoplefinder_query(Nodes, ObjectCount);
        OtherBackend ->
            ?LOG_INFO("Backend ~0p does not support query API", [OtherBackend])
    end,
    pass.

confirm_errors(Nodes) ->
    ok = inets:start(),
    {ok, {HTTP_IP, HTTP_Port}} = rt:get_http_conn_info(hd(Nodes)),
    {ok, _HTTPC} = inets:start(httpc, [{profile, test_client}]),
    ok = httpc:set_options([{verbose, false}], test_client),
    URI =
        lists:flatten(
            io_lib:format(
                "http://~s:~w/types/~s/buckets/~s/query",
                [HTTP_IP, HTTP_Port, ?BTYPE, ?BNAME]
            )
        ),
    JSON1 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qfn\" : \"SMITH\", \"qgn\" : \"ANNE\"},
                \"accumulation_option\" : \"queue_raw_keys\",
                \"inactivity_timeout\" : -1,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650501\",
                            \"end_term\"   : \"19650501~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)\",
                            \"filter_expression\" : \"($fn = :qfn ) AND (:qgn IN $gn)\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 400, "Bad Request"}, Hdrs1, RespBody1}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON1
            },
            [],
            [],
            test_client
        ),
    ?assert(lists:member({"content-type","application/json"}, Hdrs1)),
    {struct, [DecodedRsp1]} = mochijson2:decode(RespBody1),
    ?LOG_INFO("Decoded response to bad inactivity timeout ~0p", [DecodedRsp1]),
    ?assertMatch(<<"error">>, element(1, DecodedRsp1)),
    ?assertMatch(
        "Validation failure at stage init due to Bad inactivity timeout",
        element(2, DecodedRsp1)
    ),

    ValidJSON = valid_key_query(),

    {ok, {{?VERSION, 200, "OK"}, _, ValidResult}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                ValidJSON
            },
            [],
            [],
            test_client
        ),

    {struct, DecodedRsp2} = mochijson2:decode(ValidResult),
    ?LOG_INFO("Response to query ~0p", [DecodedRsp2]),
    [{<<"result_reference">>, QueueRef}] = DecodedRsp2,

    {QNode, Pid, Secret} =
        binary_to_term(
            erpc:call(
                hd(Nodes),
                riak_kv_query_filebuffer,
                safe_decode,
                [QueueRef]
            )
        ),
    
    WrongNode =
        erpc:call(
            hd(Nodes),
            riak_kv_query_filebuffer,
            safe_encode,
            [term_to_binary({'badnode@127.0.0.1', Pid, Secret})]
        ),
    WrongPid =
        erpc:call(
            hd(Nodes),
            riak_kv_query_filebuffer,
            safe_encode,
            [term_to_binary({QNode, self(), Secret})]
        ),
    WrongSecret =
        erpc:call(
            hd(Nodes),
            riak_kv_query_filebuffer,
            safe_encode,
            [term_to_binary({QNode, Pid, <<"BadSecret">>})]
        ),
    ?LOG_INFO(
        "Incorrect node, pid or secret will error as if the query buffer died"
    ),
    ?LOG_INFO("Incorrect node"),
    ok = 
        get_result_error(
            HTTP_IP,
            HTTP_Port,
            WrongNode,
            1,
            500,
            "Internal Server Error",
            "node_unreachable"
        ),
    ?LOG_INFO("Incorrect pid"),
    ok = 
        get_result_error(
            HTTP_IP,
            HTTP_Port,
            WrongPid,
            1,
            500,
            "Internal Server Error",
            "result_server_terminated"
        ),
    ?LOG_INFO("Incorrect secret"),
    ok = 
        get_result_error(
            HTTP_IP,
            HTTP_Port,
            WrongSecret,
            1,
            500,
            "Internal Server Error",
            "incorrect_reference"
        ),
    ?LOG_INFO(
        "Incorrect bucket is a specific error"
    ),
    ok = 
        get_result_error(
            HTTP_IP,
            HTTP_Port,
            QueueRef,
            1,
            500,
            "Internal Server Error",
            "incorrect_bucket",
            <<"OtherBucketName">>
        ),
    ?LOG_INFO(
        "Max results must be positive number - 400 error"
    ),
    ok = 
        get_result_error(
            HTTP_IP,
            HTTP_Port,
            QueueRef,
            -1,
            400,
            "Bad Request",
            "Invalid max_results"
        ),

    inets:stop(),
    ok.



test_peoplefinder_query(Nodes, ObjectCount) when ObjectCount > 3 ->
    ok = inets:start(),
    {ok, {HTTP_IP, HTTP_Port}} = rt:get_http_conn_info(hd(Nodes)),
    erpc:call(
        hd(Nodes),
        application,
        set_env,
        [riak_kv, queue_inactivity_timeout_secs, 2]
    ),
    {ok, _HTTPC} = inets:start(httpc, [{profile, test_client}]),
    ok = httpc:set_options([{verbose, false}], test_client),

    ?LOG_INFO("Test basic eval and filter"),
    JSON1 = valid_key_query(),

    URI =
        lists:flatten(
            io_lib:format(
                "http://~s:~w/types/~s/buckets/~s/query",
                [HTTP_IP, HTTP_Port, ?BTYPE, ?BNAME]
            )
        ),

    {ok, {{?VERSION, 200, "OK"}, _, ResultJson0}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON1
            },
            [],
            [],
            test_client
        ),
    
    {struct, DecodedResponse} = mochijson2:decode(ResultJson0),
    ?LOG_INFO("Response to query ~0p", [DecodedResponse]),
    [{<<"result_reference">>, QueueRef}] = DecodedResponse,

    MapResponse1 = get_results(HTTP_IP, HTTP_Port, QueueRef, 0, false),
    ?assertMatch([], maps:get(<<"raw_keys">>, MapResponse1)),
    ?assertMatch(0, maps:get(<<"responses_count">>, MapResponse1)),

    rt:wait_until(
        fun() ->
            MR = get_results(HTTP_IP, HTTP_Port, QueueRef, 0, true),
            maps:get(<<"received_count">>, MR) > 1
        end
    ),

    MapResponse2 = get_results(HTTP_IP, HTTP_Port, QueueRef, 1, false),
    ?assertMatch(1, length(maps:get(<<"raw_keys">>, MapResponse2))),
    ?assertMatch(1, maps:get(<<"responses_count">>, MapResponse2)),

    KeyList2 = maps:get(<<"raw_keys">>, MapResponse2),

    {ok, {HTTP_IPL, HTTP_PortL}} = rt:get_http_conn_info(lists:last(Nodes)),

    MapResponse3 = get_results(HTTP_IPL, HTTP_PortL, QueueRef, 1, false),
    ?assertMatch(1, length(maps:get(<<"raw_keys">>, MapResponse3))),
    ?assertMatch(2, maps:get(<<"responses_count">>, MapResponse3)),

    KeyList3 = maps:get(<<"raw_keys">>, MapResponse3) ++ KeyList2,

    MapResponse4 = get_results(HTTP_IPL, HTTP_PortL, QueueRef, 1, false),
    ?assertMatch(1, length(maps:get(<<"raw_keys">>, MapResponse4))),
    ?assertMatch(3, maps:get(<<"responses_count">>, MapResponse4)),

    KeyList4 = maps:get(<<"raw_keys">>, MapResponse4) ++ KeyList3,

    rt:wait_until(
        fun() ->
            MR = get_results(HTTP_IP, HTTP_Port, QueueRef, 0, true),
            maps:get(<<"received_count">>, MR) == ObjectCount
                andalso maps:get(<<"query_complete">>, MR) == true
        end
    ),

    MapResponse5 = get_results(HTTP_IPL, HTTP_PortL, QueueRef, 997, true),
    ?assertMatch(ObjectCount, length(maps:get(<<"raw_keys">>, MapResponse5)) + 3),
    ?assertMatch(ObjectCount, maps:get(<<"responses_count">>, MapResponse5)),
    
    KeyList5 = KeyList4 ++ maps:get(<<"raw_keys">>, MapResponse5),

    ExpList = lists:map(fun to_key/1, lists:seq(1, ObjectCount)),
    ?assertMatch(ExpList, lists:sort(KeyList5)),

    MapResponse6 = get_results(HTTP_IP, HTTP_Port, QueueRef, 100, false),
    ?assertMatch([], maps:get(<<"raw_keys">>, MapResponse6)),
    ?assertMatch(1000, maps:get(<<"responses_count">>, MapResponse6)),
    ?assertMatch(1000, maps:get(<<"received_count">>, MapResponse6)),
    ?assertMatch(true, maps:get(<<"query_complete">>, MapResponse6)),

    timer:sleep(2001),

    ok =
        get_result_error(
            HTTP_IPL,
            HTTP_PortL,
            QueueRef,
            1,
            500,
            "Internal Server Error",
            "result_server_terminated"
        ),

    ?LOG_INFO("Checking queue of raw_terms"),
    ?LOG_INFO(
        "Use a different node to set inactivity_timeout "
        "through alternate means"
    ),
    JSON2 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qfn\" : \"SMITH\", \"qgn\" : \"ANNE\"},
                \"accumulation_option\" : \"queue_raw_terms\",
                \"inactivity_timeout\" : 3,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650501\",
                            \"end_term\"   : \"19650501~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)\",
                            \"filter_expression\" : \"($fn = :qfn ) AND (:qgn IN $gn)\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ResultJson2}} =
        httpc:request(
            post,
            {
                lists:flatten(
                    io_lib:format(
                        "http://~s:~w/types/~s/buckets/~s/query",
                        [HTTP_IPL, HTTP_PortL, ?BTYPE, ?BNAME]
                    )
                ),
                [],
                "application/json",
                JSON2
            },
            [],
            [],
            test_client
        ),
    
    {struct, DecodedResponse2} = mochijson2:decode(ResultJson2),
    ?LOG_INFO("Response to query ~0p", [DecodedResponse2]),
    [{<<"result_reference">>, QueueRef2}] = DecodedResponse2,
    {AllRawTermResults, true} =
        lists:foldl(
            fun(I, {Acc, Complete}) ->
                case Complete of
                    true ->
                        {Acc, Complete};
                    false ->
                        RM =
                            get_results(
                                HTTP_IP,
                                HTTP_Port,
                                QueueRef2,
                                250,
                                true
                            ),
                        UpdAcc = maps:get(<<"raw_terms">>, RM) ++ Acc,
                        case {
                            maps:get(<<"query_complete">>, RM),
                            maps:get(<<"responses_count">>, RM),
                            maps:get(<<"received_count">>, RM)
                        } of
                            {true, RspC, RcvC} when RspC == RcvC ->
                                ?LOG_INFO("All results received in ~w loops", [I]),
                                {UpdAcc, true};
                            _ ->
                                timer:sleep(1),
                                {UpdAcc, false}
                        end
                end
            end,
            {[], false},
            lists:seq(1, 100)
        ),
    ?assertMatch(ObjectCount, length(AllRawTermResults)),
    [ExpectedTerm] = ?PEOPLE_TERMS,
    AllRawTermResultsKeys =
        lists:map(
            fun({struct, [{T, K}]}) ->
                case binary_to_list(T) of
                    LT when LT == ExpectedTerm ->
                        K
                end
            end,
            AllRawTermResults
        ),
    
    ?assertMatch(ExpList, lists:sort(AllRawTermResultsKeys)),

    timer:sleep(3001),

    ok =
        get_result_error(
            HTTP_IPL,
            HTTP_PortL,
            QueueRef2,
            1,
            500,
            "Internal Server Error",
            "result_server_terminated"
        ),

    ok = inets:stop(),
    ok.

get_results(HTTP_IP, HTTP_Port, QueueRef, MaxResults, Quiet) ->
    URI =
        lists:flatten(
                io_lib:format(
                    "http://~s:~w/types/~s/buckets/~s/query"
                    "?result_queue=~s&max_results=~w",
                    [HTTP_IP, HTTP_Port, ?BTYPE, ?BNAME, QueueRef, MaxResults]
                )
            ),
    
    ?LOG_INFO("Request to URI: ~s", [URI]),

    {T, {ok, {{?VERSION, 200, "OK"}, _, ResultJson}}} =
        timer:tc(
            fun() ->
                httpc:request(get, {URI, []}, [], [], test_client)
            end
        ),
    {struct, DecodedResponse} = mochijson2:decode(ResultJson),

    case Quiet of
        true -> ok;
        false -> ?LOG_INFO("Decoded response: ~0p", [DecodedResponse])
    end,
    ?LOG_INFO("Response received in ~w microseconds", [T]),

    maps:from_list(DecodedResponse).

get_result_error(IP, Port, QueueRef, MaxResults, Code, Type, Text) ->
    get_result_error(IP, Port, QueueRef, MaxResults, Code, Type, Text, ?BNAME).

get_result_error(IP, Port, QueueRef, MaxResults, Code, Type, Text, BName) ->
    URI =
        lists:flatten(
                io_lib:format(
                    "http://~s:~w/types/~s/buckets/~s/query"
                    "?result_queue=~s&max_results=~w",
                    [IP, Port, ?BTYPE, BName, QueueRef, MaxResults]
                )
            ),
    ?LOG_INFO("Request to URI: ~s", [URI]),

    {ok, {{?VERSION, ErrorCode, ErrorType}, Headers, ResultBody}} =
        httpc:request(get, {URI, []}, [], [], test_client),
    
    ?LOG_INFO( "Error check returned: ~w ~s", [ErrorCode, ErrorType]),
    ?LOG_INFO("Error body: ~0p", [ResultBody]),

    ?assertMatch(ErrorCode, Code),
    ?assertMatch(ErrorType, Type),
    case lists:member({"content-type","application/json"}, Headers) of
        true ->
            {struct, [{<<"error">>, ErrorText}]} = mochijson2:decode(ResultBody),
            ?assertMatch({match, _}, re:run(ErrorText, Text));
        false ->
            ?assertMatch({match, _}, re:run(ResultBody, Text))
    end,
    ok.

setup_data(Nodes, ObjectCount) ->
    PBPid = rt:pbc(hd(Nodes)),
    rt:create_and_activate_bucket_type(hd(Nodes), ?BTYPE, [{magic, true}]),
    ?LOG_INFO("Loading ~w objects with the same indexes", [ObjectCount]),
    lists:foreach(
        fun(I) ->
            ok =
                put_an_object(
                    PBPid,
                    {?BTYPE, ?BNAME},
                    to_key(I),
                    <<"foo">>,
                    [
                        {?PEOPLE_INDEX, ?PEOPLE_TERMS},
                        {?FAMILY_INDEX, ?FAMILY_TERMS},
                        {?GIVEN_INDEX, ?GIVEN_TERMS},
                        {?POSTCODE_INDEX, ?POSTCODE_TERMS},
                        {?REPORT_INDEX, ?REPORT_TERMS}
                    ]
                )
        end,
        lists:seq(1, ObjectCount)
    ).
    
put_an_object(Pid, Bucket, Key, Data, Indexes) when is_list(Indexes) ->
    ?LOG_DEBUG("Putting object ~0p", [Key]),
    FlatIndexes =
        lists:map(
            fun({F, TL}) ->
                case TL of
                    TL when is_list(TL) ->
                        lists:map(fun(T) -> {F, T} end, TL);
                    T ->
                        {F, T}
                end
            end,
            Indexes
        ),
    MetaData = dict:from_list([{<<"index">>, lists:flatten(FlatIndexes)}]),
    Robj0 = riakc_obj:new(Bucket, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

valid_key_query() ->
    "
        {
            \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qfn\" : \"SMITH\", \"qgn\" : \"ANNE\"},
            \"accumulation_option\" : \"queue_raw_keys\",
            \"query_list\" :
                [
                    {
                        \"index_name\" : \"peoplefinder_bin\",
                        \"start_term\" : \"19650501\",
                        \"end_term\"   : \"19650501~\",
                        \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)\",
                        \"filter_expression\" : \"($fn = :qfn ) AND (:qgn IN $gn)\"
                    }
                ]
        }
    ".