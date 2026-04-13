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
-module(query_json).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
% -include_lib("stdlib/include/assert.hrl").

-define(RING_SIZE, 16).
-define(DEFAULT_BUCKET_PROPS, [{allow_mult, true}, {dvv_enabled, true}]).
-define(BTYPE, <<"Type1">>).
-define(BNAME, <<"Bucket1">>).

-define(KEY, <<"9000000001">>).
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
            Nodes = rt:build_cluster(1, ?CONFIG(?RING_SIZE)),
            ok = setup_data(Nodes),
            ok = test_peoplefinder_query(hd(Nodes));
        OtherBackend ->
            ?LOG_INFO("Backend ~0p does not support query API", [OtherBackend])
    end,
    pass.

test_peoplefinder_query(Node) ->
    ok = inets:start(),
    {ok, {HTTP_IP, HTTP_Port}} = rt:get_http_conn_info(Node),
    {ok, _HTTPC} = inets:start(httpc, [{profile, test_client}]),
    ok = httpc:set_options([{verbose, false}], test_client),

    ?LOG_INFO("Test basic eval and filter"),
    JSON1 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qfn\" : \"SMITH\", \"qgn\" : \"ANNE\"},
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

    URI =
        lists:flatten(
            io_lib:format(
                "http://~s:~w/types/~s/buckets/~s/query",
                [HTTP_IP, HTTP_Port, ?BTYPE, ?BNAME]
            )
        ),
    ExpectedKeys =
        lists:flatten(
            io_lib:format("{\"keys\":[\"~s\"]}", [?KEY])
        ),
    ExpectedCount = "{\"raw_count\":1}",
    ExpectedAgeCount = "{\"term_with_rawcount\":{\"60\":1}}",

    ?LOG_INFO("Uri: ~s", [URI]),
    ?LOG_INFO("ExpectedKeys: ~s", [ExpectedKeys]),
    ?LOG_INFO("ExpectedCount: ~s", [ExpectedCount]),

    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
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

    ?LOG_INFO("Test avoiding filter by narrowing range"),
    JSON2 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qgn\" : \"ANNE\"},
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650501|SMITH|\",
                            \"end_term\"   : \"19650501|SMITH|~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)\",
                            \"filter_expression\" : \"(:qgn IN $gn)\"
                        }
                    ]
            }
        ",

    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON2
            },
            [],
            [],
            test_client
        ),
    
    ?LOG_INFO("Test use of regular expression as alternative - note double escaping"),
    JSON3 =
        "{
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650501|SMITH|\",
                            \"end_term\"   : \"19650501|SMITH|~\",
                            \"regular_expression\" : \"[^\\\\|]*\\\\|[^\\\\|]*\\\\|[^\\\\|]*ANNE\"
                        }
                    ]
            }
        ",
        % Note the extra escaping required here
        % The example in QueryAPI.md works if you post it as a file
        % - but extra escaping if you try posting as a string, anything escaped
        % within the regular expression needs to be double-escaped when placed
        % in json as a string 

    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON3
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test alternative eval expression"),
    JSON4 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qfn_begins\" : \"SM\", \"qgn\" : \"ANNE\", \"qbd\" : \"0501\"},
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650101\",
                            \"end_term\"   : \"19691231~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn) | index($dob, 4, 4, $birthday)\",
                            \"filter_expression\" : \"begins_with($fn, :qfn_begins) AND (:qgn IN $gn) AND ($birthday = :qbd)\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON4
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test use of regex within filter expression"),
    JSON5 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"dl2\" : \".\", \"qgn\" : \"ANNE\", \"fn_regex\" : \"(?P<fn_match>SM[A-Z]+TH)\"},
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"peoplefinder_bin\",
                            \"start_term\" : \"19650501\",
                            \"end_term\"   : \"19650501~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($dob, $fn, $gn, $pc)) | regex($fn, :fn_regex, ($fn_match)) | split($gn, :dl2, $gn)\",
                            \"filter_expression\" : \"attribute_exists($fn_match) AND (:qgn IN $gn)\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON5
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test basic eval and filter - alternative index strategy"),
    JSON6 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"low_dob\" : \"19650101\", \"high_dob\" : \"19650531\"},
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"familyname_bin\",
                            \"start_term\" : \"SMITH|\",
                            \"end_term\"   : \"SMITH~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($fn, $dob, $edates))\",
                            \"filter_expression\" : \"$dob BETWEEN :low_dob AND :high_dob\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON6
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test alternative index strategy - effective date filtering"),
    JSON7 =
        "
            {
                \"substitutions\" : {\"dl1\" : \"|\", \"low_dob\" : \"19650101\", \"high_dob\" : \"19650531\", \"effective_date\" : \"19800101\"},
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"postcode_bin\",
                            \"start_term\" : \"LS9_\",
                            \"end_term\"   : \"LS9_~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($pc, $dob, $edates)) | index($edates, 0, 8, $start_date) | index($edates, 8, 8, $end_date)\",
                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob) AND (:effective_date BETWEEN $start_date AND $end_date)\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON7
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test alternative index strategy - compound query"),
    JSON8 =
        "
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"substitutions\" : {\"dl1\" : \"|\", \"low_dob\" : \"19650101\", \"high_dob\" : \"19650531\", \"effective_date\" : \"19800101\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"postcode_bin\",
                            \"start_term\" : \"LS9_\",
                            \"end_term\"   : \"LS9_~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($pc, $dob, $edates)) | index($edates, 0, 8, $start_date) | index($edates, 8, 8, $end_date)\",
                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob) AND (:effective_date BETWEEN $start_date AND $end_date)\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"familyname_bin\",
                            \"start_term\" : \"SMITH|\",
                            \"end_term\"   : \"SMITH~\",
                            \"evaluation_expression\" : \"delim($term, :dl1, ($fn, $dob, $edates))\",
                            \"filter_expression\" : \"$dob BETWEEN :low_dob AND :high_dob\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedKeys}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON8
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test report index - basic query"),
    JSON9 =
        "
            {
                \"accumulation_option\" : \"raw_count\",
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"healthreport_bin\",
                            \"start_term\" : \"SHA0001\",
                            \"end_term\"   : \"SHA0001~\",
                            \"evaluation_expression\" : \"index($term, 15, 8, $dob) | index($term, 23, 1, $agc) | index($term, 24, 1, $smoker)\",
                            \"filter_expression\" : \"($dob <= \\\"19650530\\\") AND ($agc = \\\"F\\\") AND ($smoker = \\\"Y\\\")\"
                        }
                    ]
            }
        ",
    {ok, {{?VERSION, 200, "OK"}, _, ExpectedCount}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON9
            },
            [],
            [],
            test_client
        ),

    ?LOG_INFO("Test report index - advanced count with filter-based calculation"),
    JSON10 =
        "
            {
                \"substitutions\" : {\"current_date\" : \"0530\"},
                \"accumulation_option\" : \"term_with_rawcount\",
                \"accumulation_term\" : \"age\",
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"healthreport_bin\",
                            \"start_term\" : \"SHA0001\",
                            \"end_term\"   : \"SHA0001~\",
                            \"evaluation_expression\" : \"index($term, 15, 8, $dob) | index($term, 23, 1, $agc) | index($term, 24, 1, $smoker) | index($dob, 0, 4, $yob) | to_integer($yob, $yob) | index($dob, 4, 4, $birthday) | map($birthday, <=, ((:current_date, 2025)), 2024, $yoc) | subtract($yoc, $yob, $age) | to_string($age, $age)\",
                            \"filter_expression\" : \"($dob <= \\\"19650530\\\") AND ($agc = \\\"F\\\") AND ($smoker = \\\"Y\\\")\"
                        }
                    ]
            }
        ",
    {ok, {{Version, ResponseCode, ReasonPhrase}, Headers, Body}} =
        httpc:request(
            post,
            {
                URI,
                [],
                "application/json",
                JSON10
            },
            [],
            [],
            test_client
        ),
    ?LOG_INFO("Version ~p", [Version]),
    ?LOG_INFO("ResponseCode ~p", [ResponseCode]),
    ?LOG_INFO("ReasonPhrase ~p", [ReasonPhrase]),
    ?LOG_INFO("Headers ~p", [Headers]),
    ?LOG_INFO("Body ~p", [Body]),

    {ok, {{?VERSION, 200, "OK"}, _, ExpectedAgeCount}}  = {ok, {{Version, ResponseCode, ReasonPhrase}, Headers, Body}},

    ok = inets:stop(),
    ok.

setup_data(Nodes) ->
    PBPid = rt:pbc(hd(Nodes)),
    rt:create_and_activate_bucket_type(hd(Nodes), ?BTYPE, [{magic, true}]),
    ok =
        put_an_object(
            PBPid,
            {?BTYPE, ?BNAME},
            ?KEY,
            <<"foo">>,
            [
                {?PEOPLE_INDEX, ?PEOPLE_TERMS},
                {?FAMILY_INDEX, ?FAMILY_TERMS},
                {?GIVEN_INDEX, ?GIVEN_TERMS},
                {?POSTCODE_INDEX, ?POSTCODE_TERMS},
                {?REPORT_INDEX, ?REPORT_TERMS}
            ]
        ).
    
put_an_object(Pid, Bucket, Key, Data, Indexes) when is_list(Indexes) ->
    ?LOG_INFO("Putting object ~0p", [Key]),
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
