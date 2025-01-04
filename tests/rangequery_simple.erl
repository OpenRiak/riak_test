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
-module(rangequery_simple).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(RING_SIZE, 16).
-define(DEFAULT_BUCKET_PROPS, [{allow_mult, true}, {dvv_enabled, true}]).
-define(BTYPE, <<"Type1">>).
-define(BNAME, <<"Bucket1">>).
-define(KEY1, <<"SingleKey1">>).
-define(KEY2, <<"SingleKey2">>).
-define(INDEX1, <<"index1_bin">>).
-define(INDEX2, <<"index2_bin">>).
-define(IDXV1, <<"PRINCE|19580607">>).
-define(IDXV2, <<"AFKAP|19580607">>).
-define(IDXV3, <<"EASTON|19590427">>).
-define(ERROR_QEPREFIX, "Validation failure at stage query_evaluation due to").

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
    Nodes = rt:build_cluster(1, ?CONFIG(?RING_SIZE)),
    ok = setup_data(Nodes),
    ok = test_client_query(Nodes, http),
    ok = test_client_invalid_query(Nodes, http),
    pass.

test_client_query(Nodes, http) ->
    HTTPC = rt:httpc(hd(Nodes)),
    test_basic_range(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_range(HTTPC, ?BNAME),
    test_basic_keycount(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_keycount(HTTPC, ?BNAME),
    test_basic_matchcount(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_matchcount(HTTPC, ?BNAME),
    test_basic_regex(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_regex(HTTPC, ?BNAME),
    test_basic_range_returning_terms(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_range_returning_terms(HTTPC, ?BNAME),
    test_basic_filter_query(HTTPC, {?BTYPE, ?BNAME}),
    test_basic_filter_query(HTTPC, ?BNAME),
    test_combo_query(HTTPC, {?BTYPE, ?BNAME}),
    test_combo_query(HTTPC, ?BNAME).

test_client_invalid_query(Nodes, http) ->
    ?LOG_INFO("Test error responses for http"),
    Client = rt:httpc(hd(Nodes)),
    {error, E0} =
        rhc:range_query(Client, ?BNAME, <<"index1_int">>, {<<"0">>, <<"1">>}),
    ?assertMatch(
        ?ERROR_QEPREFIX " Invalid index name",
        E0
    ),
    {error, E2} = rhc:range_query(Client, ?BNAME, ?INDEX1, {<<"B">>, <<"A">>}),
    ?assertMatch(
        ?ERROR_QEPREFIX " Invalid query range",
        E2
    ),
    {error, E4} = 
        rhc:range_query(
            Client,
            ?BNAME,
            ?INDEX1,
            {<<"A">>, <<"B">>},
            <<"[A-Z]+\\|1959[0-9]+)">>,
            keys,
            []
        ),
    ?assertMatch(
        ?ERROR_QEPREFIX " Invalid regex",
        E4
    ).


% -define(ERROR_AOPREFIX, "Validation failure at stage accumulation_option due to").

% test_client_invalid_type(Nodes, http) ->
%     ?LOG_INFO("Test error responses for http - where type is invalid"),
%     Client = rt:httpc(hd(Nodes)),
%     {error, E1} = rhc:range_query(Client, ?BNAME, ?INDEX1, {0, 1}),
%     ?assertMatch(
%         ?ERROR_QEPREFIX " Invalid query range",
%         E1
%     ),
%     {error, E3} = 
%         rhc:range_query(
%             Client,
%             ?BNAME,
%             ?INDEX1,
%             {<<"A">>, <<"B">>},
%             undefined,
%             matches,
%             []
%         ),
%     ?assertMatch(
%         ?ERROR_AOPREFIX " Unrecognised option <<\"matches\">>",
%         E3
%     ).

test_basic_range(Client, Bucket) ->
    ?LOG_INFO("Test basic range with ~0p ~0p", [Client, Bucket]),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client, Bucket, ?INDEX1, {<<"A">>, <<"Q">>}
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client, Bucket, ?INDEX2, {<<"A">>, <<"Q">>}
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client, Bucket, ?INDEX1, {<<"A">>, <<"F">>}
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client, Bucket, ?INDEX1, {<<"E">>, <<"Q">>}
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1]}},
        rhc:range_query(
            Client, Bucket, ?INDEX1, {<<"A">>, <<"B">>}
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY2]}},
        rhc:range_query(
            Client, Bucket, ?INDEX1, {<<"E">>, <<"F">>}
        )
    ).

test_basic_range_returning_terms(Client, Bucket) ->
    ?LOG_INFO(
        "Test basic range returning terms with ~0p ~0p",
        [Client, Bucket]
    ),
    ?assertMatch(
        {
            ok,
            {
                terms,
                [
                    {struct, [{?IDXV2, ?KEY1}]},
                    {struct, [{?IDXV3, ?KEY2}]},
                    {struct, [{?IDXV1, ?KEY1}]}
                ]
            }
        },
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            undefined,
            terms,
            []
        )
    ),
    ?assertMatch(
        {
            ok,
            {
                terms,
                [
                    {struct, [{?IDXV3, ?KEY2}]},
                    {struct, [{?IDXV1, ?KEY1}]}
                ]
            }
        },
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            undefined,
            terms,
            []
        )
    ).

test_basic_keycount(Client, Bucket) ->
    ?LOG_INFO("Test basic keycount with ~0p ~0p", [Client, Bucket]),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            undefined, count, []
        )
    ),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            undefined, count, []
        )
    ),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"F">>},
            undefined, count, []
        )
    ),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"E">>, <<"Q">>},
            undefined, count, []
        )
    ),
    ?assertMatch(
        {ok, {count, 1}},
        rhc:range_query(
            Client, Bucket,
            ?INDEX1,
            {<<"A">>, <<"B">>},
            undefined, count, []
        )
    ),
    ?assertMatch(
        {ok, {count, 1}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"E">>, <<"F">>},
            undefined, count, []
        )
    ).

test_basic_matchcount(Client, Bucket) ->
    ?LOG_INFO("Test basic matchcount with ~0p ~0p", [Client, Bucket]),
    ?assertMatch(
        {ok, {raw_count, 3}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            undefined, raw_count, []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            undefined, raw_count, []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"F">>},
            undefined, raw_count, []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"E">>, <<"Q">>},
            undefined, raw_count, []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 1}},
        rhc:range_query(
            Client, Bucket,
            ?INDEX1,
            {<<"A">>, <<"B">>},
            undefined, raw_count, []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 1}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"E">>, <<"F">>},
            undefined, raw_count, []
        )
    ).

test_basic_regex(Client, Bucket) ->
    ?LOG_INFO("Test basic regex query with ~0p ~0p", [Client, Bucket]),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|195[0-9]+">>,
            keys,
            []
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|195[0-9]+">>,
            keys,
            []
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY2]}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|1959[0-9]+">>,
            keys,
            []
        )
    ),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|195[0-9]+">>,
            count,
            []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 3}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|195[0-9]+">>,
            raw_count,
            []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 2}},
        rhc:range_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            <<"[A-Z]+\\|195[0-9]+">>,
            raw_count,
            []
        )
    ).

test_basic_filter_query(Client, Bucket) ->
    ?LOG_INFO("Test basic filter query with ~0p ~0p", [Client, Bucket]),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:filter_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
            <<"$yob = :yob1 OR $yob = :yob2">>,
            keys,
            undefined,
            #{<<"yob1">> => <<"1958">>, <<"yob2">> => <<"1959">>},
            []
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:filter_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
            <<"$yob IN (:yob1, :yob2)">>,
            keys,
            undefined,
            #{<<"yob1">> => <<"1958">>, <<"yob2">> => <<"1959">>},
            []
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY2]}},
        rhc:filter_query(
            Client,
            Bucket,
            ?INDEX2,
            {<<"A">>, <<"Q">>},
            <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
            <<"$yob = :yob">>,
            keys,
            undefined,
            #{<<"yob">> => <<"1959">>},
            []
        )
    ),
    ?assertMatch(
        {ok, {count, 2}},
        rhc:filter_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
            <<"$yob = :yob1 OR $yob = :yob2">>,
            count,
            undefined,
            #{<<"yob1">> => <<"1958">>, <<"yob2">> => <<"1959">>},
            []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 3}},
        rhc:filter_query(
            Client,
            Bucket,
            ?INDEX1,
            {<<"A">>, <<"Q">>},
            <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
            <<"$yob = :yob1 OR $yob = :yob2">>,
            raw_count,
            undefined,
            #{<<"yob1">> => <<"1958">>, <<"yob2">> => <<"1959">>},
            []
        )
    ).


test_combo_query(Client, Bucket) ->
    ?LOG_INFO("Test basic combo query with ~0p ~0p", [Client, Bucket]),
    Q1 =
        rhc:make_query(
            1,
            ?INDEX1,
            {<<"P">>, <<"Q">>},
            {
                <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
                <<"$yob = :yob1">>
            }
        ),
    Q2 =
        rhc:make_query(
            2,
            ?INDEX2,
            {<<"E">>, <<"F">>},
            {
                <<"delim($term, \"|\", ($fn, $dob)) | index($dob, 0, 4, $yob)">>,
                <<"$yob = :yob2">>
            }
        ),
    SubsMap = #{<<"yob1">> => <<"1958">>, <<"yob2">> => <<"1959">>},
    ?assertMatch(
        {ok, {raw_count, 2}},
        rhc:combo_query(
            Client,
            Bucket,
            raw_count,
            SubsMap,
            <<"$1 UNION $2">>,
            [Q1, Q2],
            []
        )
    ),
    ?assertMatch(
        {ok, {keys, [?KEY1, ?KEY2]}},
        rhc:combo_query(
            Client,
            Bucket,
            keys,
            SubsMap,
            <<"$1 UNION $2">>,
            [Q1, Q2],
            []
        )
    ),
    ?assertMatch(
        {ok, {raw_count, 0}},
        rhc:combo_query(
            Client,
            Bucket,
            raw_count,
            SubsMap,
            <<"$1 INTERSECT $2">>,
            [Q1, Q2],
            []
        )
    ).


setup_data(Nodes) ->
    PBPid = rt:pbc(hd(Nodes)),
    rt:create_and_activate_bucket_type(hd(Nodes), ?BTYPE, [{magic, true}]),
    ok =
        put_an_object(
            PBPid,
            {?BTYPE, ?BNAME},
            ?KEY1,
            <<"foo">>,
            [{?INDEX1, ?IDXV1}, {?INDEX1, ?IDXV2}, {?INDEX2, ?IDXV1}]
        ),
    ok =
        put_an_object(
            PBPid,
            ?BNAME,
            ?KEY1,
            <<"foo">>,
            [{?INDEX1, ?IDXV1}, {?INDEX1, ?IDXV2}, {?INDEX2, ?IDXV1}]
        ),
    ok =
        put_an_object(
            PBPid,
            {?BTYPE, ?BNAME},
            ?KEY2,
            <<"bar">>,
            [{?INDEX1, ?IDXV3}, {?INDEX2, ?IDXV3}]
        ),
    ok =
        put_an_object(
            PBPid,
            ?BNAME,
            ?KEY2,
            <<"bar">>,
            [{?INDEX1, ?IDXV3}, {?INDEX2, ?IDXV3}]
        ).
    

put_an_object(Pid, Bucket, Key, Data, Indexes) when is_list(Indexes) ->
    ?LOG_INFO("Putting object ~0p", [Key]),
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(Bucket, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

    
