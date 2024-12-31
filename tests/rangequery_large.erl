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
-module(rangequery_large).
-behavior(riak_test).

-export([confirm/0]).
-export([spawn_profile_fun/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(RING_SIZE, 64).
-define(DEFAULT_BUCKET_PROPS, [{allow_mult, true}, {dvv_enabled, true}]).
-define(BTYPE, <<"Type1">>).
-define(BNAME, <<"Bucket1">>).
-define(POC_IDX, <<"pcdob_bin">>).
-define(NAME_IDX, <<"fngndob_bin">>).
-define(GP_IDX, <<"shagpdob_bin">>).
-define(KEYCOUNT_FACTOR, 4).
-define(KEYCOUNT, ?KEYCOUNT_FACTOR * 65536).

-record(index_results_v1,
    {keys, terms, continuation}
).

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
    Nodes = rt:build_cluster(6, ?CONFIG(?RING_SIZE)),
    ?LOG_INFO(
        "Loading ~w items of data with ~w index terms",
        [?KEYCOUNT, ?KEYCOUNT * 6]
    ),
    {LoadTime, ok} = timer:tc(fun() -> setup_data(Nodes) end),
    ?LOG_INFO(
        "Loading complete in ~w milliseconds",
        [LoadTime div 1000]
    ),
    query_tests(hd(Nodes)),
    pass.

query_tests(HdNode) ->
    HTTPC = rt:httpc(HdNode),
    % Find all Smiths
    NumberofSmiths = 5 * (?KEYCOUNT div 64),
    % spawn_profile_fun(HdNode),
    lists:foreach(
        fun(_I) ->
            secondary_index_comparison(HTTPC),
            query_test(
                HTTPC,
                ?NAME_IDX,
                {<<"Smith|">>, <<"Smith~">>},
                undefined,
                NumberofSmiths,
                NumberofSmiths,
                NumberofSmiths
            )
        end,
        lists:seq(1, 4)
    ),
    NumberofMos = 2 * (?KEYCOUNT div 64),
    query_test(
        HTTPC,
        ?NAME_IDX,
        {<<"Mo">>, <<"Mo~">>},
        undefined,
        NumberofMos,
        NumberofMos,
        NumberofMos
    ),
    % Find all Mo* born in 1980 - covers Moore and Morris
    Numberof1980Mos = (2 * (?KEYCOUNT div 64)) div 64,
    query_test(
        HTTPC,
        ?NAME_IDX,
        {<<"Mo">>, <<"Mo~">>},
        <<"[A-Z][a-z]*\\|1980.*">>,
        Numberof1980Mos,
        Numberof1980Mos,
        2 * (?KEYCOUNT div 64)
    ),
    % As before but different way of expressing regex
    query_test(
        HTTPC,
        ?NAME_IDX,
        {<<"Mo">>, <<"Mo~">>},
        <<"[^\\|]*\\|1980">>,
        Numberof1980Mos,
        Numberof1980Mos,
        2 * (?KEYCOUNT div 64)
    ),
    Numberof80sMs = ((4 * (?KEYCOUNT div 64)) div 64) * 10,
    query_test(
        HTTPC,
        ?NAME_IDX,
        {<<"M">>, <<"M~">>},
        <<"[A-Z][a-z]*\\|198.*">>,
        Numberof80sMs,
        Numberof80sMs,
        4 * (?KEYCOUNT div 64)
    ),
    % As before but different way of expressing regex
    query_test(
        HTTPC,
        ?NAME_IDX,
        {<<"M">>, <<"M~">>},
        <<"[^\\|]*\\|198">>,
        Numberof80sMs,
        Numberof80sMs,
        4 * (?KEYCOUNT div 64)
    ),
    NumberofLS1_endsA = ?KEYCOUNT div 8,
    query_test(
        HTTPC,
        ?POC_IDX,
        {<<"LS1_">>, <<"LS1_~">>},
        <<"LS1_[0-9][A-Z]A">>,
        NumberofLS1_endsA,
        NumberofLS1_endsA, % all the matches are different keys
        ?KEYCOUNT % There are four index entries per key
    ),
    NumberofLS1_1 = ((?KEYCOUNT div 4) div 32) * 11,
    query_test(
        HTTPC,
        ?POC_IDX,
        {<<"LS1_1">>, <<"LS1_1~">>},
        undefined,
        NumberofLS1_1,
        (NumberofLS1_1 div 11) * 32,
        ?KEYCOUNT div 4
    ),
    NumberofLS1_1_90s = (NumberofLS1_1 div 64) * 10,
    query_test(
        HTTPC,
        ?POC_IDX,
        {<<"LS1_1">>, <<"LS1_1~">>},
        <<"LS1_1[A-Z]{2}\\|199">>,
        NumberofLS1_1_90s,
        (((NumberofLS1_1 div 11) * 32) div 64) * 10,
        ?KEYCOUNT div 4
    ).

secondary_index_comparison(HTTPC) ->
    {TC4, {ok, #index_results_v1{keys=TwoIKeys}}} =
        timer:tc(
            fun() ->
                rhc:get_index(
                    HTTPC, {?BTYPE, ?BNAME}, ?NAME_IDX, {<<"Smith|">>, <<"Smith~">>}
                )
            end
        ),
    ?LOG_INFO(
        "2i query took ~w ms to find ~w results",
        [TC4 div 1000, length(TwoIKeys)]
    ).

query_test(
        HTTPC, Idx, Range, Regex, ExpCount, ExpMatches, ScanCount) ->
    B = {?BTYPE, ?BNAME},
    {TC1, {ok, {keys, Keys}}} =
        timer:tc(
            fun() ->
                rhc:range_query(HTTPC, B, Idx, Range, Regex, keys, [])
            end
        ),
    {TC2, {ok, {raw_keys, RawKeys}}} =
        timer:tc(
            fun() ->
                rhc:range_query(HTTPC, B, Idx, Range, Regex, raw_keys, [])
            end
        ),
    {TC3, {ok, {match_count, MC}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC, B, Idx, Range, Regex, match_count, [])
            end
        ),
    {TC4, {ok, {key_count, KC}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC, B, Idx, Range, Regex, key_count, [])
            end
        ),
    ?assertMatch(ExpCount, length(Keys)), 
    ?assertMatch(ExpMatches, length(RawKeys)),
    ?assertMatch(ExpMatches, MC), 
    ?assertMatch(ExpCount, KC),
    ?LOG_INFO(
        "Timings for finding ~w keys scanning ~w terms in ~w ~w ~w ~w",
        [
            ExpCount,
            ScanCount,
            TC1 div 1000,
            TC2 div 1000,
            TC3 div 1000,
            TC4 div 1000
        ]
    ).


setup_data(Nodes) ->
    PBPid = rt:pbc(hd(Nodes)),
    rt:create_and_activate_bucket_type(
        hd(Nodes), ?BTYPE, [{allow_mult, true}]
    ),
    lists:foreach(
        fun(I) ->
            ok = put_an_object(PBPid, I)
        end,
        lists:seq(1, ?KEYCOUNT)
    ).

put_an_object(Pid, N) ->
    case N rem 16384 of
        0 ->
            ?LOG_INFO("~w keys loaded", [N]);
        _ ->
            ok
    end,
    Key = to_key(N),
    Indexes =
        generate_postcode_indices(N) ++
        generate_provider_indices(N) ++
        generate_name_indices(N),
    Data = <<N:32/integer>>,
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new({?BTYPE, ?BNAME}, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

generate_postcode_indices(N) ->
    % Each record has 4 post codes
    lists:map(
        fun(I) -> 
            PC = to_postcode(I),
            DOB = to_dob(N),
            {?POC_IDX, <<PC/binary, <<"|">>/binary, DOB/binary>>}
        end,
        lists:seq(N, N + 3)
    ).

generate_provider_indices(N) ->
    % Each record has one and only one provider
    SHA = to_sha(N),
    GP = to_gp(N),
    DOB = to_dob(N),
    [{?GP_IDX, <<SHA/binary, GP/binary, DOB/binary>>}].

generate_name_indices(N) ->
    % Each record has one and only one family name
    FN = to_familyname(N),
    GN1 = to_givenname(N),
    GN2 = to_givenname(N + 1),
    DOB = to_dob(N),
    [
        {
            ?NAME_IDX,
            <<FN/binary,
                <<"|">>/binary,
                DOB/binary,
                <<"|">>/binary,
                GN1/binary, GN2/binary
            >>
        }
    ]. 

to_key(N) ->
    list_to_binary(io_lib:format("NHS~16..0B", [N])).

to_postcode(N) ->
    District = ((N div 32) rem 4) +  1,
    Sector = 
        lists:nth(
            (N rem 32) + 1,
            [
                "1AA", "1AB", "1AC", "1AD", "1AE", "1AF", "1AG", "1AH",
                "2BA", "2BB", "2BC", "2BD", "2BE", "2BF", "2BG", "2BH",
                "3AA", "3AB", "3AC", "3AD", "3AE", "3AF", "3AG", "3AH",
                "4CA", "4CB", "4CC", "4CD", "4CE", "4CF", "4CG", "4CH"
            ]
        ),
    list_to_binary(io_lib:format("LS~w_~s", [District, Sector])).

to_givenname(N) ->
    lists:nth(
        ((N div 64) rem 16) + 1,
        [
            <<"Noah">>, <<"Oliver">>, <<"George">>, <<"Arthur">>,
            <<"Muhammad">>, <<"Leo">>, <<"Harry">>, <<"Oscar">>,
            <<"Olivia">>, <<"Amelia">>, <<"Isla">>, <<"Ava">>,
            <<"Ivy">>, <<"Freya">>, <<"Lily">>, <<"Florence">>
        ]
    ).

to_familyname(N) ->
    lists:nth(
        (N rem 64) + 1,
        [
            <<"Smith">>, <<"Smith">>, <<"Smith">>, <<"Smith">>, <<"Smith">>,
            <<"Jones">>, <<"Jones">>, <<"Jones">>, <<"Taylor">>, <<"Taylor">>,
            <<"Brown">>, <<"Brown">>, <<"Williams">>, <<"Williams">>,
            <<"Wilson">>, <<"Johnson">>, <<"Davies">>, <<"Patel">>, <<"Robinson">>,
            <<"Wright">>, <<"Thompson">>, <<"Evans">>, <<"Walker">>, <<"White">>,
            <<"Roberts">>, <<"Green">>, <<"Hall">>, <<"Thomas">>, <<"Clarke">>,
            <<"Jackson">>, <<"Wood">>, <<"Harris">>, <<"Edwards">>, <<"Turner">>,
            <<"Martin">>, <<"Cooper">>, <<"Hill">>, <<"Ward">>, <<"Hughes">>,
            <<"Moore">>, <<"Clark">>, <<"King">>, <<"Harrison">>, <<"Lewis">>,
            <<"Baker">>, <<"Lee">>, <<"Allen">>, <<"Morris">>, <<"Khan">>,
            <<"Scott">>, <<"Watson">>, <<"Davis">>, <<"Parker">>, <<"James">>,
            <<"Bennett">>, <<"Young">>, <<"Phillips">>, <<"Richardson">>, <<"Mitchell">>,
            <<"Bailey">>, <<"Carter">>, <<"Cook">>, <<"Singh">>, <<"Shaw">>
        ]
    ).

to_sha(N) ->
    list_to_binary(io_lib:format("SHA~4..0B", [(N rem 4) + 1])).

to_gp(N) ->
    list_to_binary(io_lib:format("SHA~4..0B", [((N div 4) rem 64) + 1])).

to_dob(N) ->
    YOB = 1960 + ((N div 128) rem 64),
    MOB = (N div 8192) rem 8,
    DOB = (N div 65536) rem 16,
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0B", [YOB, MOB, DOB])).
    
spawn_profile_fun(Node) ->
    spawn(
        fun() ->
            lists:foreach(
                fun(_I) ->
                    erpc:call(Node, riak_kv_util, profile_riak, [50]),
                    timer:sleep(10)
                end,
                lists:seq(1, 10)
            )
        end
    ).