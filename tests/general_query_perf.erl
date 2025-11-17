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
%% A single node test that tests queries run through different strategies
%% which may be configured to run large data-sets for testing
%% 
%% Not intended to be part of regular test runs, but may be used to compare
%% performance between releases, and hardware platforms.

-module(general_query_perf).
-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include("postal_area.hrl").
-include("family_name.hrl").
-include("given_name.hrl").

-define(RING_SIZE, 16).
-define(ALLOW_MULT, false).
-define(N_VAL, 1).
-define(HOT_POSTCODE_CHANCE, 0.01).
-define(MEAN_POSTCODES, 5).
-define(PREVIOUS_FNAME_CHANCE, 0.3).
-define(CURRENT_YEAR, 2025).
-define(EXISTING, "99999999").
-define(CLIENT_COUNT, 4).
-define(TOTAL_KEYS, 1000000).
-define(APPLY_BRAKES_FOR_LAST, 2000). % per client
-define(QUERY_LOOPS, 10).
-define(POST_LOAD_PAUSE, 10000).

-define(CONF,
    [
        {riak_kv,
            [
                {anti_entropy, {off, []}},
                {delete_mode, keep},
                {tictacaae_active, active},
                {tictacaae_parallelstore, leveled_ko},
                {tictacaae_storeheads, true},
                {tictacaae_rebuildtick, 3600000},
                {tictacaae_suspend, true}
            ]
        },
        {leveled,
            [
                {compression_method, zstd}
            ]
        },
        {riak_core,
            [
                {ring_creation_size, ?RING_SIZE},
                {handoff_concurrency, 8},
                {forced_ownership_handoff, 8},
                {vnode_inactivity_timeout, 4000},
                {vnode_management_timer, 2000},
                {
                    default_bucket_props,
                    [{allow_mult, ?ALLOW_MULT}, {n_val, ?N_VAL}]
                }
            ]
        }
    ]
).

confirm() ->
    [Node] = rt:build_cluster(1, ?CONF),
    rt:wait_for_service(Node, riak_kv),
    Bucket = get_bucketprefix(Node, true),
    load_data(Node, Bucket),
    timer:sleep(?POST_LOAD_PAUSE),
    ?LOG_INFO("Running count tests on report index"),
    loop_and_log_results(fun() -> count_test(Node, Bucket) end),
    ?LOG_INFO("Running key fetch tests on postcode index"),
    loop_and_log_results(fun() -> postcode_test(Node, Bucket) end),
    pass.

loop_and_log_results(ResultFun) ->
    ResultListRaw =
        lists:foldl(
            fun(_I, Acc) ->
                Rs = ResultFun(),
                case Acc of
                    none ->
                        Rs;
                    Acc when is_list(Acc), length(Acc) == length(Rs) ->
                        lists:map(
                            fun({A, R}) -> A + R end,
                            lists:zip(Acc, Rs)
                        )
                end
            end,
            none,
            lists:seq(1, ?QUERY_LOOPS)
        ),
    ?LOG_INFO(
        "Timings ~0p",
        [lists:map(fun(T) -> T div ?QUERY_LOOPS end, ResultListRaw)]
    ).


get_bucketprefix(Node, true) ->
    rt:create_activate_and_wait_for_bucket_type(
        [Node],
        <<"BucketTypeName">>,
        [{allow_mult, ?ALLOW_MULT}]
    ),
    {<<"BucketTypeName">>, <<"BucketName">>}.

load_data(Node, Bucket) ->
    Me = self(),
    lists:foreach(
        fun(I) ->
            C = rt:pbc(Node),
            spawn(
                fun() ->
                    ?LOG_INFO("Client ~w spawned", [I]),
                    pbc_load(
                        C,
                        I,
                        Bucket,
                        0,
                        ?TOTAL_KEYS div ?CLIENT_COUNT,
                        false
                    ),
                    Me ! complete
                end
            )
        end,
        lists:seq(1, ?CLIENT_COUNT)
    ),
    ok = receive_loop(?CLIENT_COUNT),
    ok.

receive_loop(0) ->
    ok;
receive_loop(CountDown) ->
    receive
        complete ->
            receive_loop(CountDown - 1)
    end.

generate_record() ->
    {YOB, DOB} = generate_dob(),
    PostCodeCount =
        rand:uniform(
            max(
                1,
                min((?CURRENT_YEAR - YOB) div 4, 2 * ?MEAN_POSTCODES)
            )
        ),
    [{CA, CurrentPostCode}|PreviousPostCodes] =
        lists:map(
            fun(_I) ->
                generate_postcode(?HOT_POSTCODE_MAP, ?HOT_POSTCODE_CHANCE)
            end,
            lists:seq(1, max(1, PostCodeCount))
        ),
    PCEffectiveWindows =
        case length(PreviousPostCodes) of
            0 ->
                [
                    {
                        CurrentPostCode,
                        {DOB, ?EXISTING}
                    }
                ];
            N ->
                Spacing = (?CURRENT_YEAR - YOB) div N,
                {FED, PrevPCs} =
                    lists:foldl(
                        fun({_PA, PC}, {LED, PCAcc}) ->
                            LEY = list_to_integer(lists:sublist(LED, 4)),
                            NEY = LEY + Spacing,
                            {MED, DED} = generate_date(NEY),
                            NED = flatten_date(NEY, MED, DED),
                            {
                                NED,
                                [{PC, {LED, NED}} | PCAcc]
                            }
                        end,
                        {DOB, []},
                        PreviousPostCodes
                    ),
                [{CurrentPostCode, {FED, ?EXISTING}}|PrevPCs]
        end,
    CurrentFamilyName = generate_familyname(),
    PreviousFamilyNames =
        case rand:uniform() of
            RN when RN < ?PREVIOUS_FNAME_CHANCE ->
                [generate_familyname()];
            _ ->
                []
        end,
    {CurrentGivenName, OtherGivenNames} = generate_givenname(),
    {SHA, GP} = generate_provider(CA),
    #{
        date_of_birth => DOB,
        current_postcode => CurrentPostCode,
        historic_postcodes => PCEffectiveWindows,
        current_fname => CurrentFamilyName,
        historic_fnames => PreviousFamilyNames,
        preferred_gname => CurrentGivenName,
        other_gnames => OtherGivenNames,
        sha => SHA,
        gp_provider => GP,
        status_flags => generate_status_flags(YOB, CA)
    }.

generate_postcode(HotPostCodes, PostCodeChance) ->
    {AC, Area, DC} =
        case rand:uniform() of
            N when N < PostCodeChance, HotPostCodes /= none ->
                maps:get(rand:uniform(?HOT_POSTCODE_COUNT), HotPostCodes);
            _ ->
                maps:get(rand:uniform(?POSTAL_AREA_COUNT), ?POSTAL_AREA_MAP)
        end,
    case DC of
        DC when is_integer(DC) ->
            {
                Area,
                lists:flatten(
                    io_lib:format(
                        "~s~p_~s~s~s",
                        [
                            AC,
                            rand:uniform(DC),
                            [(64 + rand:uniform(26))],
                            [51 + rand:uniform(5)],
                            [51 + rand:uniform(5)]
                        ]
                    )
                )
            };
        ActualPostCode ->
            {Area, ActualPostCode}
    end.

generate_familyname() ->
    binary_to_list(
        maps:get(rand:uniform(?FAMILY_NAME_COUNT), ?FAMILY_NAME_MAP)
    ).

generate_givenname() ->
    {GN0, GN1, GN2, GN3} =
        maps:get(rand:uniform(?GIVEN_NAME_COUNT), ?GIVEN_NAME_MAP),
    {
        binary_to_list(GN0),
        lists:map(
            fun binary_to_list/1,
            lists:usort([GN1, GN2, GN3]) -- [GN0]
        )
    }.

generate_provider(CurrentArea) ->
    SHA = erlang:phash2(CurrentArea) band 15,
    GP = rand:uniform(500),
    SHA_ID = io_lib:format("SHA01~3..0B", [SHA]),
    GP_ID = io_lib:format("GP~3..0B~3..0b", [SHA, GP]),
    {SHA_ID, GP_ID}.

generate_dob() ->
    YOB = ?CURRENT_YEAR - rand:uniform(100),
    {MOB, DOB} = generate_date(YOB),
    {YOB, flatten_date(YOB, MOB, DOB)}.

flatten_date(Y, M, D) ->
    io_lib:format("~4..0B~2..0B~2..0B", [Y, M, D]).

generate_date(YOB) ->
    MOB = rand:uniform(12),
    DOB =
        case MOB of
            M when M == 2, YOB =/= 2000, YOB band 3 == 0 ->
                rand:uniform(29);
            M when M == 2 ->
                rand:uniform(28);
            M when M == 9; M == 4; M == 6; M == 1 ->
                rand:uniform(30);
            _ ->
                rand:uniform(31)
        end,
    {MOB, DOB}.

generate_id(Worker, Count) ->
    io_lib:format("HSS0~2..0B~8..0B", [Worker, Count]).

pbc_load(Client, ClientID, Bucket, Total, Total, _Brake) ->
    ?LOG_INFO(
        "ClientID ~0p finished load of ~w records into Bucket ~0p",
        [ClientID, Total, Bucket]
    ),
    riakc_pb_socket:stop(Client);
pbc_load(Client, ClientID, Bucket, RecordNumber, Total, Brake) ->
    ApplyBrake =
        case RecordNumber rem 1000 of
            0 when RecordNumber > 0 ->
                ?LOG_INFO(
                    "Client ~w has loaded ~w records of ~w",
                    [ClientID, RecordNumber, Total]
                ),
                case Total - RecordNumber of
                    ToGo when ToGo =< ?APPLY_BRAKES_FOR_LAST ->
                        true;
                    _ ->
                        false
                    end;
            _ ->
                Brake
        end,
    ID = generate_id(ClientID, RecordNumber),
    PatientRecord = generate_record(),
    IdxMap = generate_indexes(PatientRecord),
    Obj =
        riakc_obj:new(
            Bucket,
            list_to_binary(ID),
            term_to_binary(PatientRecord)
        ),
    MD0 = riakc_obj:get_metadata(Obj),
    MD1 =
        maps:fold(
            fun(IdxField, IdxValues, MDAcc) ->
                riakc_obj:set_secondary_index(
                    MDAcc,
                    {
                        {binary_index, IdxField},
                        lists:map(fun erlang:list_to_binary/1, IdxValues)
                    }
                )
            end,
            MD0,
            IdxMap
        ),
    ok = riakc_pb_socket:put(Client, riakc_obj:update_metadata(Obj, MD1)),
    case ApplyBrake of
        true ->
            timer:sleep(10);
        _ ->
            ok
    end,
    pbc_load(Client, ClientID, Bucket, RecordNumber + 1, Total, ApplyBrake).

generate_indexes(PatientRecord) ->
    #{
        <<"peoplefinder">> => generate_pfinder1(PatientRecord),
        <<"postalcode">> => generate_pc1(PatientRecord),
        <<"healthreport">> => generate_rep1(PatientRecord)
    }.

generate_pfinder1(PatientRecord) ->
    GNString =
        lists:flatten(
            [
                maps:get(preferred_gname, PatientRecord)|
                lists:map(
                    fun(OGN) ->
                        lists:flatten([".", OGN])
                    end,
                    maps:get(other_gnames, PatientRecord)
                )
            ]
        ),
    [
        lists:flatten(
            [
                maps:get(date_of_birth, PatientRecord),
                "|",
                maps:get(current_fname, PatientRecord),
                "|",
                GNString,
                "|",
                maps:get(current_postcode, PatientRecord)
            ]
        )
    ].

generate_pc1(PatientRecord) ->
    HistoricPostCodes = maps:get(historic_postcodes, PatientRecord),
    DOB = maps:get(date_of_birth, PatientRecord),
    lists:map(
        fun({PC, {SED, EED}}) ->
            lists:flatten([PC, "|", DOB, "|", SED, EED])
        end,
        HistoricPostCodes
    ).

generate_rep1(PatientRecord) ->
    SHA = maps:get(sha, PatientRecord),
    GPP = maps:get(gp_provider, PatientRecord),
    DOB = maps:get(date_of_birth, PatientRecord),
    HSF = maps:get(status_flags, PatientRecord),
    [
        lists:flatten([SHA, GPP, DOB, HSF])
    ].

%% @doc
%% Some flags are biased by age and some by location
generate_status_flags(YOB, CA) ->
    Flag1 =
        case {rand:uniform(), (?CURRENT_YEAR - YOB) / 400 } of
            {R1, P1} when R1 < P1 ->
                "Y";
            _ ->
                "N"
        end,
    Flag2 =
        case {rand:uniform(), (?CURRENT_YEAR - YOB) / 1000 } of
            {R2, P2} when R2 < P2 ->
                "Y";
            _ ->
                "N"
        end,
    Flag3 =
        case {rand:uniform(100), 20 + erlang:phash2(CA, 10)} of
            {R3, P3} when R3 < P3 ->
                "Y";
            _ ->
                "N"
        end,
    Flag4 =
        case rand:uniform() < 0.001 of
            true ->
                "YN";
            false ->
                "NY"
        end,
    lists:flatten([Flag1, Flag2, Flag3, Flag4]).

postcode_test(Node, Bucket) ->
    HTTPC = rt:httpc(Node),
    PcIdx = <<"postalcode_bin">>,
    {_HotPostalArea, HotPostCode} =
        generate_postcode(?HOT_POSTCODE_MAP, 1.0),
    {_ColdPostalArea, ColdPostCode} =
        generate_postcode(?HOT_POSTCODE_MAP, 0.0),
    HPABin = list_to_binary(lists:sublist(HotPostCode, 3)),
    HPCBin = list_to_binary(HotPostCode),
    CPABin = list_to_binary(lists:sublist(ColdPostCode, 3)),
    CPCBin = list_to_binary(ColdPostCode),
    ?LOG_INFO(
        "Testing with hot code ~s, and cold code ~s",
        [HotPostCode, ColdPostCode]
    ),
    {
        {T0, {ok, {count, R0HA_Cnt}}}, 
        {T1, {ok, {count, R0HC_Cnt}}},
        {T2, {ok, {count, R0CA_Cnt}}},
        {T3, {ok, {count, R0CC_Cnt}}}
    } =
        get_hot_and_cold_raw(
            HTTPC,
            Bucket,
            PcIdx,
            HPABin,
            HPCBin,
            CPABin,
            CPCBin,
            count
        ),
    ?LOG_INFO(
        "Count results for: "
        "Hot Area ~0p Hot Code ~0p Cold Area ~0p Cold Code ~0p",
        [{T0, R0HA_Cnt}, {T1, R0HC_Cnt}, {T2, R0CA_Cnt}, {T3, R0CC_Cnt}]
    ),
    {
        {T4, {ok, {raw_keys, R0HA_RKL}}},
        {T5, {ok, {raw_keys, R0HC_RKL}}},
        {T6, {ok, {raw_keys, R0CA_RKL}}},
        {T7, {ok, {raw_keys, R0CC_RKL}}}
    } =
        get_hot_and_cold_raw(
            HTTPC,
            Bucket,
            PcIdx,
            HPABin,
            HPCBin,
            CPABin,
            CPCBin,
            raw_keys
        ),
    ?LOG_INFO(
        "Raw key results for Hot Area: "
        "~0p Hot Code ~0p Cold Area ~0p Cold Code ~0p",
        [
            {T4, length(R0HA_RKL)},
            {T5, length(R0HC_RKL)},
            {T6, length(R0CA_RKL)},
            {T7, length(R0CC_RKL)}
        ]
    ),
    {
        {T8, {ok, {keys, R0HA_KL}}},
        {T9, {ok, {keys, R0HC_KL}}},
        {T10, {ok, {keys, R0CA_KL}}},
        {T11, {ok, {keys, R0CC_KL}}}
    } =
        get_hot_and_cold_raw(
            HTTPC,
            Bucket,
            PcIdx,
            HPABin,
            HPCBin,
            CPABin,
            CPCBin,
            keys
        ),
    ?LOG_INFO(
        "Deduplicated and sorted key results for Hot Area: "
        "~0p Hot Code ~0p Cold Area ~0p Cold Code ~0p",
        [
            {T8, length(R0HA_KL)},
            {T9, length(R0HC_KL)},
            {T10, length(R0CA_KL)},
            {T11, length(R0CC_KL)}
        ]
    ),
    HADup = length(R0HA_RKL) - R0HA_Cnt,
    HCDup = length(R0HC_RKL) - R0HC_Cnt,
    CADup = length(R0CA_RKL) - R0CA_Cnt,
    CCDup = length(R0CC_RKL) - R0CC_Cnt,
    ?LOG_INFO(
        "Duplicates found: ~w in ~w;  ~w in ~w; ~w in ~w; ~w in ~w",
        [
            HADup, R0HA_Cnt, HCDup, R0HC_Cnt, CADup, R0CA_Cnt, CCDup, R0CC_Cnt
        ]
    ),
    {T12, T13, CHC0, CHCR0} =
        get_postcode_yobr(HTTPC, Bucket, PcIdx, HPCBin),
    ?LOG_INFO(
        "Difference between fetching keys for hot postcode: "
        "With eval ~w and pure range ~w "
        "keys in eval range ~w results ~w",
        [T12, T13, CHC0, CHCR0]
    ),
    {T14, T15, CCC0, CCCR0} =
        get_postcode_yobr(HTTPC, Bucket, PcIdx, CPCBin),
    ?LOG_INFO(
        "Difference between fetching keys for cold postcode: "
        "With eval ~w and pure range ~w "
        "keys in eval range ~w results ~w",
        [T14, T15, CCC0, CCCR0]
    ),
    {T16, T17, T18, CHC1, CHCR1} =
        get_postcode_effective(HTTPC, Bucket, PcIdx, HPCBin),
    ?LOG_INFO(
        "Difference between fetching keys for hot postcode: "
        "With eval ~w and pure range ~w non-raw ~w "
        "keys in eval range ~w results ~w",
        [T16, T17, T18, CHC1, CHCR1]
    ),
    {T19, T20, T21, CCC1, CCCR1} =
        get_postcode_effective(HTTPC, Bucket, PcIdx, CPCBin),
    ?LOG_INFO(
        "Difference between fetching keys for cold postcode: "
        "With eval ~w and pure range ~w non-raw ~w "
        "keys in eval range ~w results ~w",
        [T19, T20, T21, CCC1, CCCR1]
    ),

    [
        T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11,
        R0HA_Cnt, R0HC_Cnt, R0CA_Cnt, R0CC_Cnt, HADup, HCDup, CADup, CCDup,
        T12, T13, CHC0, CHCR0, T14, T15, CCC0, CCCR0,
        T16, T17, T18, CHC1, CHCR1, T19, T20, T21, CCC1, CCCR1
    ].
    

get_hot_and_cold_raw(
    HTTPC, Bucket, PcIdx, HPABin, HPCBin, CPABin, CPCBin, AccOpt)
->
    TildaBin = <<"~">>,
    Options = [{timeout, 600}],
    R0HA =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPABin, <<HPABin/binary, TildaBin/binary>>},
                    undefined,
                    AccOpt,
                    Options
                )
            end
        ),
    R0HC =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    undefined,
                    AccOpt,
                    Options
                )
            end
        ),
    R0CA =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {CPABin, <<CPABin/binary, TildaBin/binary>>},
                    undefined,
                    AccOpt,
                    Options
                )
            end
        ),
    R0CC =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {CPCBin, <<CPCBin/binary, TildaBin/binary>>},
                    undefined,
                    AccOpt,
                    Options
                )
            end
        ),
    {R0HA, R0HC, R0CA, R0CC}.

get_postcode_yobr(HTTPC, Bucket, PcIdx, HPCBin) ->
    TildaBin = <<"~">>,
    Options = [{timeout, 600}],
    {_T0, {ok, {count, C0}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    undefined,
                    count,
                    Options
                )
            end
        ),
    {T1, {ok, {keys, KL1}}} =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    <<"delim($term, \"|\", ($pc, $dob, $ed))">>,
                    <<"$dob BETWEEN \"1981\" AND \"1985\"">>,
                    keys,
                    undefined,
                    #{},
                    Options
                )
            end
        ),
    StartKey = <<HPCBin/binary, <<"|1981">>/binary>>,
    EndKey = <<HPCBin/binary, <<"|1985">>/binary>>,
    {T2, {ok, {keys, KL1}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {StartKey, EndKey},
                    undefined,
                    keys,
                    Options
                )
            end
        ),
    {T1, T2, C0, length(KL1)}.

get_postcode_effective(HTTPC, Bucket, PcIdx, HPCBin) ->
    TildaBin = <<"~">>,
    Options = [{timeout, 600}],
    {_T0, {ok, {raw_count, C0}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    undefined,
                    raw_count,
                    Options
                )
            end
        ),
    {T1, {ok, {keys, KL1}}} =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    <<"delim($term, \"|\", ($pc, $dob, $ed)) | index($ed, 0, 8, $sed) | index($ed, 8, 16, $eed)">>,
                    <<"\"20100901\" BETWEEN $sed AND $eed">>,
                    keys,
                    undefined,
                    #{},
                    Options
                )
            end
        ),
    {T2, {ok, {raw_terms, TKL2}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    undefined,
                    raw_terms,
                    Options
                )
            end
        ),
    {T3, {ok, {terms, _}}} =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    PcIdx,
                    {HPCBin, <<HPCBin/binary, TildaBin/binary>>},
                    undefined,
                    terms,
                    Options
                )
            end
        ),
    
    true = C0 == length(TKL2),
    {T1, T2, T3, C0, length(KL1)}.

count_test(Node, Bucket) ->
    HTTPC = rt:httpc(Node),
    ?LOG_INFO("Counting - raw, de-duplicated, and by term"),
    RepIdx = <<"healthreport_bin">>,
    Options = [{timeout, 600}],
    _R0 =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA">>, <<"SHA~">>},
                    undefined,
                    count,
                    Options
                )
            end
        ),
    R1 =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA">>, <<"SHA~">>},
                    undefined,
                    count,
                    Options
                )
            end
        ),
    R2 =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA">>, <<"SHA~">>},
                    undefined,
                    raw_count,
                    Options
                )
            end
        ),
    R3 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA">>, <<"SHA~">>},
                    <<"index($term, 0, 8, $sha)">>,
                    <<"attribute_exists($sha)">>,
                    term_with_count,
                    <<"sha">>,
                    #{},
                    Options
                )
            end
        ),
    R4 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA">>, <<"SHA~">>},
                    <<"index($term, 0, 8, $sha)">>,
                    <<"attribute_exists($sha)">>,
                    term_with_rawcount,
                    <<"sha">>,
                    #{},
                    Options
                )
            end
        ),
    R5 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01002">>},
                    <<"index($term, 0, 8, $sha)">>,
                    <<"attribute_exists($sha)">>,
                    term_with_count,
                    <<"sha">>,
                    #{},
                    Options
                )
            end
        ),
    R6 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01002">>},
                    <<"index($term, 0, 8, $sha)">>,
                    <<"attribute_exists($sha)">>,
                    term_with_rawcount,
                    <<"sha">>,
                    #{},
                    Options
                )
            end
        ),
    {TC7, {ok, {term_with_count, TCL7}}} =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01001">>},
                    <<"index($term, 8, 8, $gp)">>,
                    <<"attribute_exists($gp)">>,
                    term_with_count,
                    <<"gp">>,
                    #{},
                    Options
                )
            end
        ),
    R7 = {TC7, {ok, {term_with_count, length(TCL7)}}},
    {TC8, {ok, {term_with_rawcount, TCL8}}} =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01001">>},
                    <<"index($term, 8, 8, $gp)">>,
                    <<"attribute_exists($gp)">>,
                    term_with_rawcount,
                    <<"gp">>,
                    #{},
                    Options
                )
            end
        ),
    R8 = {TC8, {ok, {term_with_rawcount, length(TCL8)}}},
    R9 =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01010">>, <<"SHA01011">>},
                    undefined,
                    raw_count,
                    Options
                )
            end
        ),
    R10 =
        timer:tc(
            fun() ->
                rhc:range_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01010">>, <<"SHA01011">>},
                    undefined,
                    count,
                    Options
                )
            end
        ),
    AgeEvalExpr1 =
        <<"index($term, 16, 8, $dob) | index($term, 24, 1, $smoker) | ">>,
    AgeEvalExpr2 =
        <<"map($dob, <, ((:sy, :s), (:by, :b), (:xy, :x), (:my, :m)), :z, $generation)">>,
    AgeEvalExpr3=
        <<" | join(($generation, $smoker), \".\", $gensmoke)">>,
    GenSubs =
        #{
            <<"sy">> => <<"1946">>,
            <<"by">> => <<"1966">>,
            <<"xy">> => <<"1980">>, 
            <<"my">> => <<"1997">>,
            <<"s">> => <<"Silent">>,
            <<"b">> => <<"Boomer">>,
            <<"x">> => <<"GenX">>,
            <<"m">> => <<"Millenial">>,
            <<"z">> => <<"GenZ">>
        },
    AgeEvalExpr = <<AgeEvalExpr1/binary, AgeEvalExpr2/binary, AgeEvalExpr3/binary>>,
    R11 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01~">>},
                    AgeEvalExpr,
                    <<"attribute_exists($gensmoke)">>,
                    term_with_count,
                    <<"gensmoke">>,
                    GenSubs,
                    Options
                )
            end
        ),
    R12 =
        timer:tc(
            fun() ->
                rhc:filter_query(
                    HTTPC,
                    Bucket,
                    RepIdx,
                    {<<"SHA01000">>, <<"SHA01~">>},
                    AgeEvalExpr,
                    <<"attribute_exists($gensmoke)">>,
                    term_with_rawcount,
                    <<"gensmoke">>,
                    GenSubs,
                    Options
                )
            end
        ),
    ResultList = [R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12],
    lists:foreach(fun(R) -> ?LOG_INFO("~0p", [R]) end, ResultList),
    lists:map(fun({TS, _}) -> TS end, ResultList).