%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
%% Copyright (c) 2017-2023 Workday, Inc.
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
%% @private
-module(riak_test_escript).

-export([
    main/1,
    add_deps/1
]).

-include_lib("kernel/include/logger.hrl").

%% Logger handler
-define(LOGFILE_HANDLER_NAME,   rt_file_h).
-define(LOGFILE_HANDLER_MODULE, logger_std_h).

%% If defined, dumps a file next to the JUnit file with the suffix .config
%% containing the test results that were evaluated to create the JUnit file.
% -define(DEBUG_JUNIT_INPUT, true).

add_deps(Path) ->
    case file:list_dir(Path) of
        {ok, Deps} ->
            ?LOG_NOTICE("Adding path ~s", [Path]),
            lists:foreach(
                fun(Dep) ->
                    code:add_path(filename:join([Path, Dep, "ebin"]))
                end, Deps);
        {error, enoent} ->
            ?LOG_WARNING("!!! Skipping path ~s", [Path]);
        {error, Reason} ->
            erlang:error(Reason, [Path])
    end.

cli_options() ->
%% Option Name, Short Code, Long Code, Argument Spec, Help Message
    [
        {help,               $h, "help",     undefined,  "Print this usage page"},
        {config,             $c, "conf",     string,     "specifies the project configuration"},
        {tests,              $t, "tests",    string,     "specifies which tests to run"},
        {suites,             $s, "suites",   string,     "which suites to run"},
        {dir,                $d, "dir",      string,     "run all tests in the specified directory"},
        {skip,               $x, "skip",     string,     "list of tests to skip in a directory"},
        {verbose,            $v, "verbose",  undefined,  "verbose output"},
        {outdir,             $o, "outdir",   string,     "output directory"},
        {backend,            $b, "backend",  atom,       "backend to test [memory | bitcask | eleveldb | leveled]"},
        {junit,       undefined, "junit",    boolean,    "output junit xml to the outdir directory"},
        {upgrade_version,    $u, "upgrade",  atom,       "which version to upgrade from [ previous | legacy ]"},
        {keep,        undefined, "keep",     boolean,    "do not teardown cluster"},
        {batch,       undefined, "batch",    undefined,  "running a batch, always teardown, even on failure"},
        {report,             $r, "report",   string,     "you're reporting an official test run, provide platform info (e.g. ubuntu-1204-64)\nUse 'config' if you want to pull from ~/.riak_test.config"},
        {file,               $F, "file",     string,     "use the specified file instead of ~/.riak_test.config"},
        {apply_traces,undefined, "trace",    undefined,  "Apply traces to the target node, defined in the SUITEs"}
    ].

print_help() ->
    getopt:usage(cli_options(), escript:script_name()),
    erlang:halt(1).

run_help([]) -> true;
run_help(ParsedArgs) ->
    lists:member(help, ParsedArgs).

-spec main(Args :: list(string())) -> no_return().
main(Args) ->
    try
        {ok, CWD} = file:get_cwd(),
        EbinDir = filename:join(CWD, ebin),
        case filelib:is_dir(EbinDir) of
            true ->
                code:add_patha(EbinDir);
            _ ->
                meh
        end,
        {ParsedArgs, HarnessArgs} = case getopt:parse(cli_options(), Args) of
            {ok, {P, H}} -> {P, H};
            _ -> print_help()
        end,
        case run_help(ParsedArgs) of
            true -> print_help();
            _ -> ok
        end,

        %% Load application defaults
        application:load(riak_test),

        %% Now get initial configuration. We need at least the 'verbose'
        %% setting before setting up the console log.

        Config = proplists:get_value(config, ParsedArgs),
        ConfigFile = proplists:get_value(file, ParsedArgs),
        %% Loads from specified file or default ~/.riak_test.config
        rt_config:load(Config, ConfigFile),

        %% Verbosity
        %% only applies to captured output, log files are always verbose
        ConfVerbose = rt_config:get(verbose, undefined),
        Verbose = case proplists:is_defined(verbose, ParsedArgs) of
            true = T ->
                T;
            _ ->
                erlang:is_boolean(ConfVerbose) andalso ConfVerbose
        end,
        ConfVerbose =:= Verbose orelse rt_config:set(verbose, Verbose),

        %% We have enough now to update the default (console) log handler.
        ok = logger:set_handler_config(default, #{
            config => #{type => standard_io},
            filters => rt_config:logger_filters(all),
            formatter => rt_config:logger_formatter(Verbose, true, true)
        }),

        %% That should keep the startup noise down, open up the lower levels.
        LogLevel = rt_config:get(log_level, info),
        logger:set_primary_config(level, LogLevel),

        erlang:register(riak_test, erlang:self()),

        %% Sets up extra paths earlier so that tests can be loadable
        %% without needing the -d flag.
        code:add_paths(rt_config:get(test_paths, [])),

        %% Ensure existence of scratch_dir
        case file:make_dir(rt_config:get(rt_scratch_dir)) of
            ok ->
                great;
            {error, eexist} ->
                great;
            ScratchError ->
                ?LOG_ERROR("Could not create scratch dir, ~0p", [ScratchError])
        end,

        %% Output directory
        ConfOutDir = rt_config:get(outdir, undefined),
        OutDir = case proplists:get_value(outdir, ParsedArgs, ConfOutDir) of
            undefined ->
                CWD;
            OD ->
                OD
        end,
        ConfOutDir == OutDir orelse rt_config:set(outdir, OutDir),

        LogFile = filename:join([OutDir, log, "riak_test.log"]),
        case filelib:ensure_dir(LogFile) of
            ok ->
                ok;
            {error, DirErr} ->
                erlang:error(DirErr, [LogFile])
        end,
        LogFileHConfig = #{
            config => #{type => file, file => LogFile, file_check => 100},
            filters => rt_config:logger_filters(default),
            formatter => rt_config:logger_formatter(true, true, false)
        },
        ok = logger:add_handler(
            ?LOGFILE_HANDLER_NAME, ?LOGFILE_HANDLER_MODULE, LogFileHConfig),

        %% Become a networked node
        rt:ensure_network_node(),

        %% ibrowse
        application:load(ibrowse),
        application:start(ibrowse),

        %% Report
        Report = case proplists:get_value(report, ParsedArgs, undefined) of
            undefined -> undefined;
            "config" -> rt_config:get(platform, undefined);
            R -> R
        end,

        Suites = proplists:get_all_values(suites, ParsedArgs),
        Suites =:= [] orelse ?LOG_WARNING(
            "Suites are not currently supported, ignoring ~0p", [Suites]),

        CommandLineTests = parse_command_line_tests(ParsedArgs),
        Tests0 = which_tests_to_run(Report, CommandLineTests),
        %% ToDo: Is there really any reason to support offset/workers config?
        Tests = case {rt_config:get(offset, undefined), rt_config:get(workers, undefined)} of
            {undefined, _} ->
                Tests0;
            {_, undefined} ->
                Tests0;
            {Offset, Workers} ->
                TestCount = length(Tests0),
                %% Avoid dividing by zero, computers hate that
                Denominator = case Workers rem (TestCount + 1) of
                    0 -> 1;
                    D -> D
                end,
                ActualOffset = ((TestCount div Denominator) * Offset) rem (TestCount + 1),
                {TestA, TestB} = lists:split(ActualOffset, Tests0),
                ?LOG_INFO("Offsetting ~b tests by ~b (~b workers, ~b offset)",
                    [TestCount, ActualOffset, Workers, Offset]),
                TestB ++ TestA
        end,

        ?LOG_NOTICE("Tests to run: ~0p", [Tests]),
        %% Two hard-coded deps...
        add_deps(rt:get_deps()),
        add_deps("_build/test/lib/riak_test/tests"),

        [add_deps(Dep) || Dep <- rt_config:get(rt_deps, [])],
        CoverDir = rt_config:get(cover_output, "coverage"),

        StartUTC = calendar:universal_time(),

        NumTests = erlang:length(Tests),
        TestResults = lists:filter(fun results_filter/1,
            [run_test(Test, OutDir, TestMetaData, Report, HarnessArgs, NumTests)
                || {Test, TestMetaData} <- Tests]),
        [rt_cover:maybe_import_coverage(proplists:get_value(coverdata, R)) || R <- TestResults],
        Coverage = rt_cover:maybe_write_coverage(all, CoverDir),
        case proplists:get_value(junit, ParsedArgs, false) of
            true ->
                JUnitFile = filename:join(junit_outdir(OutDir), junit_filename(StartUTC)),
                write_junit_output(JUnitFile, StartUTC, TestResults);
            _ ->
                ok
        end,
        Teardown = not proplists:get_value(keep, ParsedArgs, false),
        Batch = lists:member(batch, ParsedArgs),
        maybe_teardown(Teardown, TestResults, Coverage, Verbose, Batch),
        ?LOGFILE_HANDLER_MODULE:filesync(?LOGFILE_HANDLER_NAME),
        ExitCode = exit_code(TestResults),
        if Batch -> ExitCode; true -> erlang:halt(ExitCode) end

    catch
        Class:Reason:StackTrace ->
            ?LOG_ERROR("~0p: ~0p~n~p", [Class, Reason, StackTrace]),
            %% Give it time to write ...
            timer:sleep(333),
            %% We don't know if the handler was installed yet, but try to
            %% sync it in case it was.
            catch ?LOGFILE_HANDLER_MODULE:filesync(?LOGFILE_HANDLER_NAME),
            erlang:halt(1)
    end.

-spec maybe_teardown(
    Teardown :: boolean(),
    TestResults :: list(),
    Coverage :: term(),
    Verbose :: boolean(),
    Batch :: boolean() ) -> ok.
maybe_teardown(_Teardown, [] = TestResults, Coverage, Verbose, _Batch) ->
    print_summary(TestResults, Coverage, Verbose);
maybe_teardown(false, TestResults, Coverage, Verbose, _Batch) ->
    print_summary(TestResults, Coverage, Verbose),
    ?LOG_INFO("Keeping cluster running as requested");
maybe_teardown(true, [_] = TestResults, Coverage, Verbose, true) ->
    ?LOG_INFO("Batch mode specified"),
    rt:teardown(),
    print_summary(TestResults, Coverage, Verbose);
maybe_teardown(true, [TestResult] = TestResults, Coverage, Verbose, false) ->
    case proplists:get_value(status, TestResult) of
        fail ->
            print_summary(TestResults, Coverage, Verbose),
            so_kill_riak_maybe();
        _ ->
            ?LOG_INFO("Success"),
            rt:teardown(),
            print_summary(TestResults, Coverage, Verbose)
    end;
maybe_teardown(true, TestResults, Coverage, Verbose, _Batch) ->
    ?LOG_INFO("Multiple tests run"),
    rt:teardown(),
    print_summary(TestResults, Coverage, Verbose).

exit_code(TestResults) ->
    FailedTests = lists:filter(fun is_failed_test/1, TestResults),
    case FailedTests of
        [] ->
            0;
        _ ->
            1
    end.

is_failed_test(Result) ->
    proplists:get_value(status, Result) =:= fail.

junit_outdir(undefined) -> "log";
junit_outdir(Dir) -> Dir.

-ifdef(DEBUG_JUNIT_INPUT).

write_junit_output(JUnitFile, StartUTC, TestResults) ->
    %% So editors will see it as Erlang terms
    DumpFile = JUnitFile ++ ".config",
    file:write_file(DumpFile, io_lib:format("~p.~n", [TestResults])),
    file:write_file(JUnitFile, junit_xml(StartUTC, TestResults)).

-else.

write_junit_output(JUnitFile, StartUTC, TestResults) ->
    file:write_file(JUnitFile, junit_xml(StartUTC, TestResults)).

-endif.

junit_xml(StartUTC, TestResults) ->
    {ElapsedMS, Failed} = lists:foldl(
        fun(TR, {ET, FC}) ->
            F = case proplists:get_value(status, TR) of
                pass -> 0;
                _ -> 1
            end,
            {(ET + test_et_ms(TR)), (FC + F)}
        end, {0, 0}, TestResults),
    %% Sadly, there doesn't appear to be any formal schema for junit output
    %% files, so it's taken some experimentation to come up with something
    %% that seems to be ok for generic processing ...
    SuiteName = junit_suite(["RIAK_TEST_SUITE", "RIAK_TEST_CATEGORY"]),
    Timestamp = junit_timestamp(StartUTC),
    [
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n",
        io_lib:format(
            "<testsuite package=\"riak_test\" name=\"~s\" timestamp=\"~s\""
            " tests=\"~b\" failures=\"~b\" time=\"~.3.0f\">~n",
            [SuiteName, Timestamp, erlang:length(TestResults), Failed, (ElapsedMS / 1000)]),
        [junit_xml_testcase(R) || R <- TestResults],
        "</testsuite>\n"
    ].

%% TestResult properties [
%%  {backend, atom()}
%%  {coverdata, atom()}
%%  {elapsed_ms, pos_integer()}
%%  {id, integer()}
%%  {log, binary()}
%%  {platform, binary()} ex: <<"local">>
%%  {project, binary()} ex: <<"riak">>
%%  {reason, term()} only if status == fail
%%  {result, pass | fail | test_does_not_exist | all_prereqs_not_present}
%%  {status, pass | fail}
%%  {test, atom()} test name
%%  {version, binary()} ex: <<"riak-X.Y.Z">>
%% ]
junit_xml_testcase(TestResult) ->
    ETMS = test_et_ms(TestResult),
    Test = proplists:get_value(test, TestResult),
    Head = io_lib:format(
        "  <testcase name=\"~s\" time=\"~.3.0f\"", [Test, (ETMS / 1000)]),
    Result = proplists:get_value(result, TestResult),
    case Result of
        pass ->
            [Head, " />\n"];
        fail ->
            [
                Head, ">\n"
                "    <failure message=\"fail\" type=\"ERROR\">\n",
                junit_clean_error(Test, proplists:get_value(log, TestResult)),
                "    </failure>\n"
                "  </testcase>\n"
            ];
        _ ->
            [
                Head, ">\n", io_lib:format(
                "    <failure message=\"~s\" type=\"ERROR\" />\n", [Result]),
                "  </testcase>\n"
            ]
    end.

junit_clean_error(Test, Log) ->
    RE = io_lib:format(
        "\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\h+\\[notice\\]\\h+Running\\h+Test\\h+~s\\b",
        [Test]),
    Error = case re:run(Log, RE, [{capture, first, index}]) of
        {match, [{Start, _}]} ->
            binary:bin_to_list(Log, Start, (erlang:byte_size(Log) - Start));
        _ ->
            erlang:binary_to_list(Log)
    end,
    junit_encode_error(Error, []).

% xml-escape and strip CRs in one pass
junit_encode_error([], Out) ->
    lists:reverse(Out);
junit_encode_error([$\r | In], Out) ->
    junit_encode_error(In, Out);
junit_encode_error([$< | In], Out) ->
    junit_encode_error(In, [$;, $t, $l, $& | Out]);
junit_encode_error([$> | In], Out) ->
    junit_encode_error(In, [$;, $t, $g, $& | Out]);
junit_encode_error([$& | In], Out) ->
    junit_encode_error(In, [$;, $p, $m, $a, $& | Out]);
junit_encode_error([Ch | In], Out) ->
    junit_encode_error(In, [Ch | Out]).

%% always return at least 1ms
test_et_ms(Result) ->
    case proplists:get_value(elapsed_ms, Result) of
        I when erlang:is_integer(I) andalso I > 0 -> I;
        _ -> 1
    end.

junit_timestamp({{Y, Mo, D}, {H, Mn, S}}) ->
    io_lib:format(
        "~4..0b-~2..0b-~2..0bT~2..0b:~2..0b:~2..0bZ",
        [Y, Mo, D, H, Mn, S]).

junit_filename({{Y, Mo, D}, {H, Mn, S}}) ->
    io_lib:format(
        "junit_~4..0b-~2..0b-~2..0bT~2..0b-~2..0b-~2..0bZ.xml",
        [Y, Mo, D, H, Mn, S]).

junit_suite([]) ->
    "Riak Test";
junit_suite([Var | Vars]) ->
    case os:getenv(Var) of
        % ugly special-case hack for our driver
        "undefined" ->
            junit_suite(Vars);
        [_ | _] = Val ->
            Val;
        _ ->
            junit_suite(Vars)
    end.

parse_command_line_tests(ParsedArgs) ->
    Backends = case proplists:get_all_values(backend, ParsedArgs) of
        [] -> [undefined];
        Other -> Other
    end,
    Upgrades = case proplists:get_all_values(upgrade_version, ParsedArgs) of
        [] -> [undefined];
        UpgradeList -> UpgradeList
    end,

    rt_redbug:set_tracing_applied(proplists:is_defined(apply_traces, ParsedArgs)),

    %% Parse Command Line Tests
    {CodePaths, SpecificTests} =
        lists:foldl(fun extract_test_names/2,
            {[], []},
            proplists:get_all_values(tests, ParsedArgs)),
    [code:add_patha(CodePath) || CodePath <- CodePaths,
        CodePath /= "."],
    Dirs = proplists:get_all_values(dir, ParsedArgs),
    SkipTests = string:tokens(proplists:get_value(skip, ParsedArgs, []), [$,]),
    DirTests = lists:append([load_tests_in_dir(Dir, SkipTests) || Dir <- Dirs]),
    lists:foldl(fun(Test, Tests) ->
        [{
            list_to_atom(Test),
                [
                    {id, -1},
                    {platform, <<"local">>},
                    {version, rt:get_version()},
                    {project, list_to_binary(rt_config:get(rt_project, "undefined"))}
                ] ++
                [{backend, Backend} || Backend =/= undefined] ++
                [{upgrade_version, Upgrade} || Upgrade =/= undefined]}
            || Backend <- Backends,
            Upgrade <- Upgrades] ++ Tests
    end, [], lists:usort(DirTests ++ SpecificTests)).

extract_test_names(Test, {CodePaths, TestNames}) ->
    {[filename:dirname(Test) | CodePaths],
        [filename:rootname(filename:basename(Test)) | TestNames]}.

which_tests_to_run(_, CommandLineTests) ->
    CommandLineTests.

run_test(Test, OutDir, TestMetaData, Report, HarnessArgs, NumTests) ->
    rt_cover:maybe_start(Test),
    SingleTestResult = riak_test_runner:confirm(
        Test, OutDir, TestMetaData, HarnessArgs),
    CoverDir = rt_config:get(cover_output, "coverage"),
    case NumTests of
        1 -> keep_them_up;
        _ -> rt:teardown()
    end,
    CoverageFile = rt_cover:maybe_export_coverage(
        Test, CoverDir, erlang:phash2(TestMetaData)),
    case Report of
        undefined ->
            ok;
        _ ->
            {value, {log, _L}, _TestResult} =
                lists:keytake(log, 1, SingleTestResult)
    end,
    rt_cover:stop(),
    [{coverdata, CoverageFile} | SingleTestResult].

print_summary(TestResults, CoverResult, Verbose) ->
    %% Calculate everything before we start logging to reduce the chance
    %% of interleaved messages.
    %% Call logger:log/2,3 directly so we _don't_ get the location metadata.
    LogLev = notice,

    Results = [ [
        lists:concat([
            proplists:get_value(test, SingleTestResult), "-",
            backend_list(proplists:get_value(backend, SingleTestResult))
        ]),
        proplists:get_value(status, SingleTestResult),
        proplists:get_value(reason, SingleTestResult)
    ] || SingleTestResult <- TestResults],

    TestWidth = test_name_width(Results),
    CoverWidth =  case CoverResult of
        cover_disabled ->
            0;
        {_Coverage, CovApps} ->
            cover_app_width(CovApps)
    end,
    MaxWidth = erlang:max(TestWidth, CoverWidth),
    Border = lists:duplicate((MaxWidth + 10), $=),

    CountFun = fun
        ([_Test, pass, _Reason], {P, F}) ->
            {(P + 1), F};
        ([_Test, _Status, _Reason], {P, F}) ->
            {P, (F + 1)}
    end,
    PrintFun = case Verbose of
        true ->
            fun
                ([Test, fail = Status, Reason]) ->
                    logger:log(LogLev,
                        "~-*s: ~s~n\t~p", [MaxWidth, Test, Status, Reason]);
                ([Test, Status, _Reason]) ->
                    logger:log(LogLev, "~-*s: ~s", [MaxWidth, Test, Status])
            end;
        _ ->
            fun([Test, Status, _Reason]) ->
                logger:log(LogLev, "~-*s: ~s", [MaxWidth, Test, Status])
            end
    end,
    CoverFun = case CoverResult of
        cover_disabled ->
            fun() -> ok end;
        {Coverage, AppCov} ->
            fun() ->
                logger:log(notice, "Coverage: ~.1f%", [Coverage]),
                lists:foreach(
                    fun({App, Cov, _}) ->
                        logger:log(LogLev, "~-*s: ~.1f%", [MaxWidth, App, Cov])
                    end, AppCov)
            end
    end,

    {PassCount, FailCount} = lists:foldl(CountFun, {0, 0}, Results),
    TestCount = (PassCount + FailCount),
    Percentage = if
        TestCount == 0 ->
            0;
        FailCount == 0 ->
            100;
        true ->
            ((PassCount * 100) div TestCount)
    end,

    logger:log(LogLev, Border),
    logger:log(LogLev, "Test Results:"),
    lists:foreach(PrintFun, Results),
    logger:log(LogLev, "Tests: ~w,  Passed: ~w,  Failed: ~w",
        [TestCount, PassCount, FailCount]),
    logger:log(LogLev, "That's ~w% for those keeping score", [Percentage]),
    CoverFun(),
    logger:log(LogLev, Border).

test_name_width([] = _Results) ->
    0;
test_name_width(Results) ->
    lists:max([erlang:length(Name) || [Name | _T] <- Results]).

cover_app_width([] = _CovApps) ->
    0;
cover_app_width(CovApps) ->
    %% We don't know whether App is an atom or a string, just let
    %% lists:concat/1 sort it out and carry on in blissful ignorance.
    lists:max([erlang:length(lists:concat([App])) || {App, _, _} <- CovApps]).

backend_list(Backend) when erlang:is_atom(Backend) ->
    erlang:atom_to_list(Backend);
backend_list([A | _] = Backends) when erlang:is_atom(A) ->
    lists:concat(lists:join(",", Backends));
backend_list(Backends) when erlang:is_list(Backends) ->
    Backends.

results_filter(Result) ->
    case proplists:get_value(status, Result) of
        not_a_runnable_test ->
            false;
        _ ->
            true
    end.

load_tests_in_dir(Dir, SkipTests) ->
    case filelib:is_dir(Dir) of
        true ->
            code:add_path(Dir),
            lists:sort(
                lists:foldl(load_tests_folder(SkipTests),
                    [],
                    filelib:wildcard("*.beam", Dir)));
        _ ->
            ?LOG_ERROR("~s is not a directory!", [Dir])
    end.

load_tests_folder(SkipTests) ->
    fun(X, Acc) ->
        Test = string:substr(X, 1, length(X) - 5),
        case lists:member(Test, SkipTests) of
            true ->
                Acc;
            false ->
                [Test | Acc]
        end
    end.

so_kill_riak_maybe() ->
    io:format("~n~nSo, we find ourselves in a tricky situation here. ~n"),
    io:format("You've run a single test, and it has failed.~n"),
    io:format("Would you like to leave Riak running in order to debug?~n"),
    Input = io:get_chars("[Y/n] ", 1),
    case Input of
        "n" -> rt:teardown();
        "N" -> rt:teardown();
        _ ->
            io:format("Leaving Riak Up... "),
            rt:whats_up()
    end.
