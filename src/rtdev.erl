%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%% Copyright (c) 2022-2023 Workday, Inc.
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
%% @private Harness operating on devrels in the local filesystem.
-module(rtdev).
-behavior(rt_harness).

%% Harness API
-export([
    admin/3,
    admin_stats/1,
    clean_data_dir/2,
    cmd/3,
    copy_conf/3,
    default_node_name/0,
    deploy_clusters/1,
    deploy_nodes/1,
    get_backends/0,
    get_deps/0,
    get_ip/1,
    get_node_debug_logs/0,
    get_node_id/1,
    get_node_path/1,
    get_node_version/1,
    get_version/1,
    get_vsn_rec/1,
    interact/3,
    process_node_logs/3,
    restore_data_dir/3,
    riak/2,
    riak_repl/2,
    search_logs/2,
    set_advanced_conf/2,
    set_conf/2,
    setup_harness/2,
    spawn_cmd/2,
    spawned_result/1,
    start/1,
    start_naked/1,
    stop/1,
    teardown/0,
    update_app_config/2,
    upgrade/3, upgrade/4,
    versions/0,
    whats_up/0
]).

-compile([
    % export_all,
    nowarn_export_all
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N),    lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N),     erlang:list_to_atom(?DEVS(N))).

-define(DEBUG_LOG_FILE(N),
    lists:concat(["dev", N, "@127.0.0.1-riak-debug.tar.gz"])).

-define(DEFAULT_RIAK_SHUTDOWN_TIME, 10000).

%% Syntactic shortcuts
-define(is_pos_integer(N), (erlang:is_integer(N) andalso N > 0)).
-define(is_timeout(T), (?is_pos_integer(T) andalso T =< ((1 bsl 32) - 1))).

-type pid_string() :: nonempty_list($0..$9).

%% Descriptive names for path locations
-type dev_riak_dir()    :: rtt:fs_path().   %% dev/devN/riak
-type dev_riak_path()   :: rtt:fs_path().   %% vsn_root_path()/dev_riak_dir()
-type git_root_path()   :: rtt:fs_path().   %% git root
-type vsn_root_path()   :: rtt:fs_path().   %% dev_root_path()/vsn_label()
-type vsn_dev_path()    :: rtt:fs_path().   %% vsn_root_path()/dev

-record(stop_node, {
    riak :: dev_riak_path(),
    node :: node(),
    tto :: rtt:millisecs(),     %% overall timeout
    retry :: non_neg_integer(),
    delay :: rtt:millisecs()
}).

%% ===================================================================
%% Internal Maps
%% ===================================================================
%% These maps are interdependent and assume a constant `rtdev_path'
%% configuration - changing the configuration after the maps have been
%% populated will have no effect.
%% A `reset_cache()' operation would be trivial to implement should the
%% need arise.
%%
%% Many of these functions should end up inlined by the compiler.

-type tag_vsn_rec() :: {rtt:vsn_rec() | unknown, rtt:vsn_str() | unknown}.

-type nid_tag_map() :: #{rtt:node_id() => rtt:vsn_tag()}.
-type tag_path_map():: #{rtt:vsn_tag() => vsn_root_path()}.
-type tag_vsn_map() :: #{rtt:vsn_tag() => tag_vsn_rec()}.
-type node_nid_map():: #{node() => rtt:node_id()}.

%% @hidden Change from historical behavior!
%%
%% Prior implementations supported a configuration where `rtdev_path' is
%% set to a single filesystem path, not a proplist. This wasn't documented
%% in the README, and it's not clear whether any such configuration is in
%% use anywhere, so it's no longer supported.
%%
-spec get_tag_path_map() -> tag_path_map().
get_tag_path_map() ->
    case application:get_env(riak_test, rtdev_tag_path_map) of
        {ok, Val} ->
            Val;
        _ ->
            PathMap = case rt_config:get(rtdev_path) of
                Map when erlang:is_map(Map) ->
                    Map;
                [{_, _} | _] = PathList ->
                    maps:from_list(PathList);
                Invalid ->
                    erlang:error(badarg, [Invalid])
            end,
            set_tag_path_map(PathMap),
            PathMap
    end.

-spec set_tag_path_map(PathMap :: tag_path_map()) -> ok.
set_tag_path_map(PathMap) when erlang:is_map(PathMap) ->
    application:set_env(riak_test, rtdev_tag_path_map, PathMap).

-spec get_tag_vsn_map() -> tag_vsn_map().
get_tag_vsn_map() ->
    case application:get_env(riak_test, rtdev_tag_vsn_map) of
        {ok, Val} ->
            Val;
        _ ->
            PathMap = get_tag_path_map(),
            VsnMap = maps:fold(fun get_tag_vsn_map_fold/3, #{}, PathMap),
            set_tag_vsn_map(VsnMap),
            VsnMap
    end.

%% @hidden Used only by get_tag_vsn_map/0 initialization.
-spec get_tag_vsn_map_fold(
    Key :: rtt:vsn_tag(), Path :: vsn_root_path(), Vsns :: tag_vsn_map())
        -> tag_vsn_map().
get_tag_vsn_map_fold(root, _Path, Vsns) ->
    Vsns;
get_tag_vsn_map_fold(Key, Path, Vsns) ->
    Rec = case get_relpath_version(Path) of
        unknown ->
            {unknown, unknown};
        BinStr ->
            {rt_vsn:parse_version(BinStr), BinStr}
    end,
    Vsns#{Key => Rec}.

-spec set_tag_vsn_map(VsnMap :: tag_vsn_map()) -> ok.
set_tag_vsn_map(VsnMap) when erlang:is_map(VsnMap) ->
    application:set_env(riak_test, rtdev_tag_vsn_map, VsnMap).

-spec get_nid_tag_map() -> nid_tag_map().
get_nid_tag_map() ->
    application:get_env(riak_test, rtdev_nid_tag_map, #{}).

-spec set_nid_tag_map(VsnMap :: nid_tag_map()) -> ok.
set_nid_tag_map(VsnMap) when erlang:is_map(VsnMap) ->
    application:set_env(riak_test, rtdev_nid_tag_map, VsnMap).

-spec get_node_nid_map() -> node_nid_map().
get_node_nid_map() ->
    application:get_env(riak_test, rtdev_node_nid_map, #{}).

-spec set_node_nid_map(NodeMap :: node_nid_map()) -> ok.
set_node_nid_map(NodeMap) when erlang:is_map(NodeMap) ->
    application:set_env(riak_test, rtdev_node_nid_map, NodeMap).

%% ===================================================================
%% Further cleanup welcome ...
%% ===================================================================

%% @private Return the default node name to use if it's not configured.
%% The node MUST be reachable by all test nodes.
%% @see rt:ensure_network_node()
-spec default_node_name() -> node().
default_node_name() ->
    'riak_test@127.0.0.1'.

get_deps() ->
    filename:join([relpath(current), "dev", "dev1", "riak", "lib"]).

-spec git_cmd(Path :: git_root_path(), Args :: rtt:cmd_args())
        -> rtt:cmd_list().
git_cmd(Path, Args) ->
    ["git", lists:flatten(["--git-dir=", Path, "/.git"]),
        lists:flatten(["--work-tree=", Path]) | Args].

-spec riak_cmd(DevRiakPath :: dev_riak_path(), Args :: rtt:cmd_args())
        -> rtt:cmd_list().
riak_cmd(DevRiakPath, Args) ->
    [filename:join([DevRiakPath, "bin",
        rt_config:get(exec_name, "riak")]) | Args].

-spec riak_cmd(
    Path :: vsn_root_path(), DevN :: rtt:node_id(), Args :: rtt:cmd_args())
        -> rtt:cmd_list().
riak_cmd(Path, DevN, Args) ->
    riak_cmd(filename:join(Path, dev_riak_dir(DevN)), Args).

riak_admin_cmd(Path, DevN, Args) ->
    riak_cmd(Path, DevN, ["admin" | Args]).

riak_debug_cmd(Path, DevN, Args) ->
    riak_cmd(Path, DevN, ["debug" | Args]).

riak_repl_cmd(Path, DevN, Args) ->
    riak_cmd(Path, DevN, ["repl" | Args]).

run_git(Path, Args) ->
    [Exe | Params] = CmdList = git_cmd(Path, Args),
    ?LOG_INFO("Running: ~s", [rt_exec:cmd_line(CmdList)]),
    case rt_exec:cmd(Exe, cwd, Params, [], rlines) of
        {0, rlines, _Output} = Success ->
            Success;
        {error, Reason} ->
            erlang:error(Reason, CmdList);
        ErrResult ->
            erlang:error(ErrResult, CmdList)
    end.

%% This is the "compatibility" version that tries to accommodate various
%% past loose usage.
-spec run_riak(
    DevN :: rtt:node_id(),
    Path :: vsn_root_path(),
    Args :: atom() | nonempty_string() | list(nonempty_string()))
        -> Out :: string() | no_return().
run_riak(DevN, Path, [Arg | _] = Args) when erlang:is_list(Arg) ->
    run_riak(DevN, Path, Args, string, false);
run_riak(DevN, Path, Arg) when erlang:is_list(Arg) ->
    run_riak(DevN, Path, [Arg]);
run_riak(DevN, Path, Arg) when erlang:is_atom(Arg) ->
    run_riak(DevN, Path, [erlang:atom_to_list(Arg)]).

-spec run_riak(
    DevN :: rtt:node_id(),
    Path :: vsn_root_path(),
    Args :: rtt:cmd_args(),
    Form :: rtt:cmd_out_format(),
    Naked :: boolean() )
        -> Out :: list(string()) | string() | no_return().
run_riak(DevN, Path, [Cmd | _] = Args, Form, Naked)
        when erlang:is_list(Cmd) andalso erlang:is_boolean(Naked) ->
    [Exe | Params] = CmdList = riak_cmd(Path, DevN, Args),
    _ = Cmd /= "stop" orelse rt_cover:maybe_stop_on_node(?DEV(DevN)),
    CmdLine = rt_exec:cmd_line(CmdList),
    ?LOG_INFO("Running: ~s", [CmdLine]),
    case rt_exec:cmd(Exe, cwd, Params, [], Form) of
        {0, Form, Output} ->
            if
                Naked ->
                    ok;
                Cmd =:= "start" ->
                    Dev = ?DEV(DevN),
                    rt_cover:maybe_start_on_node(Dev, get_node_version(Dev)),
                    %% Intercepts may load code on top of the cover compiled
                    %% modules. We'll just get no coverage info then.
                    case rt_intercept:are_intercepts_loaded(Dev) of
                        false ->
                            ok = rt_intercept:load_intercepts([Dev]);
                        _ ->
                            ok
                    end;
                true ->
                    ok
            end,
            Output;
        {RC, Form, Output} ->
            ?LOG_WARNING("non-zero result ~b from ~s", [RC, CmdLine]),
            Output;
        {error, Reason} ->
            erlang:error(Reason, CmdList)
    end.

-spec start_riak(DevN :: pos_integer(), Path :: vsn_root_path())
        -> ok | no_return().
start_riak(DevN, Path) ->
    %% if it returns at all, the result was success
    _ = run_riak(DevN, Path, ["start"], rlines, false),
    ok.

-spec stop_riak(DevN :: pos_integer(), Path :: vsn_root_path())
        -> ok | no_return().
stop_riak(DevN, Path) ->
    %% if it returns at all, the result was success
    _ = run_riak(DevN, Path, ["stop"], rlines, false),
    ok.

-spec run_riak_repl(
    DevN :: pos_integer(),
    Path :: vsn_root_path(),
    Args :: atom() | nonempty_string() | list(nonempty_string()))
        -> Out :: string() | no_return().
run_riak_repl(DevN, Path, [Arg | _] = Args) when erlang:is_list(Arg) ->
    %% don't mess with intercepts and/or coverage,
    %% they should already be setup at this point
    [Exe | Params] = CmdList = riak_repl_cmd(Path, DevN, Args),
    ?LOG_INFO("Running: ~s", [rt_exec:cmd_line(CmdList)]),
    case rt_exec:cmd(Exe, Params) of
        {0, Output} ->
            Output;
        {error, Reason} ->
            erlang:error(Reason, CmdList);
        ErrResult ->
            erlang:error(ErrResult, CmdList)
    end;
run_riak_repl(DevN, Path, Arg) when erlang:is_list(Arg) ->
    run_riak_repl(DevN, Path, [Arg]);
run_riak_repl(DevN, Path, Arg) when erlang:is_atom(Arg) ->
    run_riak_repl(DevN, Path, [erlang:atom_to_list(Arg)]).

%% @private
-spec setup_harness(TestModule :: module(), HarnessArgs :: list(string()) )
        -> ok | rtt:std_error().
setup_harness(_TestModule, _HarnessArgs) ->
    %% fully stop all processes on any nodes
    _ = teardown(),

    %% Reset nodes to base state
    RootPath = relpath(root),
    ?LOG_INFO("Resetting nodes to fresh state"),
    _ = run_git(RootPath, ["reset", "HEAD", "--hard"]),
    _ = run_git(RootPath, ["clean", "-fdqx"]),

    ?LOG_INFO("Cleaning up lingering pipe directories"),
    PipeDirFilt = fun(DevPath, RmDirs) ->
        %% when joining two absolute paths, filename:join intentionally
        %% throws away the first one, so ensure DevPath is NOT absolute
        PipeDir = filename:join("/tmp", string:trim(DevPath, leading, "/")),
        [PipeDir | RmDirs]
    end,
    PipeDirs = lists:foldl(PipeDirFilt, [], devpaths()),
    ?assertMatch({0, _}, rt_exec:cmd("/bin/rm", ["-rf" | PipeDirs])).

%% @hidden Change from historical behavior!
%%
%% Prior implementations tried to transparently map version tags between atoms
%% and strings so that entries in `rtdev_path' could looked up in either form,
%% invoking a bunch of undocumented code to do so.
%% This wasn't documented in the README, and it's not believed that any current
%% configuration relies on the behavior, so it's no longer supported.
%%
-spec relpath(VsnTag :: rtt:vsn_tag()) -> vsn_root_path().
relpath(VsnTag) ->
    case get_tag_path_map() of
        #{VsnTag := Path} ->
            Path;
        _ ->
            erlang:error({bad_version, VsnTag})
    end.

upgrade(Node, NewVersion, UpgradeCallback) when is_function(UpgradeCallback) ->
    upgrade(Node, NewVersion, same, UpgradeCallback).

upgrade(Node, NewVersion, Config, UpgradeCallback) ->
    OldVersion = get_node_version(Node),
    upgrade(Node, OldVersion, NewVersion, Config, UpgradeCallback).

upgrade(Node, SameVersion, SameVersion, same, _UpgradeCallback) ->
    ?LOG_INFO(
        "Upgrade requested on node ~0p, version ~0p : nothing to do",
        [Node, SameVersion]),
    ok;
upgrade(Node, SameVersion, SameVersion, Config, _UpgradeCallback) ->
    ?LOG_INFO(
        "Upgrade requested on node ~0p, version ~0p : updating config only",
        [Node, SameVersion]),
    stop(Node),
    rt:wait_until_unpingable(Node),
    update_app_config(Node, Config),
    start(Node),
    rt:wait_until_pingable(Node),
    ok;
upgrade(Node, OldVersion, NewVersion, Config, UpgradeCallback) ->
    NodeNum = get_node_id(Node),
    OldVersion = get_node_version(NodeNum),
    ?LOG_INFO("Upgrading ~0p : ~0p -> ~0p", [Node, OldVersion, NewVersion]),
    stop(Node),
    rt:wait_until_unpingable(Node),

    OldNodePath = get_node_path(NodeNum, OldVersion),
    NewNodePath = get_node_path(NodeNum, NewVersion),
    OldConfPath = filename:join(OldNodePath, "etc"),
    NewConfPath = filename:join(NewNodePath, "etc"),
    OldDataPath = filename:join(OldNodePath, "data"),
    NewDataPath = filename:join(NewNodePath, "data"),

    Commands = [
        ["/bin/rm", "-rf", NewDataPath],
        ["/bin/mv", OldDataPath, NewDataPath],
        ["/bin/mkdir", OldDataPath],
        ["/bin/cp", "-pPR", OldConfPath, NewNodePath]
    ],
    lists:foreach(
        fun([Exe | Params] = CmdList) ->
            ?LOG_INFO("Running: ~s", [rt_exec:cmd_line(CmdList)]),
            {0, _} = rt_exec:cmd(Exe, Params)
        end, Commands),
    set_nid_tag_map(maps:put(NodeNum, NewVersion, get_nid_tag_map())),
    Config =:= same orelse update_app_config(Node, Config),

    Params = [
        {old_data_dir, OldDataPath},
        {new_data_dir, NewDataPath},
        {new_conf_dir, NewConfPath},
        {old_version,  OldVersion},
        {new_version,  NewVersion}
    ],
    ok = UpgradeCallback(Params),
    start(Node),
    rt:wait_until_pingable(Node),
    ok.

-spec copy_conf(
    NumNodes :: pos_integer(),
    FromVersion :: rtt:vsn_tag(),
    ToVersion :: rtt:vsn_tag() ) -> ok | rtt:std_error().
copy_conf(NumNodes, FromVersion, ToVersion) when ?is_pos_integer(NumNodes) ->
    ?LOG_INFO("Copying config from ~0p to ~0p", [FromVersion, ToVersion]),
    FromPath = relpath(FromVersion),
    ToPath = relpath(ToVersion),
    lists:foreach(
        fun(N) ->
            copy_node_conf(N, FromPath, ToPath)
        end, lists:seq(1, NumNodes)).

%% @hidden
-spec copy_node_conf(
    NodeNum :: rtt:node_id(),
    FromPath :: rtt:fs_path(),
    ToPath :: rtt:fs_path() ) -> ok.
copy_node_conf(NodeNum, FromPath, ToPath) ->
    DevPath = dev_riak_dir(NodeNum),
    SrcDir = filename:join([FromPath, DevPath, "etc"]),
    DstDir = filename:join(ToPath, DevPath),
    ?assertMatch({0, _}, rt_exec:cmd("/bin/cp", ["-pPR", SrcDir, DstDir])).

-spec set_conf(atom() | string(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    ?LOG_INFO("rtdev:set_conf(all, ~0p)", [NameValuePairs]),
    [set_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_riak_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    [append_to_conf_file(RiakConf, NameValuePairs)
        || RiakConf <- all_the_files(DevPath, "etc/riak.conf")],
    ok.

set_advanced_conf(all, NameValuePairs) ->
    ?LOG_INFO("rtdev:set_advanced_conf(all, ~0p)", [NameValuePairs]),
    [set_advanced_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    update_app_config_file(get_advanced_riak_conf(Node), NameValuePairs),
    ok;
set_advanced_conf(DevPath, NameValuePairs) ->
    AdvancedConfs = case all_the_files(DevPath, "etc/advanced.config") of
        [] ->
            %% no advanced conf? But we _need_ them, so make 'em
            make_advanced_confs(DevPath);
        Confs ->
            Confs
    end,
    ?LOG_INFO("AdvancedConfs = ~0p", [AdvancedConfs]),
    [update_app_config_file(RiakConf, NameValuePairs) || RiakConf <- AdvancedConfs],
    ok.

make_advanced_confs(DevPath) ->
    case filelib:is_dir(DevPath) of
        false ->
            ?LOG_ERROR("Failed generating advanced.conf ~0p is not a directory.", [DevPath]),
            [];
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/riak/etc", [DevPath]),
            ConfDirs = filelib:wildcard(Wildcard),
            [
                begin
                    AC = filename:join(Path, "advanced.config"),
                    ?LOG_DEBUG("writing advanced.conf to ~0p", [AC]),
                    file:write_file(AC, io_lib:fwrite("~p.\n", [[]])),
                    AC
                end || Path <- ConfDirs]
    end.

get_riak_conf(Node) ->
    N = get_node_id(Node),
    filename:join([
        relpath(get_node_version(N)), dev_riak_dir(N), "etc", "riak.conf"]).

get_advanced_riak_conf(Node) ->
    N = get_node_id(Node),
    filename:join([relpath(get_node_version(N)),
        dev_riak_dir(N), "etc", "advanced.config"]).

append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
        [io_lib:format("~n~s = ~s~n", [Name, Value])
            || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

all_the_files(DevPath, File) ->
    case filelib:is_dir(DevPath) of
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/riak/~s", [DevPath, File]),
            filelib:wildcard(Wildcard);
        _ ->
            ?LOG_DEBUG("~s is not a directory.", [DevPath]),
            []
    end.

all_the_app_configs(DevPath) ->
    AppConfigs = all_the_files(DevPath, "etc/app.config"),
    case length(AppConfigs) =:= 0 of
        true ->
            AdvConfigs = filelib:wildcard(DevPath ++ "/dev/dev*/riak/etc"),
            [filename:join(AC, "advanced.config") || AC <- AdvConfigs];
        _ ->
            AppConfigs
    end.

update_app_config(all, Config) ->
    ?LOG_INFO("rtdev:update_app_config(all, ~0p)", [Config]),
    [update_app_config(DevPath, Config) || DevPath <- devpaths()];
update_app_config(Node, Config) when is_atom(Node) ->
    N = get_node_id(Node),
    ConfDir = filename:join([
        relpath(get_node_version(N)), dev_riak_dir(N), "etc"]),
    AppConfigFile = filename:join(ConfDir, "app.config"),
    AdvConfigFile = filename:join(ConfDir, "advanced.config"),
    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's advanced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            update_app_config_file(AppConfigFile, Config);
        _ ->
            update_app_config_file(AdvConfigFile, Config)
    end;
update_app_config(DevPath, Config) ->
    [update_app_config_file(AppConfig, Config) || AppConfig <- all_the_app_configs(DevPath)].

update_app_config_file(ConfigFile, Config) ->
    ?LOG_INFO("rtdev:update_app_config_file(~s, ~0p)", [ConfigFile, Config]),

    BaseConfig = case file:consult(ConfigFile) of
        {ok, [ValidConfig]} ->
            ValidConfig;
        {error, enoent} ->
            []
    end,
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
            MergeC = orddict:from_list(VarsA),
            MergeD = orddict:from_list(VarsB),
            orddict:merge(fun(_, ValA, _ValB) ->
                ValA
            end, MergeC, MergeD)
        end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.
get_backends() ->
    lists:usort(
        lists:flatten([get_backends(DevPath) || DevPath <- devpaths()])).

get_backends(DevPath) ->
    rt:pmap(fun get_backend/1, all_the_app_configs(DevPath)).

get_backend(AppConfig) ->
    ?LOG_INFO("get_backend(~s)", [AppConfig]),
    Tokens = lists:reverse(filename:split(AppConfig)),
    ConfigFile = case Tokens of
        ["app.config" | _] ->
            AppConfig;
        ["advanced.config", "etc", "riak", "dev" ++ NNStr, "dev" | RPath ] ->
            VsnPath = filename:join(lists:reverse(RPath)),
            NodeNum = erlang:list_to_integer(NNStr),
            %% Why chkconfig? It generates an app.config from cuttlefish
            %% without starting riak.

            %% Use rt_exec:cmd directly so we can capture error output rather
            %% than throwing an exception from the invocation.
            [Exe | Args] = CmdList = riak_cmd(VsnPath, NodeNum, ["chkconfig"]),
            case rt_exec:cmd(Exe, cwd, Args, [], rlines) of
                {0, rlines, [LastLine | _] = RevOutLines} ->
                    %% LastLine looks like this:
                    %% -config /path/to/app.config \
                    %%  -args_file /path/to/vm.args -vm_args /path/to/vm.args
                    MatchedFiles = [FN || FN <- string:lexemes(LastLine, [$\s]),
                        filename:extension(FN) == ".config"],
                    case MatchedFiles of
                        [] ->
                            %% Successful execution, but no matching file
                            %% - this is not good.
                            ?LOG_ERROR(
                                "Unexpected output from: ~s",
                                [rt_exec:cmd_line(CmdList)]),
                            _ = [?LOG_ERROR(L) || L <- lists:reverse(RevOutLines)],
                            erlang:error(bad_config, [LastLine]);
                        [File | _] ->
                            case filename:pathtype(File) of
                                absolute ->
                                    File;
                                relative ->
                                    RelFile = case File of
                                        ["./" ++ RF] ->
                                            string:trim(RF, leading, "/");
                                        _ ->
                                            File
                                    end,
                                    filename:join([VsnPath,
                                        dev_riak_dir(NodeNum), RelFile])
                            end
                    end;
                {RC, rlines, RevOutLines} when erlang:is_integer(RC) ->
                    erlang:error({RC, lists:reverse(RevOutLines)}, CmdList);
                {error, {Reason, Exe}} ->
                    erlang:error(Reason, CmdList)
            end
    end,
    case file:consult(ConfigFile) of
        {ok, [Config]} ->
            rt:get_backend(Config);
        E ->
            ?LOG_ERROR("Error reading ~s, ~0p", [ConfigFile, E]),
            error
    end.

-spec get_node_path(Node :: rtt:node_id() | node()) -> Result :: rtt:fs_path().
get_node_path(NodeNum) when erlang:is_integer(NodeNum) ->
    get_node_path(NodeNum, get_node_version(NodeNum));
get_node_path(Node) ->
    get_node_path(get_node_id(Node)).

%% Keep the more flexible API and tell dialyzer it's ok that it's not used.
%% Remove this attribute if you add a call to get_node_path(Node :: node())!
-dialyzer({no_match, get_node_path/2}).
-spec get_node_path(
    Node :: rtt:node_id() | node(), Version :: rtt:vsn_tag() )
        -> Result :: dev_riak_path().
get_node_path(NodeNum, Version) when erlang:is_integer(NodeNum) ->
    filename:join(relpath(Version), dev_riak_dir(NodeNum));
get_node_path(Node, Version) ->
    get_node_path(get_node_id(Node), Version).

-spec dev_riak_dir(Node :: rtt:node_id() | node()) -> Result :: dev_riak_dir().
dev_riak_dir(NodeNum) when erlang:is_integer(NodeNum) ->
    filename:join(["dev", lists:concat(["dev", NodeNum]), "riak"]);
dev_riak_dir(Node) ->
    dev_riak_dir(get_node_id(Node)).

get_ip(_Node) ->
    %% localhost 4 lyfe
    "127.0.0.1".

create_snmp_dirs(Nodes) ->
    SubDirs = filename:join(["data", "snmp", "agent", "db"]),
    Dirs = [filename:join(get_node_path(Node), SubDirs) || Node <- Nodes],
    ?assertMatch({0, _}, rt_exec:cmd("/bin/mkdir", ["-p" | Dirs])).

-spec clean_data_dir(Nodes :: list(node()), SubDir :: string() )
        -> ok | rtt:std_error().
clean_data_dir([_|_] = Nodes, [_|_] = SubDir) ->
    Dirs = [filename:join(
        [get_node_path(Node), "data", SubDir]) || Node <- Nodes],
    delete_dirs(Dirs);
clean_data_dir([_|_] = Nodes, []) ->
    Dirs = [filename:join(get_node_path(Node), "data") || Node <- Nodes],
    delete_dirs(Dirs);
clean_data_dir([], _)
    -> ok.

-spec delete_dirs(Dirs :: list(rtt:fs_path())) -> ok | rtt:std_error().
delete_dirs([_|_] = Dirs) ->
    ?LOG_INFO("Removing directories ~0p", [Dirs]),
    ?assertMatch({0, _}, rt_exec:cmd("/bin/rm", ["-rf" | Dirs])),
    ?assertNot(lists:any(fun filelib:is_file/1, Dirs));
delete_dirs([]) ->
    ok.

restore_data_dir(Nodes, BackendFldr, BackupFldr) when is_list(Nodes) ->
    RestoreNodeFun =
        fun(Node) ->
            DataDir = filename:join(get_node_path(Node), "data"),
            Backend = filename:join(DataDir, BackendFldr),
            Backup = filename:join(DataDir, BackupFldr),
            ?LOG_INFO("Restoring Node ~s from ~s", [Backend, Backup]),
            ?assertMatch({0, _}, rt_exec:cmd("/bin/mkdir", ["-p", Backend])),
            ?assertCmd("/bin/cp -pPR " ++ Backup ++ "/* " ++ Backend)
        end,
    lists:foreach(RestoreNodeFun, Nodes).

add_default_node_config(Nodes) ->
    case rt_config:get(rt_default_config, undefined) of
        undefined -> ok;
        Defaults when is_list(Defaults) ->
            rt:pmap(fun(Node) ->
                update_app_config(Node, Defaults)
            end, Nodes),
            ok;
        BadValue ->
            ?LOG_ERROR("Invalid value for rt_default_config : ~0p", [BadValue]),
            throw({invalid_config, {rt_default_config, BadValue}})
    end.

-spec deploy_clusters(nonempty_list(rtt:node_configs())) -> rtt:clusters().
deploy_clusters(ClusterConfigs) ->
    NumNodes = rt_config:get(num_nodes, 6),
    RequestedNodes = lists:flatten(ClusterConfigs),

    case length(RequestedNodes) > NumNodes of
        true ->
            erlang:error("Requested more nodes than available");
        false ->
            Nodes = deploy_nodes(RequestedNodes),
            {DeployedClusters, _} = lists:foldl(
                fun(Cluster, {Clusters, RemNodes}) ->
                    {A, B} = lists:split(length(Cluster), RemNodes),
                    {Clusters ++ [A], B}
                end, {[], Nodes}, ClusterConfigs),
            DeployedClusters
    end.

-spec deploy_nodes(rtt:node_configs()) -> rtt:nodes().
deploy_nodes([_|_] = NodeConfigs) ->
    PathMap = get_tag_path_map(),
    ?LOG_INFO("Riak path: ~0p", [maps:get(root, PathMap)]),
    {Versions, Configs} = lists:unzip(NodeConfigs),
    %% Check that you have the right versions available
    lists:foreach(
        fun(Version) ->
            case maps:is_key(Version, PathMap) of
                true ->
                    ok;
                _ ->
                    ErrMsg = lists:flatten(io_lib:format(
                        "You don't have Riak '~s' configured", [Version])),
                    ?LOG_ERROR(ErrMsg),
                    erlang:error(ErrMsg)
            end
        end, lists:usort(Versions)),

    NumNodes = erlang:length(NodeConfigs),
    NodeNums = lists:seq(1, NumNodes),
    [Node1 | _] = Nodes = [?DEV(N) || N <- NodeNums],

    NodeVsns = lists:zip(NodeNums, Versions),
    VsnMap = maps:from_list(NodeVsns),
    set_nid_tag_map(VsnMap),

    NodeMap = maps:from_list(lists:zip(Nodes, NodeNums)),
    set_node_nid_map(NodeMap),

    %% Set initial config
    add_default_node_config(Nodes),
    %% Only update non-default configs
    UpdateFilt = fun({_N, C}) -> C =/= default end,
    _ = case lists:filter(UpdateFilt, lists:zip(Nodes, Configs)) of
        [] ->
            ok;
        Updates ->
            Update = fun
                ({Node, {cuttlefish, Config}}) ->
                    set_conf(Node, Config);
                ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            rt:pmap(Update, Updates)
    end,

    %% create snmp dirs, for EE
    %% Almost certainly don't need these, but they don't hurt anything
    create_snmp_dirs(Nodes),

    %% All of these either return 'ok' or raise an exception
    StartNode = fun({NodeNum, RelPath}) ->
        start_riak(NodeNum, RelPath)
    end,
    WaitForNode = fun(N) ->
        %% ensure node started
        rt:wait_until_pingable(N),

        %% ensure riak_core_ring_manager is running before we go on
        rt:wait_until_registered(N, riak_core_ring_manager),

        %% ensure node is a singleton cluster
        rt:check_singleton_node(N)
    end,
    %% Always start one node first to avoid a race condition in the 'cover'
    %% module on starting the server.
    [FirstNode | RestNodes] = [{N, maps:get(V, PathMap)} || {N, V} <- NodeVsns],
    StartNode(FirstNode),
    case RestNodes of
        [] ->
            WaitForNode(Node1);
        _ ->
            %% pmap returns {'EXIT', killed} if the function throws an exception
            lists:foreach(
                fun(R) -> ?assertMatch(ok, R) end, rt:pmap(StartNode, RestNodes)),
            lists:foreach(
                fun(R) -> ?assertMatch(ok, R) end, rt:pmap(WaitForNode, Nodes))
    end,

    ?LOG_INFO("Deployed nodes: ~0p", [Nodes]),
    Nodes.

%% @hidden Single-parameter function to stop a node if it's running.
%% Called ONLY by stop_all/1, and relies on kill_stragglers/2 being called
%% afterward.
%% If the node is running and the RPC call fails, it's most likely due to a
%% cookie mismatch - something that really shouldn't happen in a properly
%% configured test environment. Nevertheless, we'll try issuing a `riak stop'
%% command, which does the same thing we do via RPC, on the assumption that
%% it'll use the correct cookie.
%% Relx-based releases' `riak stop' wait for the node to stop completely
%% before returning, but pre-relx (before 3.x) versions may not have stopped
%% fully on return. Since this is being invoked as part of a finalization
%% teardown, we don't care and will let kill_stragglers/2 kill them off
%% leaving a potentially corrupted environment, which will be cleaned up by
%% `git reset/clean' before they're next started.
-spec stop_node_fun(#stop_node{}) -> ok.
stop_node_fun(#stop_node{
        riak = DevRiakDir, node = Node,
        tto = TTO, retry = Retry, delay = Delay }) ->
    case rpc:call(Node, os, getpid, []) of
        [_|_] = PidStr ->
            ?LOG_INFO(
                "Stopping node ~0p (OS PID ~s) with init:stop/0 ...",
                [Node, PidStr]),
            rpc:call(Node, init, stop, []),
            %% If init:stop/0 fails here, the wait_for_pid/2 call
            %% below will timeout and the process will get cleaned
            %% up by the kill_stragglers/2 function.
            case rt:wait_until(fun pid_is_dead/1, PidStr, Retry, Delay) of
                {fail, _} ->
                    fail;
                _ ->
                    ok
            end;
        BadRpc ->
            [Exe | Args] = CmdList = riak_cmd(DevRiakDir, ["stop"]),
            ?LOG_INFO(
                "RPC to node ~0p returned ~0p, will try stop anyway ... ~s",
                [Node, BadRpc, rt_exec:cmd_line(CmdList)]),
            Status = case rt_exec:cmd(Exe, cwd, Args, [], rlines, TTO) of
                {0, rlines, OK} ->
                    OK;
                {_RC, rlines, _Output} ->
                    "wasn't running";
                Error ->
                    ?LOG_ERROR("~0p", [Error]),
                    Error
            end,
            ?LOG_INFO("Stopped node ~0p, stop status: ~s.", [Node, Status])
    end.

%% @hidden Kill any Erlang processes under the specified DevPath.
%% Called ONLY by stop_all/1 after stop_node_fun/1 has been called per node.
-spec kill_stragglers(
    DevPath :: rtt:fs_path(), Timeout :: rtt:millisecs()) -> ok.
kill_stragglers(DevPath, Timeout) ->
    {0, rlines, [_|_] = PsLines} =
        rt_exec:cmd("/bin/ps", cwd, ["-ef"], [], rlines),
    {ok, RE} = re:compile(
        "^\\s*\\S+\\s+(\\d+).+\\d+\\s+" ++ DevPath ++ "\\S+/beam"),
    ReOpts = [{capture, all_but_first, list}],
    PidFun = fun(Line, Acc) ->
        case re:run(Line, RE, ReOpts) of
            nomatch ->
                Acc;
            {match, [Pid]} ->
                [Pid | Acc]
        end
    end,
    case lists:foldl(PidFun, [], PsLines) of
        [] ->
            ok;
        Pids ->
            %% send them all a SIGTERM to start ...
            ?LOG_INFO("Killing stragglers ~0p", [Pids]),
            ?assertMatch(
                {RC, _} when erlang:is_integer(RC),
                rt_exec:cmd("/bin/kill", Pids)),
            %% Now check each to see if they're running ...
            Delay = 1000,
            Retry = (Timeout div Delay),
            WaitForPid = fun(PidStr) ->
                rt:wait_until(fun pid_is_dead/1, PidStr, Retry, Delay)
            end,
            WaitResult = lists:zip(rt:pmap(WaitForPid, Pids), Pids),
            NotDeadYet = fun
                ({true, _Pid}, Results) ->
                    Results;
                ({_False, Pid}, Results) ->
                    [Pid | Results]
            end,
            case lists:foldl(NotDeadYet, [], WaitResult) of
                [] ->
                    ok;
                Zombies ->
                    ?LOG_INFO(
                        "Processes still haven't stopped,"
                        " resorting to kill -s KILL ~s",
                        [string:join(Zombies, " ")]),
                    ?assertMatch({RC, _} when erlang:is_integer(RC),
                        rt_exec:cmd("/bin/kill", ["-s", "KILL" | Zombies]))
            end
    end.

-spec pid_is_dead(PidStr :: pid_string()) -> boolean().
pid_is_dead(PidStr) ->
    %% Use the POSIX null signal to check the pid
    case rt_exec:cmd("/bin/kill", ["-s", "0", PidStr]) of
        {RC, _Out} when erlang:is_integer(RC) ->
            RC /= 0;
        %% Anything else is fatal
        {error, Reason} ->
            erlang:error(Reason)
    end.

%% @hidden Stop all possibly running nodes under DevPath, where DevPath is the
%% path to a `relpath(VsnTag)/dev' directory.
-spec stop_all(DevPath :: vsn_dev_path())
        -> ok | {error, {enotdir, [rtt:fs_path()]}}.
stop_all(DevPath) ->
    case filelib:is_dir(DevPath) of
        true ->
            Glob = filename:join([DevPath, "dev*", "riak"]),
            case filelib:wildcard(Glob) of
                [] ->
                    ?LOG_WARNING("no riak nodes in ~s", [DevPath]);
                Devs ->
                    Nodes = [
                        ?DEV(N) || N <- lists:seq(1, erlang:length(Devs))],
                    Timeout = (find_shutdown_timeout(Nodes) + 5000),
                    Delay = 1000,
                    Retry = (Timeout div Delay),
                    StopArgs = [#stop_node{
                        riak = D, node = N,
                        tto = Timeout, retry = Retry, delay = Delay
                    } || {D, N} <- lists:zip(Devs, Nodes)],
                    _ = rt:pmap(fun stop_node_fun/1, StopArgs, Timeout),
                    kill_stragglers(DevPath, Timeout)
            end;
        _ ->
            ?LOG_ERROR("~s is not a directory.", [DevPath]),
            {error, {enotdir, DevPath}}
    end.

-spec find_shutdown_timeout(Nodes :: rtt:nodes()) -> rtt:millisecs().
find_shutdown_timeout([_|_] = Nodes) ->
    ?LOG_INFO("Trying to obtain node shutdown_time via RPC ..."),
    {RpcRes, _BadNodes} =
        rpc:multicall(Nodes, init, get_argument, [shutdown_time], 5000),
    find_shutdown_timeout(RpcRes, undefined).

%% @hidden Handles the case where `-shutdown_time' was improperly specified
%% but we can still figure out what was intended.
-spec find_shutdown_timeout(
    RpcResults :: list(), Improper :: rtt:millisecs() | undefined)
        -> rtt:millisecs().
find_shutdown_timeout([], undefined) ->
    ?LOG_INFO(
        "Using default node shutdown_time of ~b ms",
        [?DEFAULT_RIAK_SHUTDOWN_TIME]),
    ?DEFAULT_RIAK_SHUTDOWN_TIME;
find_shutdown_timeout([], ImproperValue) ->
    ?LOG_WARNING(
        "Using improperly specified node shutdown_time of ~b ms",
        [ImproperValue]),
    ImproperValue;
find_shutdown_timeout([{ok, [TimeoutStr]} | Rest], ImproperValue) ->
    case string:to_integer(TimeoutStr) of
        {TimeoutVal, []} when TimeoutVal > 0 ->
            ?LOG_INFO("Using node shutdown_time of ~b ms", [TimeoutVal]),
            TimeoutVal;
        _ ->
            find_shutdown_timeout(Rest, ImproperValue)
    end;
%% This pattern is returned by some Riak v3.x instances using a buggy
%% relx-generated `riak' script.
%% The instance's `init' module will have (properly) ignored the bad
%% configuration option and will use an effective shutdown_time of infinity,
%% but we can still parse out the intended value to use if no properly-
%% configured nodes responded.
find_shutdown_timeout([{ok, [TimeoutStr | _]} | Rest], undefined) ->
    case string:to_integer(TimeoutStr) of
        {TimeoutVal, []} when TimeoutVal > 0 ->
            find_shutdown_timeout(Rest, TimeoutVal);
        _ ->
            find_shutdown_timeout(Rest, undefined)
    end;
find_shutdown_timeout([_ | Rest], ImproperValue) ->
    find_shutdown_timeout(Rest, ImproperValue).

stop(Node) ->
    RiakPid = rpc:call(Node, os, getpid, []),
    N = get_node_id(Node),
    rt_cover:maybe_stop_on_node(Node),
    stop_riak(N, relpath(get_node_version(N))),
    ?assertEqual(ok, rt:wait_until(fun pid_is_dead/1, RiakPid)).

start(Node) ->
    DevN = get_node_id(Node),
    start_riak(DevN, relpath(get_node_version(DevN))).

start_naked(Node) ->
    DevN = get_node_id(Node),
    Path = relpath(get_node_version(DevN)),
    _ = run_riak(DevN, Path, ["start"], rlines, true),
    ok.

-spec interact(
    Node :: node(),
    Cmd :: nonempty_string(),
    Ops :: rtt:interact_list() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().
interact(Node, [C|_] = Cmd, [_|_] = Ops) when erlang:is_integer(C) ->
    NodeRiakPath = get_node_path(Node),
    [Exe | Args] = riak_cmd(NodeRiakPath, [Cmd]),
    ?LOG_INFO("Interacting with riak ~s.", [Cmd]),
    rt_exec:interact(Exe, Args, Ops).

-spec admin(
    Node :: node(), Args :: rtt:cmd_args(),
    Options :: proplists:proplist() )
        -> {ok, string() | {rtt:cmd_rc(), string()}}.
admin(Node, Args, Options) ->
    N = get_node_id(Node),
    Path = relpath(get_node_version(N)),
    [Exe | Params] = CmdList = riak_admin_cmd(Path, N, Args),
    ?LOG_INFO("Running: ~s", [rt_exec:cmd_line(CmdList)]),
    Result = case rt_exec:cmd(Exe, Params) of
        {error, Reason} ->
            erlang:error(Reason, CmdList);
        {_RC, Output} = FullResult ->
            case lists:member(return_exit_code, Options) of
                true ->
                    FullResult;
                _ ->
                    Output
            end
    end,
    ?LOG_DEBUG("~0p", [Result]),
    {ok, Result}.

%% @private Call 'bin/riak admin status' command on `Node'.
%% @see rt:admin_stats/1
-spec admin_stats(Node :: node() )
        -> {ok, nonempty_list({binary(), binary()})} | rtt:std_error().
admin_stats(Node) ->
    N = get_node_id(Node),
    Path = relpath(get_node_version(N)),
    [Exe | Args] = CmdList = riak_admin_cmd(Path, N, ["status"]),
    CmdLine = rt_exec:cmd_line(CmdList),
    ?LOG_INFO("Running: ~s", [CmdLine]),
    %% Get the output in reverse order for efficiency - they'll be reversed
    %% into their original order (on success) by the filtering fold.
    case rt_exec:cmd(Exe, cwd, Args, [], rlines) of
        {0, rlines, RevLines} ->
            %% Format of the output lines we care about is
            %%      "StatKey : StatValue"
            %% which the RE splits into
            %%      [<<"StatKey">>, <<"StatValue">>]
            %% Lines without the " : " separator pattern are returned as
            %%      [<<"Line">>]
            %% which we ignore. This conveniently ignores the header lines,
            %% but StatValues that split across lines, because the remote
            %% function prints them with "~p", are effectively unparseable.
            {ok, RE} = re:compile(" : ", []),
            FoldFun = fun(Line, Out) ->
                case re:split(Line, RE, [{parts, 2}, {return, binary}]) of
                    [_BinKey, _BinVal] = Stat ->
                        [erlang:list_to_tuple(Stat) | Out];
                    _Discard ->
                        Out
                end
            end,
            {ok, lists:foldl(FoldFun, [], RevLines)};
        {error, _} = Error ->
            Error;
        Other ->
            {error, Other}
    end.

%% @private
-spec riak(Node :: node(), Args :: list(string()) ) -> {ok, string()}.
riak(Node, Args) ->
    N = get_node_id(Node),
    Path = relpath(get_node_version(N)),
    Result = run_riak(N, Path, Args),
    ?LOG_DEBUG("~s", [string:trim(Result)]),
    {ok, Result}.

%% @private
-spec riak_repl(Node :: node(), Args :: list(string()) ) -> {ok, string()}.
riak_repl(Node, Args) ->
    N = get_node_id(Node),
    Path = relpath(get_node_version(N)),
    Result = run_riak_repl(N, Path, Args),
    ?LOG_DEBUG("~s", [string:trim(Result)]),
    {ok, Result}.

-spec get_node_id(Node :: node()) -> Result :: rtt:node_id().
get_node_id(Node) ->
    maps:get(Node, get_node_nid_map()).

-spec get_node_version(Node :: rtt:node_id() | node())
        -> Result :: rtt:vsn_tag().
get_node_version(NodeNum) when erlang:is_integer(NodeNum) ->
    maps:get(NodeNum, get_nid_tag_map());
get_node_version(Node) ->
    get_node_version(get_node_id(Node)).

-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Form :: rtt:cmd_out_format() )
        -> Result :: rtt:cmd_result() | rtt:cmd_error().
cmd(Cmd, Args, Form) ->
    rt_exec:cmd(Cmd, cwd, Args, [], Form).

-spec spawn_cmd(Cmd :: rtt:cmd_exe(), Args :: rtt:cmd_args()) ->
    Result :: rtt:cmd_token() | rtt:cmd_exec_err().
spawn_cmd(Cmd, Args) ->
    rt_exec:spawn_cmd(Cmd, Args).

-spec spawned_result(CmdToken :: rtt:cmd_token()) ->
    Result :: {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_timeout_err().
spawned_result(CmdToken) ->
    rt_exec:spawned_result(CmdToken).

%%set_backend(Backend) ->
%%    set_backend(Backend, []).
%%
%%set_backend(Backend, OtherOpts) ->
%%    ?LOG_INFO("rtdev:set_backend(~0p, ~0p)", [Backend, OtherOpts]),
%%    Opts = [{storage_backend, Backend} | OtherOpts],
%%    update_app_config(all, [{riak_kv, Opts}]),
%%    get_backends().

%% @private Return the raw version string of the specified version tag.
-spec get_version(VsnTag :: rtt:vsn_tag())
        -> Result :: rtt:vsn_str() | unknown.
get_version(VsnTag) ->
    {_VsnRec, BinStr} = maps:get(VsnTag, get_tag_vsn_map()),
    BinStr.

%% @private Return the comparable version of the specified version tag.
-spec get_vsn_rec(VsnTag :: rtt:vsn_tag() ) -> rtt:vsn_rec()  | unknown.
get_vsn_rec(VsnTag) ->
    {VsnRec, _BinStr} = maps:get(VsnTag, get_tag_vsn_map()),
    VsnRec.

%% @hidden Read the VERSION file from an arbitrarily tagged version.
-spec get_relpath_version(Path :: vsn_root_path()) -> rtt:vsn_str() | unknown.
get_relpath_version(Path) ->
    case file:read_file(filename:join(Path, "VERSION")) of
        {ok, Version} ->
            Version;
        {error, enoent} ->
            unknown;
        {error, Reason} ->
            erlang:error(Reason, [Path])
    end.

%% @private Stop all discoverable nodes.
-spec teardown() -> ok.
teardown() ->
    %% make sure we stop any cover processes on any nodes
    %% otherwise, if the next test boots a legacy node we'll end up with cover
    %% incompatibilities and crash the cover server
    rt_cover:maybe_stop_on_nodes(),
    DevPaths = [filename:join(P, "dev") || P <- devpaths()],
    _ = rt:pmap(fun stop_all/1, DevPaths),
    ok.

%% @private Display running nodes.
whats_up() ->
    io:format("Here's what's running...~n"),
    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n", [string:substr(Dir, 1, length(Dir) - 1)]) || Dir <- Up].

%% @hidden Return all of the configured relpaths except `root'.
-spec devpaths() -> nonempty_list(rtt:fs_path()).
devpaths() ->
    lists:usort(maps:values(maps:remove(root, get_tag_path_map()))).

%% @private Returns a list of configured versions.
-spec versions() -> nonempty_list(rtt:vsn_tag()).
versions() ->
    lists:delete(root, maps:keys(get_tag_path_map())).

%% @private Processes log files from specified running nodes.
%% @see rt:process_node_logs/3
-spec process_node_logs(
    Nodes :: all | rtt:nodes(),
    FileFun :: rtt:proc_node_file_fun(),
    FileFunParam :: term() )
        -> nonempty_list(term()) | rtt:std_error().
process_node_logs(all, FileFun, FFParam) ->
    PathMap = get_tag_path_map(),
    VsnMap  = get_nid_tag_map(),
    FoldFun = fun(Node, NodeNum, Recs) ->
        Path = maps:get(maps:get(NodeNum, VsnMap), PathMap),
        [{Node, NodeNum, Path} | Recs]
    end,
    NodeRecs = maps:fold(FoldFun, [], get_node_nid_map()),
    process_node_log_recs(NodeRecs, FileFun, FFParam);
process_node_logs([_|_] = Nodes, FileFun, FFParam) ->
    NodeMap = get_node_nid_map(),
    PathMap = get_tag_path_map(),
    VsnMap  = get_nid_tag_map(),
    FoldFun = fun(N, Recs) ->
        I = maps:get(N, NodeMap),
        P = maps:get(maps:get(N, VsnMap), PathMap),
        [{N, I, P} | Recs]
    end,
    NodeRecs = lists:foldl(FoldFun, [], Nodes),
    process_node_log_recs(NodeRecs, FileFun, FFParam).

%% @hidden Common result of process_node_logs/3
-spec process_node_log_recs(
    NodeRecs :: list({Node :: node(), LogFile :: rtt:fs_path()}),
    FileFun :: rtt:proc_node_file_fun(),
    FileFunParam :: term() )
        -> nonempty_list(term()) | rtt:std_error().
process_node_log_recs([_|_] = NodeRecs, FileFun, FFParam) ->
    FileRecs = lists:foldl(fun get_node_log_files/2, [], NodeRecs),
    LogFun = fun({Node, LogFile}) -> FileFun(Node, LogFile, FFParam) end,
    rt:pmap(LogFun, FileRecs);
process_node_log_recs([], _FileFun, _FFParam) ->
    {error, enoent}.

%% @hidden List fold used only by process_node_log_recs/3
-spec get_node_log_files(
    NodeRec :: {node(), rtt:node_id(), vsn_root_path()},
    Recs :: list({Node :: node(), LogFile :: rtt:fs_path()}) )
        -> list({Node :: node(), LogFile :: rtt:fs_path()}).
get_node_log_files({Node, NodeNum, RelPath}, Recs) ->
    Wildcard = filename:join([RelPath, dev_riak_dir(NodeNum), "log", "*"]),
    [{Node, File} || File <- filelib:wildcard(Wildcard)] ++ Recs.

%% @private
-spec get_node_debug_logs() -> list({ok, rtt:fs_path()} | rtt:std_error()).
get_node_debug_logs() ->
    rt:pmap(fun get_node_debug_log/1, maps:to_list(get_nid_tag_map())).

%% @hidden Used only by get_node_debug_logs/0
get_node_debug_log({NodeNum, NodeVsn}) ->
    DebugLogFile = ?DEBUG_LOG_FILE(NodeNum),
    %% If the debug log file exists from a previous test run it will cause the
    %% `riak_debug_cmd' to fail, so delete `DebugLogFile' if it exists.  Ignore
    %% the result of `file:delete/1' rather than bothering to see whether the
    %% file's there in the first place.
    _ = file:delete(DebugLogFile),
    Path = relpath(NodeVsn),
    [Exe | Args] = CmdList = riak_debug_cmd(Path, NodeNum, ["--logs"]),
    CmdLine = rt_exec:cmd_line(CmdList),
    ?LOG_INFO("Running: ~s", [CmdLine]),
    case rt_exec:cmd(Exe, Args) of
        {error, _} = Error ->
            Error;
        {0, _Output} ->
            DebugLogFile;
        {RC, Out} = Ret ->
            ?LOG_INFO("~s ExitCode ~b, Output = ~0p", [CmdLine, RC, Out]),
            case filelib:is_file(DebugLogFile) of
                true ->
                    DebugLogFile;
                _ ->
                    {error, Ret}
            end
    end.

%% @doc Performs a search against the log files on `Node' and returns all
%% matching lines.
-spec search_logs(Node :: node(), Pattern :: iodata())
        -> list(rtt:file_line_match() | rtt:std_error()).
search_logs(Node, Pattern) ->
    RegEx = case re:compile(Pattern) of
        {ok, MP} ->
            MP;
        {error, ReError} ->
            erlang:error(ReError, [re, compile, Pattern])
    end,
    RootPath = relpath(root),
    Wildcard = filename:join([
        "*", "dev", node_name(Node), "riak", "log", "*"]),
    LogFiles = filelib:wildcard(Wildcard, RootPath),
    SrchFun = fun(FileName) ->
        FilePath = filename:join(RootPath, FileName),
        search_file(FilePath, FileName, RegEx)
    end,
    AllMatches = rt:pmap(SrchFun, LogFiles),
    lists:flatten(AllMatches).

-spec search_file(
    FilePath :: rtt:fs_path(),  % full path used to open the file
    FileName :: rtt:fs_path(),  % relative path for result reporting
    RegEx :: re:mp() )
        -> list(rtt:file_line_match()) | rtt:std_error().
search_file(FilePath, FileName, RegEx) ->
    case file:open(FilePath, [read, binary, raw, read_ahead]) of
        {ok, IoDev} ->
            Matches = search_file(IoDev, FileName, RegEx, 1, []),
            _ = file:close(IoDev),
            lists:reverse(Matches);
        {error, Reason} ->
            {error, {Reason, FilePath}}
    end.

search_file(IoDev, File, RegEx, LineNum, Accum) ->
    case file:read_line(IoDev) of
        {ok, BinLine} ->
            NewAccum = case re:run(BinLine, RegEx, [{capture, none}]) of
                match ->
                    Match = {File, LineNum, erlang:binary_to_list(BinLine)},
                    [Match | Accum];
                _Nomatch ->
                    Accum
            end,
            search_file(IoDev, File, RegEx, (LineNum + 1), NewAccum);
        eof ->
            Accum;
        {error, Reason} ->
            [{error, {Reason, File}} | Accum]
    end.

-spec node_name(node()) -> string().
node_name(Node) ->
    lists:takewhile(fun(C) -> C /= $@ end, atom_to_list(Node)).

-ifdef(TEST).

release_versions_test() ->
    Config = [
        {root,      "/no/such/rt/root/riak"},
        {current,   "/no/such/rt/root/riak/3.0.14"},
        {previous,  "/no/such/rt/root/riak/3.0.6"},
        {legacy,    "/no/such/rt/root/riak/2.0.20"},
        {'2.9.5',   "/no/such/rt/root/riak/2.9.5"},
        {"3.0.3",   "/no/such/rt/root/riak/3.0.3"},
        {<<3,2,1>>, "/no/such/rt/root/riak/3.2.1"}
    ],
    LoadRes = case application:load(riak_test) of
        ok ->
            ok;
        {error, {already_loaded, riak_test}} ->
            ok;
        Other ->
            Other
    end,
    ?assertMatch(ok, LoadRes),
    ok = rt_config:set(rtdev_path, Config),

    ?assertEqual("/no/such/rt/root/riak/3.0.6", relpath(previous)),
    ?assertEqual("/no/such/rt/root/riak/2.9.5", relpath('2.9.5')),
    ?assertEqual("/no/such/rt/root/riak/3.0.3", relpath("3.0.3")),
    ?assertEqual("/no/such/rt/root/riak/3.2.1", relpath(<<3,2,1>>)),
    ?assertError({bad_version, "3.2.1"},        relpath("3.2.1")),

    ?assertMatch(unknown,           get_version(legacy)),
    ?assertMatch(unknown,           get_vsn_rec("3.0.3")),
    ?assertError({badkey, '3.0.3'}, get_version('3.0.3')),

    PathMap = get_tag_path_map(),
    ?assert(maps:is_key(current,    PathMap)),
    ?assert(maps:is_key('2.9.5',    PathMap)),
    ?assert(maps:is_key("3.0.3",    PathMap)),
    ?assert(maps:is_key(<<3,2,1>>,  PathMap)).

-endif.
