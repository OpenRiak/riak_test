%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
%% Copyright (c) 2018-2023 Workday, Inc.
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
%% @doc
%% Implements the base `riak_test' API, providing the ability to control
%% nodes in a Riak cluster as well as perform commonly reused operations.
%%
%% Please extend this module with new functions that prove useful between
%% multiple independent tests.
%%
%% Several functions didn't initially have any handling of error conditions,
%% instead just generating a `badmatch' error if anything went wrong. This
%% pattern annoys dialyzer mightily, so where feasible they've been revised
%% to return error indicators. However, there were functions employing this
%% pattern that are used in a LOT of tests, so we can't easily update their
%% APIs to return errors - our only recourse is to explicitly throw an error
%% exception, so I've tried to make them informative.
%%
%% @todo The functions for running external commands are semantically broken.
%% Functions ```
%%      cmd(Cmd, Args)
%%      cmd(Cmd, Args, Form)
%%      spawn_cmd(Cmd, Args)
%%      stream_cmd(Cmd) %% unused, not maintained'''
%% really only work with a harness running devrels, i.e. `rtdev'.
%% The specs <i>SHOULD</i> be```
%%      *cmd(Node, Cmd, Args)'''
%% in order to run on the machine hosting the `Node', not the harness, but
%% it's not worth the effort to make that change unless/until someone wants
%% to resurrect the `rtssh' (or similar) remote harness.
%%
%% @end
-module(rt).

%% Public API
%% These functions are legitimately usable by test modules.
-export([
    admin/2, admin/3,
    admin_stats/1,
    assert_capability/3,
    assert_nodes_agree_about_ownership/1,
    assert_supported/3,
    async_start/1,
    brutal_kill/1,
    build_cluster/1, build_cluster/2, build_cluster/3,
    build_clusters/1,
    capability/2, capability/3,
    check_ibrowse/0,
    claimant_according_to/1,
    clean_cluster/1,
    clean_data_dir/2,
    cmd/2, cmd/3,
    connection_info/1,
    copy_conf/3,
    create_activate_and_wait_for_bucket_type/3,
    create_and_activate_bucket_type/3,
    deploy_clusters/1,
    deploy_nodes/1, deploy_nodes/2, deploy_nodes/3,
    down/2,
    enable_search_hook/2,
    expect_in_log/2,
    expect_not_in_logs/2,
    get_backends/0,
    get_http_conn_info/1,
    get_https_conn_info/1,
    get_ip/1,
    get_node_debug_logs/0,
    get_node_id/1,
    get_node_path/1,
    get_node_version/1,
    get_pb_conn_info/1,
    get_preflist/3, get_preflist/4,
    get_replica/5,
    get_ring/1,
    get_stat/2,
    get_stats/1, get_stats/2,
    get_version/0, get_version/1,
    get_vsn_rec/0, get_vsn_rec/1,
    heal/1,
    heal_upnodes/1,
    http_url/1,
    httpc/1,
    httpc_read/3,
    httpc_write/4, httpc_write/5,
    https_url/1,
    interact/3,
    is_mixed_cluster/1,
    is_pingable/1,
    join/2,
    join_cluster/1,
    leave/1,
    load_modules_on_nodes/2,
    log_to_nodes/2, log_to_nodes/3,
    members_according_to/1,
    nearest_ringsize/1,
    otp_release/0, otp_release/1,
    owners_according_to/1,
    partition/2,
    partitions_for_node/1,
    pbc/1, pbc/2,
    pbc_put_dir/3,
    pbc_put_file/4,
    pbc_read/3, pbc_read/4,
    pbc_read_check/4, pbc_read_check/5,
    pbc_really_deleted/3,
    pbc_set_bucket_prop/3,
    pbc_systest_read/2, pbc_systest_read/3, pbc_systest_read/5,
    pbc_systest_write/2, pbc_systest_write/3, pbc_systest_write/5,
    pbc_write/4, pbc_write/5, pbc_write/6,
    plan_and_commit/1,
    pmap/2, pmap/3,
    post_result/2,
    priv_dir/0,
    process_node_logs/3,
    product/1,
    random_sublist/2,
    random_uniform/0, random_uniform/1,
    remove/2,
    reset_cookie/3,
    restore_data_dir/3,
    riak/2,
    riak_repl/2,
    rpc_get_env/2,
    search_cmd/2,
    select_random/1,
    set_advanced_conf/2,
    set_backend/1,
    set_conf/2,
    setup_harness/2,
    setup_log_capture/1,
    slow_upgrade/3,
    spawn_cmd/2,
    spawned_result/1,
    staged_join/2,
    staged_leave/1,
    start/1,
    start_and_wait/1,
    start_naked/1,
    status_of_according_to/2,
    stop/1,
    stop_and_wait/1,
    stop_tracing/0,
    str/2,
    stream_cmd/1, stream_cmd/2,
    systest_delete/2, systest_delete/3, systest_delete/5,
    systest_read/2, systest_read/3, systest_read/5, systest_read/6, systest_read/7,
    systest_verify_delete/2, systest_verify_delete/3, systest_verify_delete/5,
    systest_write/2, systest_write/3, systest_write/5, systest_write/6,
    try_nodes_ready/3,
    update_app_config/2,
    upgrade/2, upgrade/3, upgrade/4,
    versions/0,
    wait_for_any_webmachine_route/2,
    wait_for_cluster_service/2,
    wait_for_control/1, wait_for_control/2,
    wait_for_service/2,
    wait_until/1, wait_until/2, wait_until/3, wait_until/4,
    wait_until_aae_trees_built/1,
    wait_until_all_members/1, wait_until_all_members/2,
    wait_until_bucket_props/3,
    wait_until_bucket_type_status/3,
    wait_until_bucket_type_visible/2,
    wait_until_capability/3, wait_until_capability/4,
    wait_until_capability_contains/3,
    wait_until_connected/1,
    wait_until_legacy_ringready/1,
    wait_until_no_pending_changes/1,
    wait_until_node_handoffs_complete/1,
    wait_until_nodes_agree_about_ownership/1,
    wait_until_nodes_ready/1,
    wait_until_owners_according_to/2,
    wait_until_pingable/1,
    wait_until_ready/1,
    wait_until_registered/2,
    wait_until_ring_converged/1,
    wait_until_status_ready/1,
    wait_until_transfers_complete/1,
    wait_until_unpingable/1
]).

%% Local API
%% These functions are only to be called by modules in the `src' directory.
%% Note that `src' modules *also* use functions exported for tests.
-export([
    check_singleton_node/1,
    ensure_network_node/0,
    get_backend/1,
    get_deps/0,
    get_retry_settings/0,
    set_backend/2,
    teardown/0,
    whats_up/0
]).

%% Deprecated APIs
-export([
    attach/2,
    attach_direct/2,
    console/2
]).
-deprecated([
    attach/2,
    attach_direct/2,
    console/2
]).

%% Private API
%% These functions are only exported for use as callbacks - never called
%% directly or from outside this module.
-export([
    pmap_child/3
]).

%% There are a LOT of unused functions in here, comment out to work on cleanup.
%% TODO: The goal is to not have any unused functions!
-compile([
    nowarn_unused_function
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rt.hrl").

-define(HARNESS,        (rt_harness())).
-define(RT_ETS,         rt_ets).
-define(RT_ETS_OPTS,    [public, named_table, {write_concurrency, true}]).

%% Syntactic shortcuts
-define(is_pos_integer(N), (erlang:is_integer(N) andalso N > 0)).
-define(is_timeout(T),  (?is_pos_integer(T) andalso T < (1 bsl 32))).

%% @hidden The harness module is used a lot, so cache it in the process state.
%% It may wind up being cached in multiple processes, but still a lot cheaper
%% than fetching and filtering from the global application table on every use.
%% Use `rtdev' explicitly for xref and dialyzer so we get suitable analysis
%% for the common use case - or set to `rtssh' to bite off fixing that module.
-spec rt_harness() -> module().
-ifdef(CHECK).
rt_harness() ->
    rtdev.
-else.
rt_harness() ->
    case erlang:get(rt_harness) of
        undefined ->
            Mod = rt_config:get(rt_harness),
            erlang:put(rt_harness, Mod),
            Mod;
        Val ->
            Val
    end.
-endif. % CHECK

%% @private Ensure that the configured or default riak_test node is running
%% with the configured or default cookie.
%%
%% If the node is already started with a different cookie than this function
%% would have used a `bad_cookie' error is raised.
%%
%% The implementing harness must ensure that its default node is reachable by
%% all test nodes.
-spec ensure_network_node() -> ok | no_return().
ensure_network_node() ->
    RtNode = rt_config:get(rt_nodename, ?HARNESS:default_node_name()),
    Cookie = rt_config:get(rt_cookie, riak),
    ensure_epmd(),
    case net_kernel:start([RtNode]) of
        {ok, _} ->
            true = erlang:get_cookie() =:= Cookie
                orelse erlang:set_cookie(RtNode, Cookie),
            ?LOG_NOTICE("Node '~s' started with cookie '~s'", [RtNode, Cookie]);
        {error, {already_started, _}} ->
            case erlang:get_cookie() of
                Cookie ->
                    ok;
                Other ->
                    erlang:error(bad_cookie, [Other])
            end;
        {error, Reason} ->
            erlang:error(Reason, [net_kernel, start, [RtNode]])
    end.

%% @hidden Ensure that EPMD is running
-spec ensure_epmd() -> ok | no_return().
ensure_epmd() ->
    case erl_epmd:names() of
        {ok, _} ->
            ok;
        _ ->
            ?LOG_NOTICE("EPMD not running, starting it ..."),
            ErtsEpmd = filename:join([code:root_dir(), bin, epmd]),
            EpmdExe = case rt_exec:resolve_cmd(ErtsEpmd) of
                {error, Posix} ->
                    ?LOG_WARNING("'~s': ~s",
                        [ErtsEpmd, file:format_error(Posix)]),
                    case rt_exec:resolve_cmd("epmd") of
                        {error, Fatal} ->
                            ?LOG_ERROR("No EPMD available: ~s",
                                [file:format_error(Fatal)]),
                            erlang:error(Fatal, [epmd]);
                        Found ->
                            ?LOG_WARNING(
                                "Preferred EPMD not found, using '~s'", [Found]),
                            Found
                    end;
                FQExe ->
                    FQExe
            end,
            ?assertMatch({0, _}, rt_exec:cmd(EpmdExe, ["-daemon"])),
            ok
    end.

%% @doc Attempts to locate the 'priv' directory.
%%
%% Since we're likely running as an escript, this can be easier said than done.
%%
%% Try, in order:<ul>
%%  <li>`dirname(escript:script_name)/priv'</li>
%%  <li>`./priv'</li>
%%  <li>`code:priv_dir()'</li>
%% </ul>
%% If none exists, throws `error:bad_priv_dir'
-spec priv_dir() -> rtt:fs_path().
priv_dir() ->
    case application:get_env(riak_test, priv_dir) of
        {ok, SavedDir} ->
            SavedDir;
        _ ->
            case find_priv_dir([escript, cwd, code]) of
                false ->
                    erlang:error(bad_priv_dir);
                PrivDir ->
                    ok = application:set_env(riak_test, priv_dir, PrivDir),
                    ?LOG_INFO("Using priv dir: '~s'", [PrivDir]),
                    PrivDir
            end
    end.

-spec find_priv_dir(list(code | cwd | escript) | rtt:fs_path())
        -> false | rtt:fs_path().
find_priv_dir([]) ->
    false;
find_priv_dir([code | Modes]) ->
    %% Maybe not running as an escript ...
    case code:priv_dir(riak_test) of
        {error, _} ->
            find_priv_dir(Modes);
        CodePriv ->
            ?LOG_DEBUG("code:priv_dir/1 returned '~s'", [CodePriv]),
            %% In an escript, this is not going to be a real directory.
            case filelib:is_dir(CodePriv) of
                true ->
                    CodePriv;
                _ ->
                    find_priv_dir(Modes)
            end
    end;
find_priv_dir([cwd | Modes]) ->
    {ok, CWD} = file:get_cwd(),
    case find_priv_dir(CWD) of
        false ->
            find_priv_dir(Modes);
        PrivDir ->
            PrivDir
    end;
find_priv_dir([escript | Modes]) ->
    %% escript:script_name/0 performs NO error handling, so it'll throw a
    %% badmatch exception if there are no command-line arguments to init.
    case (catch escript:script_name()) of
        ScrName when erlang:is_list(ScrName) ->
            ?LOG_DEBUG("escript:script_name/0 returned '~s'", [ScrName]),
            case confirm_escript(ScrName) of
                false ->
                    find_priv_dir(Modes);
                ScrFile ->
                    case find_priv_dir(filename:dirname(ScrFile)) of
                        false ->
                            find_priv_dir(Modes);
                        PrivDir ->
                            PrivDir
                    end
            end;
        _ ->
            find_priv_dir(Modes)
    end;
find_priv_dir(BaseDir) ->
    %% Assume BaseDir is absolute.
    PrivDir = filename:join(BaseDir, "priv"),
    case filelib:is_dir(PrivDir) of
        true ->
            lists:flatten(PrivDir);
        _ ->
            false
    end.

%% escript:script_name/0 is pretty weak - it just returns the first
%% command-line argument - so just checking that it's a file doesn't confirm
%% much. At least verify that it *looks* like an escript with a suitable
%% shebang.
%% Standard is "#!/usr/bin/env escript"
-spec confirm_escript(rtt:fs_path()) -> false | nonempty_string().
confirm_escript(ScrFile) ->
    case filelib:is_regular(ScrFile) of
        true ->
            case file:open(ScrFile, [read]) of
                {ok, IoDev} ->
                    ReadLine = file:read_line(IoDev),
                    _ = file:close(IoDev),
                    case ReadLine of
                        {ok, Line} ->
                            case re:run(Line,
                                    "^#!.+\\bescript\\b", [{capture, none}]) of
                                match ->
                                    filename:absname(ScrFile);
                                _ ->
                                    false
                            end;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

%% @doc gets riak deps from the appropriate harness
-spec get_deps() -> rtt:fs_path().
get_deps() ->
    ?HARNESS:get_deps().

%% @doc if String contains Substr, return true.
-spec str(String :: string(), Substr :: string()) -> boolean().
str(String, Substr) ->
    string:find(String, Substr) /= nomatch.

%% @doc Appends the specified elements to `riak.conf'
-spec set_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: list({string(), string()}) )
        -> ok | rtt:std_error().
set_conf(all, NameValuePairs) ->
    ?HARNESS:set_conf(all, NameValuePairs);
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    stop(Node),
    ?assertEqual(ok, wait_until_unpingable(Node)),
    ?HARNESS:set_conf(Node, NameValuePairs),
    start(Node).

%% @doc Updates `advanced.config'
-spec set_advanced_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() )
        -> ok | rtt:std_error().
set_advanced_conf(all, NameValuePairs) ->
    ?HARNESS:set_advanced_conf(all, NameValuePairs);
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:set_advanced_conf(Node, NameValuePairs),
    start(Node).

%% @doc Rewrite the given node's app.config file, overriding the variables
%%      in the existing app.config with those in `Config'.
-spec update_app_config(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() )
        -> ok | rtt:std_error().
update_app_config(all, Config) ->
    ?HARNESS:update_app_config(all, Config);
update_app_config(Node, Config) ->
    stop(Node),
    ?assertEqual(ok, wait_until_unpingable(Node)),
    ?HARNESS:update_app_config(Node, Config),
    start(Node).

%% @doc Helper that returns first successful application get_env result,
%%      used when different versions of Riak use different app vars for
%%      the same setting.
-spec rpc_get_env(
    Node :: node(),
    AppVars :: nonempty_list(rtt:config_elem()) )
        -> {ok, term()} | undefined.
rpc_get_env(_, []) ->
    undefined;
rpc_get_env(Node, [{App,Var}|Others]) ->
    case rpc:call(Node, application, get_env, [App, Var]) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            rpc_get_env(Node, Others)
    end.

%% @doc Returns the interfaces for one or a group of nodes.
%%
%% This is a poorly designed and misused interface, but at this point it's
%% used by a *LOT* of tests that should instead be using one of the
%%  `get_<proto>_conn_info/1'
%% functions.
-ifdef(COVARIANT_SPECS).
-spec connection_info(
    Node :: node() ) -> rtt:interfaces();
(
    Nodes :: rtt:nodes() ) -> rtt:conn_info_list().
-else.  % EDoc
-spec connection_info(node() | rtt:nodes())
        -> rtt:interfaces() | rtt:conn_info_list().
-endif. % COVARIANT_SPECS
connection_info(Node) when erlang:is_atom(Node) ->
    {ok, PbEndpoint} = get_pb_conn_info(Node),
    {ok, HtEndpoint} = get_http_conn_info(Node),
    case get_https_conn_info(Node) of
        undefined ->
            [{http, HtEndpoint}, {pb, PbEndpoint}];
        {ok, HtsEndpoint} ->
            [{http, HtEndpoint}, {https, HtsEndpoint}, {pb, PbEndpoint}]
    end;
connection_info([_|_] = Nodes) ->
    [{Node, connection_info(Node)} || Node <- Nodes].

-spec get_pb_conn_info(Node :: node())
        -> {ok, rtt:net_endpoint()} | undefined.
get_pb_conn_info(Node) ->
    case rpc_get_env(Node,
            [{riak_api, pb}, {riak_api, pb_ip}, {riak_kv, pb_ip}]) of
        {ok, [{_IP, _Port} = Endpoint | _]} ->
            {ok, Endpoint};
        {ok, PB_IP} ->
            {ok, PB_Port} = rpc_get_env(Node,
                [{riak_api, pb_port}, {riak_kv, pb_port}]),
            {ok, {PB_IP, PB_Port}};
        _ ->
            undefined
    end.

-spec get_http_conn_info(Node :: node())
        -> {ok, rtt:net_endpoint()} | undefined.
get_http_conn_info(Node) ->
    case rpc_get_env(Node, [{riak_api, http}, {riak_core, http}]) of
        {ok, [{_IP, _Port} = Endpoint | _]} ->
            {ok, Endpoint};
        _ ->
            undefined
    end.

-spec get_https_conn_info(Node :: node())
        -> {ok, rtt:net_endpoint()} | undefined.
get_https_conn_info(Node) ->
    case rpc_get_env(Node, [{riak_api, https}, {riak_core, https}]) of
        {ok, [{_IP, _Port} = Endpoint | _]} ->
            {ok, Endpoint};
        _ ->
            undefined
    end.

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
-spec deploy_nodes(
    pos_integer() | nonempty_list(rtt:vsn_tag() | rtt:node_config()))
        -> rtt:nodes().
deploy_nodes([_|_] = Versions)  ->
    deploy_nodes(Versions, [riak_kv]);
deploy_nodes(NumNodes) when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    deploy_nodes(lists:duplicate(NumNodes, {current, default}), [riak_kv]).

%% @doc Deploy a set of freshly installed Riak nodes with the given
%%      `InitialConfig', returning a list of the nodes deployed.
-ifdef(COVARIANT_SPECS).
-spec deploy_nodes(
    NumNodes :: pos_integer(),
    InitialConfig :: rtt:app_config() | {cuttlefish, rtt:cuttlefish_config()} )
        -> rtt:nodes(); (
    Versions :: rtt:deploy_versions(), Services :: rtt:services() )
        -> rtt:nodes().
-else.  % EDoc
-spec deploy_nodes(
    pos_integer() | rtt:deploy_versions(),
    rtt:app_config() | {cuttlefish, rtt:cuttlefish_config()} | rtt:services() )
        -> rtt:nodes().
-endif. % COVARIANT_SPECS
deploy_nodes(NumNodes, InitialConfig)
        when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    deploy_nodes(NumNodes, InitialConfig, [riak_kv]);
deploy_nodes([_|_] = Versions, Services) ->
    NodeConfigs = [version_to_config(Version) || Version <- Versions],
    Nodes = ?HARNESS:deploy_nodes(NodeConfigs),
    ?LOG_INFO("Waiting for services ~0p to start on ~0p.", [Services, Nodes]),
    pmap(fun(Node) -> wait_for_service(Node, Services) end, Nodes),
    Nodes.

%% @doc Deploy `NumNodes' freshly installed `current' Riak nodes with the
%% specified `InitialConfig', wait for them to report `Services' running, and
%% return the list of nodes.
-spec deploy_nodes(
    NumNodes :: pos_integer(),
    Config :: rtt:app_config(),
    Services :: rtt:services() )
        -> rtt:nodes().
deploy_nodes(NumNodes, Config, Services)
        when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    deploy_nodes(lists:duplicate(NumNodes, {current, Config}), Services).

-spec version_to_config(rtt:deploy_version()) -> rtt:node_config().
%% Must differentiate between a vsn_tag(), which can be a nonempty_string(),
%% and an app_config(), which is a (possibly empty) list of 2-tuples.
%% Match a bunch of head patterns and let the compiler distill it down to
%% what it needs.
version_to_config({_Tag, default} = VsnConf) ->
    VsnConf;
version_to_config({VsnTag, []}) ->
    {VsnTag, default};
version_to_config({_Tag, [{K,_}|_]} = VsnConf) when erlang:is_atom(K) ->
    VsnConf;
version_to_config({_Tag, {cuttlefish,_}} = VsnConf) ->
    VsnConf;
version_to_config([]) ->
    {current, default};
version_to_config([{K,_}|_] = Config) when erlang:is_atom(K) ->
    {current, Config};
version_to_config(VsnTag) when erlang:is_atom(VsnTag) ->
    {VsnTag, default};
version_to_config(VsnTag) when erlang:is_binary(VsnTag) ->
    {VsnTag, default};
version_to_config([C|_] = VsnTag) when erlang:is_integer(C) ->
    {VsnTag, default};
version_to_config(Arg) ->
    erlang:error(badarg, [Arg]).

-spec deploy_clusters(ClusterConfigs :: rtt:cluster_configs()) -> rtt:clusters().
deploy_clusters([_|_] = ClusterConfigs) ->
    ?HARNESS:deploy_clusters([cluster_config(CC) || CC <- ClusterConfigs]);
deploy_clusters(Arg) ->
    erlang:error(badarg, [Arg]).

-spec cluster_config(ClusterConfig :: rtt:cluster_config())
        -> rtt:node_configs().
cluster_config([_|_] = AppConfigs) ->
    [{current, AC} || AC <- AppConfigs];
cluster_config(NumNodes)
        when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    lists:duplicate(NumNodes, {current, default});
cluster_config({NumNodes, Config})
        when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    lists:duplicate(NumNodes, {current, Config});
cluster_config({NumNodes, Vsn, Config})
        when erlang:is_integer(NumNodes) andalso NumNodes > 0 ->
    lists:duplicate(NumNodes, {Vsn, Config});
cluster_config(Arg) ->
    erlang:error(badarg, [Arg]).

-spec build_clusters(ClusterConfigs :: rtt:cluster_configs())
        -> rtt:clusters().
build_clusters(ClusterConfigs) ->
    Clusters = deploy_clusters(ClusterConfigs),
    lists:foreach(
        fun(Cluster) ->
            join_cluster(Cluster),
            ?LOG_INFO("Cluster built: ~0p", [Cluster])
        end, Clusters),
    Clusters.

%% @doc Start the specified Riak node
-spec start(Node :: node()) -> ok | rtt:std_error().
start(Node) ->
    ?HARNESS:start(Node).

%% @doc Start the specified Riak node without any hooks
%% (no cover, intercepts, etc)
-spec start_naked(Node :: node()) -> ok | rtt:std_error().
start_naked(Node) ->
    ?HARNESS:start_naked(Node).

%% @doc Start the specified Riak `Node' and wait for it to be pingable
-spec start_and_wait(Node :: node()) -> ok | rtt:std_error().
start_and_wait(Node) ->
    start(Node),
    ?assertEqual(ok, wait_until_pingable(Node)).

-spec async_start(Node :: node()) -> ok | rtt:std_error().
async_start(Node) ->
    erlang:spawn(fun() -> start(Node) end),
    ok.

%% @doc Stop the specified Riak `Node'.
-spec stop(Node :: node()) -> ok | rtt:std_error().
stop(Node) ->
    ?LOG_INFO("Stopping riak on ~0p", [Node]),
    %% The following sleep was added in 2012, with a commit comment that it
    %% was temporary! Let's see whether we can now live without it ...
    %% TODO: Remove this comment along with the sleep once we've run all
    %%       current relevant tests successfully
    % timer:sleep(10000), %% I know, I know!
    ?HARNESS:stop(Node).

%% @doc Stop the specified Riak `Node' and wait until it is not pingable
-spec stop_and_wait(Node :: node()) -> ok | rtt:std_error().
stop_and_wait(Node) ->
    stop(Node),
    ?assertEqual(ok, wait_until_unpingable(Node)).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion'.
%% @todo This upgrade function approach won't work with non-local harnesses.
%% @end
%% The built-in upgrade function deals with upgrading from a version prior
%% to v2.2.5 to one after it. In reality, that's unlikely to ever happen any
%% more, but keep the behavior until riak_ee is no longer in use anywhere,
%% since it's non-destructive to newer configs.
-spec upgrade(Node :: node(), NewVersion :: rtt:vsn_tag())
        -> ok | rtt:std_error().
upgrade(Node, current) ->
    upgrade(Node, current, fun upgrade_cleanup_old_config/1);
upgrade(Node, NewVersion) ->
    %% Ain't nobody "upgrading" to a non-current version.
    upgrade(Node, NewVersion, fun no_op/1).

%% @private Upgrade function to clean out old ee properties from configs.
%% @end
%% Based on replication2_upgrade:remove_jmx_from_conf/1, which used to be
%% called from the upgrade/2 function - that dependency seemed less than
%% desirable, so the functionality has been moved here.
-spec upgrade_cleanup_old_config(Params :: proplists:proplist()) -> ok.
upgrade_cleanup_old_config(Params) ->
    case proplists:get_value(new_conf_dir, Params) of
        undefined ->
            ok;
        NewConfPath ->
            cleanup_old_riak_conf(filename:join(NewConfPath, "riak.conf"))
    end.

%% @private Clean out old riak_ee properties from configs.
%%
%% Deletes config lines for packages removed at v2.2.5, i.e. the transition
%% of Basho `riak_ee' to Community `riak'.
%% @end
%% Based on replication2_upgrade:remove_jmx_from_conf/1, which used to be
%% called from the upgrade/2 function - that dependency seemed less than
%% desirable, so the functionality has been moved here.
-spec cleanup_old_riak_conf(RiakConfFile :: rtt:fs_path()) -> ok.
cleanup_old_riak_conf(RiakConfFile) ->
    %% On any remotely standard Unix, we don't need to search.
    SedExe = "/usr/bin/sed",
    %% note the sed filter script is prefix-based, so very broad
    SedFilt = "/^[[:space:]]*(jmx|search)\\b/d",
    Params = ["-E", "-i.bak", "-e", SedFilt, RiakConfFile],
    CmdLineElems = [SedExe | Params],
    CmdLine = rt_exec:cmd_line(CmdLineElems),
    ?LOG_INFO("Cleaning config with cmd: ~s", [CmdLine]),
    case cmd(SedExe, Params) of
        {0, _Output} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason, CmdLineElems);
        ErrResult ->
            erlang:error(ErrResult, CmdLineElems)
    end.

%% Keep the more flexible API and tell dialyzer it's ok that it's not used.
%% Remove this attribute if you add a call to upgrade(_, _, rtt:app_config()).
-dialyzer({no_match, upgrade/3}).
%% @doc Upgrade a Riak `Node' to the specified `NewVersion'.
%%
%% If an Upgrade Callback is specified it will be called after the node is
%% stopped and before the upgraded node is started.
%%
%% If a Node Config is specified the effect is equivalent to```
%%      upgrade(Node, NewVersion, Config, fun no_op/1)'''
-spec upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_tag(),
    CallbackOrConfig :: rtt:upgrade_cb() | rtt:app_config() )
        -> ok | rtt:std_error().
upgrade(Node, NewVersion, UpgradeCallback)
        when erlang:is_function(UpgradeCallback, 1) ->
    ?HARNESS:upgrade(Node, NewVersion, UpgradeCallback);
upgrade(Node, NewVersion, Config) ->
    upgrade(Node, NewVersion, Config, fun no_op/1).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion' and update
%% the config based on entries in `Config'.
%%
%% `UpgradeCallback' will be called after the node is stopped but before
%% the upgraded node is started.
-spec upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_tag(),
    NewAppConfig :: rtt:app_config(),
    UpgradeCallback :: rtt:upgrade_cb() )
        -> ok | rtt:std_error().
upgrade(Node, NewVersion, Config, UpgradeCallback) ->
    ?HARNESS:upgrade(Node, NewVersion, Config, UpgradeCallback).

%% @doc Upgrade a Riak node to a specific version using the alternate
%%      leave/upgrade/rejoin approach
slow_upgrade(Node, NewVersion, Nodes) ->
    ?LOG_INFO("Perform leave/upgrade/join upgrade on ~0p", [Node]),
    ?LOG_INFO("Leaving ~0p", [Node]),
    leave(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    upgrade(Node, NewVersion),
    ?LOG_INFO("Rejoin ~0p", [Node]),
    join(Node, hd(Nodes -- [Node])),
    ?LOG_INFO("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
    ok.

%% Ideally we'd use `wait_until' for join retries, but it isn't
%% currently flexible enough to only retry on a specific error
%% tuple. Rather than rework all of the various arities, steal the
%% concept and worry about this later if the need arises.
-spec join_with_retry(fun(() -> ok | rtt:std_error()))
        -> ok | rtt:std_error().
join_with_retry(Fun) ->
    {Delay, Retry} = get_retry_settings(),
    join_retry(Fun(), Fun, Retry, Delay).

join_retry(ok, _Fun, _Retry, _Delay) ->
    ok;
join_retry({error, node_still_starting}, _Fun, 0, _Delay) ->
    ?LOG_WARNING("Too many retries, join failed"),
    {error, too_many_retries};
join_retry({error, node_still_starting}, Fun, RetryCount, Delay) ->
    ?LOG_WARNING("Join error because node is not yet ready, retrying after ~Bms", [Delay]),
    timer:sleep(Delay),
    join_retry(Fun(), Fun, RetryCount - 1, Delay);
join_retry(Error, _Fun, _Retry, _Delay) ->
    Error.

%% @doc Have `Node' send a join request to `PNode'
-spec join(Node :: node(), PNode :: node()) -> ok.
join(Node, PNode) ->
    Fun = fun() -> rpc:call(Node, riak_core, join, [PNode]) end,
    ?LOG_INFO("[join] ~0p to (~0p)", [Node, PNode]),
    ?assertEqual(ok, join_with_retry(Fun)).

%% @doc Have `Node' send a join request to `PNode'
-spec staged_join(Node :: node(), PNode :: node()) -> ok.
staged_join(Node, PNode) ->
    %% `riak_core:staged_join/1' can now return an `{error,
    %% node_still_starting}' tuple which indicates retry.
    Fun = fun() -> rpc:call(Node, riak_core, staged_join, [PNode]) end,
    ?LOG_INFO("[join] ~0p to (~0p)", [Node, PNode]),
    ?assertEqual(ok, join_with_retry(Fun)).

%% @doc Have `Node' send a leave request
staged_leave(Node) ->
    Fun = fun() -> rpc:call(Node, riak_core_console, stage_leave, [Node]) end,
    ?LOG_INFO("[leave] ~0p from cluster", [Node]),
    ok = Fun(),
    ok.

-spec plan_and_commit(Node :: node()) -> ok.
plan_and_commit(Node) ->
    timer:sleep(1000),
    ?LOG_INFO("planning cluster change"),
    SW = os:timestamp(),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            ?LOG_INFO("plan: ring not ready"),
            timer:sleep(100),
            plan_and_commit(Node);
        {ok, _Changes, NextRings} ->
            ?LOG_INFO(
                "plan: done in ~b ms",
                [timer:now_diff(os:timestamp(), SW) div 1000]),
            NextRings2 =
                lists:sublist(NextRings, erlang:max(0, length(NextRings) - 1)),
            ?LOG_INFO("Transitions in plan: ~b", [length(NextRings2)]),
            lists:foldl(
                fun({Ring1, Ring2}, I) ->
                    compare_rings(Ring1, Ring2, I, Node),
                    I+1
                end,
                1,
                NextRings2),
            do_commit(Node)
    end.

compare_rings(Ring, NextRing, I, Node) ->
    Owners1 = rpc:call(Node, riak_core_ring, all_owners, [Ring]),
    FutureRing = rpc:call(Node, riak_core_ring, future_ring, [NextRing]),
    Owners2 = rpc:call(Node, riak_core_ring, all_owners, [FutureRing]),
    Owners3 = lists:zip(Owners1, Owners2),
    Reassigned =
        [{Idx, PrevOwner, NewOwner}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
                PrevOwner /= NewOwner],
    ?LOG_INFO(
        "Transition ~0p will result in ~b re-assignments",
        [I, length(Reassigned)]).

-spec do_commit(Node :: node()) -> ok.
do_commit(Node) ->
    ?LOG_INFO("planning cluster commit"),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            ?LOG_INFO("commit: plan changed"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            ?LOG_INFO("commit: ring not ready"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            do_commit(Node);
        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ?LOG_INFO("commit: nothing planned"),
            ok;
        ok ->
            ok
    end.

maybe_wait_for_changes(Node) ->
    Ring = get_ring(Node),
    Changes = riak_core_ring:pending_changes(Ring),
    Joining = riak_core_ring:members(Ring, [joining]),
    ?LOG_INFO("maybe_wait_for_changes, changes: ~0p joining: ~0p",
               [Changes, Joining]),
    if Changes =:= [] ->
            ok;
       Joining =/= [] ->
            ok;
       true ->
            ok = wait_until_no_pending_changes([Node])
    end.

%% @doc Have the `Node' leave the cluster
leave(Node) ->
    R = rpc:call(Node, riak_core, leave, []),
    ?LOG_INFO("[leave] ~0p: ~0p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]])).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, down, [[atom_to_list(OtherNode)]]).

%% @doc partition the `P1' from `P2' nodes
%%      note: the nodes remained connected to riak_test@local,
%%      which is how `heal/1' can still work.
partition(P1, P2) ->
    ?LOG_INFO("Split partition between nodes ~0p and ~0p", [P1, P2]),
    OldCookie = rpc:call(hd(P1), erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    ?LOG_INFO("Changing cookie ~0p to ~0p on nodes ~0p", [OldCookie, NewCookie, P1]),
    [reset_cookie(N, NewCookie, OldCookie) || N <- P1],
    [reset_cookie(N, OldCookie, OldCookie) || N <- P2],
    [[ok = node_disconnect(N, P2N) || N <- P1] || P2N <- P2],
    wait_until_partitioned(P1, P2),
    {NewCookie, OldCookie, P1, P2}.

node_disconnect(N, P2N) ->
    node_disconnect(N, P2N, 10).

node_disconnect(_N, _P2N, 0) ->
    {badrpc, nodedown};
node_disconnect(N, P2N, I) ->
    ?LOG_INFO("RPC to node ~0p to disconnect from node ~0p", [N, P2N]),
    case rpc:call(N, erlang, disconnect_node, [P2N]) of
        true ->
            ok;
        NotTrue ->
            ?LOG_INFO("Disconnect got response ~0p", [NotTrue]),
            case rpc:call(N, erlang, nodes, []) of
                {badrpc, nodedown} ->
                    timer:sleep(1000),
                    ?LOG_INFO(
                        "Node ~0p down for disconnect request.  Try again",
                        [N]),
                    node_disconnect(N, P2N, I - 1);
                VisibleNodes ->
                    ?LOG_INFO("Remote view of nodes ~0p", [VisibleNodes]),
                    ?LOG_INFO("... ok to proceed if ~0p not connected", [P2N]),
                    false = lists:member(P2N, VisibleNodes),
                    ok
            end
    end.

%% @doc heal the partition created by call to `partition/2'
%%      `OldCookie' is the original shared cookie
heal({NewCookie, OldCookie, P1, P2}) ->
    Cluster = P1 ++ P2,
    % set OldCookie on P1 Nodes
    lists:foreach(fun(N) -> reset_cookie(N, OldCookie, NewCookie) end, P1),
    wait_until_connected(Cluster),
    {_GN, []} = rpc:sbcast(Cluster, riak_core_node_watcher, broadcast),
    ok.

%% @doc heal the partition created by call to partition/2, but if some
%% node in P1 is down, just skip it, rather than failing. Returns {ok,
%% list(node())} where the list is those nodes down and therefore not
%% healed/reconnected.
heal_upnodes({NewCookie, OldCookie, P1, P2}) ->
    %% set OldCookie on UP P1 Nodes
    Res = lists:map(fun(N) -> {N, reset_cookie(N, OldCookie, NewCookie)} end, P1),
    UpForReconnect = [N || {N, true} <- Res],
    DownForReconnect = [N || {N, RPC} <- Res, RPC /= true],
    Cluster = UpForReconnect ++ P2,
    wait_until_connected(Cluster),
    {_GN, []} = rpc:sbcast(Cluster, riak_core_node_watcher, broadcast),
    {ok, DownForReconnect}.

%% @doc Spawn `Cmd' on the machine running the test harness
-spec spawn_cmd(Cmd :: rtt:cmd_exe(), Args :: rtt:cmd_args())
        -> Result :: rtt:cmd_token() | rtt:cmd_exec_err().
spawn_cmd(Cmd, Args) ->
    ?HARNESS:spawn_cmd(Cmd, Args).

%% @doc Wait for a command spawned by `spawn_cmd', returning
%%      the exit status and result
-spec spawned_result(CmdToken :: rtt:cmd_token()) ->
    Result :: {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_timeout_err().
spawned_result(CmdToken) ->
    ?HARNESS:spawned_result(CmdToken).

%% @doc Run `Cmd' on the machine running the test harness, returning
%%      the exit status and result
-spec cmd(Cmd :: rtt:cmd_exe(), Args :: rtt:cmd_args())
        -> Result :: {rtt:cmd_rc(), string()} | rtt:cmd_error().
cmd(Cmd, Args) ->
    case cmd(Cmd, Args, string) of
        {RC, string, Output} ->
            {RC, Output};
        Error ->
            Error
    end.

%% @doc Run `Cmd' on the machine running the test harness, returning
%%      the exit status, format, and output.
-spec cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Form :: rtt:cmd_out_format() )
        -> Result :: rtt:cmd_result() | rtt:cmd_error().
cmd(Cmd, Args, Form) ->
    ?HARNESS:cmd(Cmd, Args, Form).

%% @doc pretty much the same as os:cmd/1 but it will stream the output to logger.
%%      If you're running a long running command, it will dump the output
%%      once per second, as to not create the impression that nothing is happening.
-spec stream_cmd(iolist()) -> {integer(), string()}.
stream_cmd(Cmd) ->
    stream_cmd(Cmd, []).

%% @doc same as rt:stream_cmd/1, but with options, like open_port/2
-spec stream_cmd(iolist(), list()) -> {integer(), string()}.
stream_cmd(Cmd, Opts) ->
    CmdLine = erlang:binary_to_list(erlang:iolist_to_binary(Cmd)),
    Port = open_port(
        {spawn, CmdLine}, [stream, stderr_to_stdout, exit_status | Opts]),
    stream_cmd_loop(Port, "", "", os:timestamp()).

stream_cmd_loop(Port, Buffer, NewLineBuffer, Time={_MegaSecs, Secs, _MicroSecs}) ->
    receive
        {Port, {data, Data}} ->
            {_, Now, _} = os:timestamp(),
            NewNewLineBuffer = case Now > Secs of
                true ->
                    ?LOG_INFO(NewLineBuffer),
                    "";
                _ ->
                    NewLineBuffer
            end,
            case rt:str(Data, "\n") of
                true ->
                    ?LOG_INFO(NewNewLineBuffer),
                    Tokens = string:tokens(Data, "\n"),
                    [ ?LOG_INFO(Token) || Token <- Tokens ],
                    stream_cmd_loop(Port, Buffer ++ NewNewLineBuffer ++ Data, "", Time);
                _ ->
                    stream_cmd_loop(Port, Buffer, NewNewLineBuffer ++ Data, os:timestamp())
            end;
        {Port, {exit_status, Status}} ->
            catch port_close(Port),
            {Status, Buffer}
    after rt_config:get(rt_max_wait_time) ->
            {-1, Buffer}
    end.

%%%===================================================================
%%% Remote code management
%%%===================================================================

%% @doc Loads Modules on Nodes.
-spec load_modules_on_nodes(
    Modules :: rtt:modules(), Nodes :: rtt:nodes()) -> ok.
load_modules_on_nodes([], [_|_] = _Nodes) ->
    ok;
load_modules_on_nodes([Mod | Modules], [_|_] = Nodes) ->
    case code:get_object_code(Mod) of
        {Mod, Bin, File} ->
            case rpc:multicall(Nodes, code, load_binary, [Mod, File, Bin]) of
                {Results, []} ->
                    IsError = fun(Elem) -> Elem =/= {module, Mod} end,
                    case lists:filter(IsError, Results) of
                        [] ->
                            ?LOG_INFO(
                                "Loaded module '~s' on Nodes ~0p",
                                [Mod, Nodes]);
                        Errors ->
                            erlang:error(remote_load_failure, Errors)
                    end;
                {_Results, BadNodes} ->
                    erlang:error(bad_nodes, BadNodes)
            end;
        error ->
            erlang:error(object_code_not_found, [Mod])
    end,
    load_modules_on_nodes(Modules, Nodes).

%%%===================================================================
%%% Status / Wait Functions
%%%===================================================================

%% @doc Is the `Node' up according to net_adm:ping
-spec is_pingable(Node :: node()) -> boolean().
is_pingable(Node) ->
    net_adm:ping(Node) =:= pong.

%% @doc Reports whether the specified nodes, or the nodes connected via
%%      normal disterl connections to the single specified node, are
%%      running different Riak versions.
%%
%% Nodes that are not running are ignored.
%%
%% If any of the live nodes returns an error, the result is an error tuple
%% containing a list of received errors.
%% @end
-spec is_mixed_cluster(Nodes :: rtt:nodes() | node())
        -> boolean() | rtt:std_error().
is_mixed_cluster([Node1 | _] = Nodes) when erlang:is_atom(Node1) ->
    %% If the nodes are bad, we don't care what version they are
    {Returned, _BadNodes} = rpc:multicall(
        Nodes, init, script_id, [], rt_config:get(rt_max_wait_time)),
    IsError = fun
        ({badrpc, _}) ->
            true;
        ({error, _}) ->
            true;
        (_) ->
            false
    end,
    case lists:filter(IsError, Returned) of
        [] ->
            erlang:length(lists:usort(Returned)) > 1;
        Errors ->
            {error, Errors}
    end;
is_mixed_cluster(Node) when erlang:is_atom(Node) ->
    case rpc:call(Node, erlang, nodes, [visible]) of
        [] ->
            false;
        [_|_] = Nodes ->
            is_mixed_cluster(Nodes);
        {badrpc, _} = RpcErr ->
            {error, RpcErr};
        Error ->
            Error
    end;
is_mixed_cluster(Arg) ->
    erlang:error(badarg, [Arg]).

%% @private
-spec is_ready(Node :: node()) -> boolean().
is_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            lists:member(Node, riak_core_ring:ready_members(Ring));
        _ ->
            false
    end.

%% @private
-spec is_ring_ready(Node :: node()) -> boolean().
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            riak_core_ring:ring_ready(Ring);
        _ ->
            false
    end.

%% @doc Utility function used to construct test predicates.
%%
%% Retries the function `Fun' until it returns `true', or until the maximum
%% number of retries is reached. The retry limit is based on the configured
%% `rt_max_wait_time' and `rt_retry_delay' parameters in specified `riak_test'
%% config file.
wait_until(Fun) when erlang:is_function(Fun, 0) ->
    {Delay, Retry} = get_retry_settings(),
    wait_until(Fun, Retry, Delay).

%% @doc Utility function used to construct test predicates.
%%
%% Retries the function `Fun' until it returns `true', or until the maximum
%% number of retries is reached. The retry limit is based on the configured
%% `rt_max_wait_time' and `rt_retry_delay' parameters in specified `riak_test'
%% config file.
-ifdef(COVARIANT_SPECS).
-spec wait_until(Fun :: fun((term()) -> boolean()), Param :: term() )
        -> ok | {fail, Result :: term()}; (
    Node :: node() | rtt:nodes(), Fun :: fun((node()) -> boolean()) )
        -> ok | {fail, Result :: term()}.
-else.  % EDoc
-spec wait_until(node() | rtt:nodes() | fun((term()) -> boolean()), term() )
        -> ok | {fail, term()}.
-endif. % COVARIANT_SPECS
wait_until(Fun, Param) when erlang:is_function(Fun, 1) ->
    {Delay, Retry} = get_retry_settings(),
    wait_until(Fun, Param, Retry, Delay);
wait_until(Node, Fun)
        when erlang:is_atom(Node) andalso erlang:is_function(Fun, 1) ->
    wait_until(Fun, Node);
wait_until([Node1 | _] = Nodes, Fun)
        when erlang:is_atom(Node1) andalso erlang:is_function(Fun, 1) ->
    {Delay, Retry} = get_retry_settings(),
    lists:foreach(
        fun(Node) ->
            ?assertEqual(ok, wait_until(Fun, Node, Retry, Delay))
        end, Nodes);
wait_until(Arg1, Arg2) ->
    erlang:error(badarg, [Arg1, Arg2]).

%% @doc Try `Fun' as many as `Retry + 1` times until it returns `true'.
%%
%% If the allowed number of retries expires, returns `{fail, LastResult}'.
-spec wait_until(
    Fun :: fun(() -> boolean()),
    Retry :: non_neg_integer(),
    Delay :: rtt:millisecs() )
        -> ok | {fail, term()}.
wait_until(Fun, Retry, Delay) when erlang:is_function(Fun, 0)
        andalso erlang:is_integer(Retry) andalso Retry >= 0
        andalso erlang:is_integer(Delay) andalso Delay > 0 ->
    case Fun() of
        true ->
            ok;
        _Res when Retry > 0 ->
            timer:sleep(Delay),
            wait_until(Fun, (Retry - 1), Delay);
        Res ->
            {fail, Res}
    end;
wait_until(Arg1, Arg2, Arg3) ->
    erlang:error(badarg, [Arg1, Arg2, Arg3]).

%% @doc Try `Fun(Param)' as many as `Retry + 1` times until it returns `true'.
%%
%% If the allowed number of retries expires, returns `{fail, LastResult}'.
-spec wait_until(
    Fun :: fun((term()) -> boolean()),
    Param :: term(),
    Retry :: non_neg_integer(),
    Delay :: rtt:millisecs() )
        -> ok | {fail, term()}.
wait_until(Fun, Param, Retry, Delay) when erlang:is_function(Fun, 1)
        andalso erlang:is_integer(Retry) andalso Retry >= 0
        andalso erlang:is_integer(Delay) andalso Delay > 0 ->
    case Fun(Param) of
        true ->
            ok;
        _Res when Retry > 0 ->
            timer:sleep(Delay),
            wait_until(Fun, Param, (Retry - 1), Delay);
        Res ->
            {fail, Res}
    end;
wait_until(Arg1, Arg2, Arg3, Arg4) ->
    erlang:error(badarg, [Arg1, Arg2, Arg3, Arg4]).

-spec get_retry_settings() -> {rtt:millisecs(), non_neg_integer()}.
get_retry_settings() ->
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = rt_config:get(rt_retry_delay),
    case (MaxTime div Delay) of
        Retry when Retry > 0 ->
            {Delay, (Retry - 1)};
        _ ->
            {Delay, 0}
    end.

%% @doc Wait until the specified node is considered ready by `riak_core'.
%%      As of Riak 1.0, a node is ready if it is in the `valid' or `leaving'
%%      states. A ready node is guaranteed to have current preflist/ownership
%%      information.
-spec wait_until_ready(Node :: node()) -> ok.
wait_until_ready(Node) when erlang:is_atom(Node) ->
    ?LOG_INFO("Wait until ~0p ready", [Node]),
    ?assertEqual(ok, wait_until(fun is_ready/1, Node)).

%% @doc Wait until status can be read from riak_kv_console
-spec wait_until_status_ready(Node :: node()) -> ok.
wait_until_status_ready(Node) ->
    ?LOG_INFO("Wait until status ready in ~0p", [Node]),
    Fun = fun(N) ->
        case rpc:call(N, riak_kv_console, status, [[]]) of
            ok ->
                true;
            Res ->
                Res
        end
    end,
    ?assertEqual(ok, wait_until(Fun, Node)).

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes(Nodes :: rtt:nodes()) -> ok.
wait_until_no_pending_changes(NodesIn) ->
    ?LOG_INFO("Wait until no pending changes on ~0p", [NodesIn]),
    Fun = fun(Nodes) ->
        case no_pending_changes(Nodes) of
            true ->
                ?LOG_INFO("No pending changes - sleep then confirm"),
                % Some times there may be no pending changes, just because
                % changes haven't triggered yet
                timer:sleep(2000),
                no_pending_changes(Nodes);
            False ->
                False
        end
    end,
    ?assertEqual(ok, wait_until(Fun, NodesIn)).

-spec no_pending_changes(Nodes :: rtt:nodes()) -> boolean().
no_pending_changes(Nodes) ->
    rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
    {Rings, BadNodes} =
        rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
    Changes =
        [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
    case BadNodes =:= [] andalso
            length(Changes) =:= length(Nodes) andalso
            lists:all(fun(T) -> T end, Changes) of
        true ->
            true;
        false ->
            NodesWithChanges =
                [Node ||
                    {Node, false} <- lists:zip(Nodes -- BadNodes, Changes)],
            ?LOG_INFO(
                "Changes not yet complete, or bad nodes. "
                "BadNodes=~0p, Nodes with Pending Changes=~0p",
                [BadNodes, NodesWithChanges]),
            false
    end.

%% @doc Waits until no transfers are in-flight or pending, checked by
%% riak_core_status:transfers().
-spec wait_until_transfers_complete(Nodes :: rtt:nodes()) -> ok.
wait_until_transfers_complete([Node0 | _]) ->
    ?LOG_INFO("Wait until transfers complete ~0p", [Node0]),
    Fun = fun(Node) ->
        {DownNodes, Transfers} = rpc:call(Node, riak_core_status, transfers, []),
        ?LOG_INFO("DownNodes: ~0p Transfers: ~0p", [DownNodes, Transfers]),
        DownNodes =:= [] andalso Transfers =:= []
    end,
    ?assertEqual(ok, wait_until(Fun, Node0)).

%% @doc Waits until hinted handoffs from `Node0' are complete
-spec wait_until_node_handoffs_complete(Node :: node()) -> ok.
wait_until_node_handoffs_complete(NodeIn) ->
    ?LOG_INFO("Wait until Node's transfers complete ~0p", [NodeIn]),
    Fun = fun(Node) ->
        Handoffs = rpc:call(Node,
            riak_core_handoff_manager, status, [{direction, outbound}]),
        ?LOG_INFO("Handoffs: ~0p", [Handoffs]),
        Handoffs =:= []
    end,
    ?assertEqual(ok, wait_until(Fun, NodeIn)).

-spec wait_for_service(
    Node :: node(), Services :: rtt:services() | rtt:service()) -> ok.
wait_for_service(NodeIn, [] = _Services) ->
    Fun = fun(Node) ->
        case rpc:call(Node, riak_core_node_watcher, services, [Node]) of
            {badrpc, _} = Error ->
                {error, Error};
            CurrServices when erlang:is_list(CurrServices) ->
                ?LOG_INFO(
                    "Waiting for any services on node ~0p. Current services: ~0p",
                    [Node, CurrServices]),
                true;
            Res ->
                Res
        end
    end,
    ?assertEqual(ok, wait_until(Fun, NodeIn));
wait_for_service(NodeIn, [_|_] = Services) ->
    Fun = fun(Node) ->
        case rpc:call(Node, riak_core_node_watcher, services, [Node]) of
            {badrpc, _} = Error ->
                {error, Error};
            CurrServices when erlang:is_list(CurrServices) ->
                ?LOG_INFO(
                    "Waiting for services ~0p: on node ~0p. Current services: ~0p",
                    [Services, Node, CurrServices]),
                lists:all(
                    fun(Service) ->
                        lists:member(Service, CurrServices)
                    end, Services);
            Res ->
                Res
        end
    end,
    ?assertEqual(ok, wait_until(Fun, NodeIn));
wait_for_service(Node, Service) ->
    wait_for_service(Node, [Service]).

-spec wait_for_cluster_service(
    Nodes :: rtt:nodes(), Service :: rtt:service()) -> ok.
wait_for_cluster_service(Nodes, Service) ->
    ?LOG_INFO("Wait for cluster service ~0p in ~0p", [Service, Nodes]),
    Fun = fun(Node) ->
        case rpc:call(Node, riak_core_node_watcher, nodes, [Service]) of
            {badrpc, _} = Error ->
                {error, Error};
            UpNodes when erlang:is_list(UpNodes) ->
                (Nodes -- UpNodes) =:= [];
            Res ->
                Res
        end
    end,
    lists:foreach(
        fun(N) ->
            ?assertEqual(ok, wait_until(Fun, N))
        end, Nodes).

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
-spec wait_until_nodes_ready(Nodes :: rtt:nodes()) -> ok.
wait_until_nodes_ready(Nodes) ->
    ?LOG_INFO("Wait until nodes are ready : ~0p", [Nodes]),
    lists:foreach(
        fun(Node) ->
            ?assertEqual(ok, wait_until(fun is_ready/1, Node))
        end, Nodes).

%% @doc Wait until all nodes in the list `Nodes' believe each other to be
%%      members of the cluster.
wait_until_all_members(Nodes) ->
    wait_until_all_members(Nodes, Nodes).

%% @doc Wait until all nodes in the list `Nodes' believes all nodes in the
%%      list `Members' are members of the cluster.
wait_until_all_members(Nodes, ExpectedMembers) ->
    ?LOG_INFO("Wait until all members ~0p ~0p", [Nodes, ExpectedMembers]),
    ExpectedSet = ordsets:from_list(ExpectedMembers),
    Fun = fun(Node) ->
        case members_according_to(Node) of
            [_|_] = ReportedMembers ->
                ReportedSet = ordsets:from_list(ReportedMembers),
                ordsets:is_subset(ExpectedSet, ReportedSet);
            _ ->
                false
        end
    end,
    lists:foreach(
        fun(Node) ->
            ?assertMatch(ok, wait_until(Fun, Node))
        end, Nodes).

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
    ?LOG_INFO("Wait until ring converged on ~0p", [Nodes]),
    lists:foreach(
        fun(Node) ->
            ?assertMatch(ok, wait_until(fun is_ring_ready/1, Node))
        end, Nodes).

wait_until_legacy_ringready(Node) ->
    ?LOG_INFO("Wait until legacy ring ready on ~0p", [Node]),
    rt:wait_until(Node,
                  fun(_) ->
                          case rpc:call(Node, riak_kv_status, ringready, []) of
                              {ok, _Nodes} ->
                                  true;
                              Res ->
                                  Res
                          end
                  end).

%% @doc wait until each node in Nodes is disterl connected to each.
wait_until_connected(Nodes) ->
    ?LOG_INFO("Wait until connected ~0p", [Nodes]),
    NodeSet = sets:from_list(Nodes),
    F = fun(Node) ->
        Connected = rpc:call(Node, erlang, nodes, []),
        sets:is_subset(NodeSet, sets:from_list(([Node] ++ Connected) -- [node()]))
    end,
    [?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
    ok.

%% @doc Wait until the specified node is pingable
-spec wait_until_pingable(Node :: node()) -> ok.
wait_until_pingable(Node) ->
    ?LOG_INFO("Wait until ~0p is pingable", [Node]),
    Fun = fun(N) -> net_adm:ping(N) =:= pong end,
    ?assertEqual(ok, wait_until(Fun, Node)).

%% @doc Wait until the specified node is no longer pingable
-spec wait_until_unpingable(Node :: node()) -> ok.
wait_until_unpingable(Node) ->
    ?LOG_INFO("Wait until ~0p is not pingable", [Node]),
    _OSPidToKill = rpc:call(Node, os, getpid, []),
    Fun = fun(N) -> net_adm:ping(N) =:= pang end,
    %% riak stop will kill -9 after 5 minutes, so wait at least 6 minutes.
    MaxTO = erlang:max(360000, rt_config:get(rt_max_wait_time)),
    Delay = rt_config:get(rt_retry_delay),
    Retry = MaxTO div Delay,
    case wait_until(Fun, Node, Retry, Delay) of
        ok ->
            ok;
        _ ->
            ?LOG_ERROR("Timed out waiting for node ~0p to shut down", [Node]),
            erlang:error(node_shutdown_timed_out)
    end.

%% Waits until a certain registered name pops up on the remote node.
-spec wait_until_registered(Node :: node(), Name :: atom()) -> ok.
wait_until_registered(Node, Name) ->
    ?LOG_INFO("Wait until ~0p is up on ~0p", [Name, Node]),
    Fun = fun() ->
        case rpc:call(Node, erlang, registered, []) of
            [_|_] = NameList ->
                lists:member(Name, NameList);
            _ ->
                false
        end
    end,
    case wait_until(Fun) of
        ok ->
            ok;
        _ ->
            ?LOG_INFO(
                "The server with the name ~0p on ~0p is not coming up.",
                [Name, Node]),
            erlang:error(registered_name_timed_out)
    end.

%% Waits until the cluster actually detects that it is partitioned.
%% TODO: Need a version of wait_until() that takes a fixed time barrier -
%% in failure scenarios, this could take approximately forever :(
-spec wait_until_partitioned(P1 :: rtt:nodes(), P2 :: rtt:nodes()) -> ok.
wait_until_partitioned(P1, P2) ->
    ?LOG_INFO("Waiting until partition acknowledged: ~0p ~0p", [P1, P2]),
    wait_until_nodes_partitioned(P1, P2),
    wait_until_nodes_partitioned(P2, P1),
    %% Confirm that the partitioned clusters are still/again intraconnected.
    %% They've possibly all had their cookie changed, so use wait_until.
    %% We have to have already talked to each of the nodes to get here, but
    %% still need to ensure they're talking to each other.
    wait_until_nodes_connected(P1),
    wait_until_nodes_connected(P2).

-spec wait_until_nodes_connected(Nodes :: rtt:nodes()) -> ok.
wait_until_nodes_connected(Nodes) ->
    lists:foreach(
        fun(Node) ->
            wait_until_node_connected(Node, Nodes)
        end, Nodes).

-spec wait_until_node_connected(Node :: node(), Nodes :: rtt:nodes()) -> ok.
wait_until_node_connected(Node, Nodes) ->
    ?LOG_INFO("Checking that ~0p is connected to ~0p", [Node, Nodes]),
    ?assertMatch(ok,
        wait_until(fun() -> is_node_connected(Node, Nodes) end)).

-spec wait_until_nodes_partitioned(
    Nodes :: rtt:nodes(), FromNodes :: rtt:nodes() ) -> ok.
wait_until_nodes_partitioned(Nodes, FromNodes) ->
    lists:foreach(
        fun(Node) ->
            wait_until_node_partitioned(Node, FromNodes)
        end, Nodes).

-spec wait_until_node_partitioned(Node :: node(), Nodes :: rtt:nodes()) -> ok.
wait_until_node_partitioned(Node, Nodes) ->
    ?LOG_INFO("Waiting for ~0p to be partitioned from ~0p", [Node, Nodes]),
    ?assertMatch(ok,
        wait_until(fun() -> is_node_partitioned(Node, Nodes) end)).

-spec is_node_connected(Node :: node(), Nodes :: rtt:nodes()) -> boolean().
is_node_connected(Node, Nodes) ->
    TestFun = fun(Peers, Connected) ->
        sets:is_subset(
            sets:from_list(Peers), sets:from_list(Connected))
        end,
    test_node_connected(TestFun, Node, Nodes).

-spec is_node_partitioned(Node :: node(), Nodes :: rtt:nodes() ) -> boolean().
is_node_partitioned(Node, Nodes) ->
    TestFun = fun(Peers, Connected) ->
        sets:is_empty(sets:intersection(
            sets:from_list(Peers), sets:from_list(Connected)))
    end,
    test_node_connected(TestFun, Node, Nodes).

-spec test_node_connected(
    TestFun :: fun((
        Peers :: rtt:nodes(), Connected :: rtt:nodes()) -> boolean()),
    Node :: node(), Nodes :: rtt:nodes() )
        -> boolean().
test_node_connected(TestFun, Node, Nodes) ->
    %% Callers rely on Node being in the returned list, so don't use
    %% erlang:nodes/0
    case rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]) of
        [] ->
            true;
        [_|_] = Connected ->
            ?LOG_INFO(
                "testing ~0p against ~0p connected to ~0p",
                [Nodes, Connected, Node]),
            TestFun(Nodes, Connected);
        {badrpc, _} = RpcErr ->
            ?LOG_INFO(
                "Node ~0p not responding to rpc call, try again ...",
                [Node]),
            {error, RpcErr};
        Error ->
            Error
    end.

% when you just can't wait
brutal_kill(Node) ->
    rt_cover:maybe_stop_on_node(Node),
    ?LOG_INFO("Killing node ~0p", [Node]),
    OSPidToKill = rpc:call(Node, os, getpid, []),
    %% try a normal kill first, but set a timer to
    %% kill -9 after 5 seconds just in case
    rpc:cast(Node, timer, apply_after,
             [5000, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
    rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
    ok.

capability(Node, all) ->
    rpc:call(Node, riak_core_capability, all, []);
capability(Node, Capability) ->
    rpc:call(Node, riak_core_capability, get, [Capability]).

capability(Node, Capability, Default) ->
    rpc:call(Node, riak_core_capability, get, [Capability, Default]).

wait_until_capability(Node, Capability, Value) ->
    rt:wait_until(Node,
                  fun(_) ->
                      Cap = capability(Node, Capability),
                      ?LOG_INFO("Capability on node ~0p is ~0p",[Node, Cap]),
                      cap_equal(Value, Cap)
                  end).

wait_until_capability(Node, Capability, Value, Default) ->
    rt:wait_until(Node,
                  fun(_) ->
                          Cap = capability(Node, Capability, Default),
                          ?LOG_INFO("Capability on node ~0p is ~0p",[Node, Cap]),
                          cap_equal(Value, Cap)
                  end).

-spec wait_until_capability_contains(node(), atom() | {atom(), atom()}, list()) -> ok.
wait_until_capability_contains(Node, Capability, Value) ->
    rt:wait_until(Node,
                fun(_) ->
                    Cap = capability(Node, Capability),
                    ?LOG_INFO("Capability on node ~0p is ~0p",[Node, Cap]),
                    cap_subset(Value, Cap)
                end).

cap_equal(Val, Cap) when is_list(Cap) ->
    lists:sort(Cap) == lists:sort(Val);
cap_equal(Val, Cap) ->
    Val == Cap.

cap_subset(Val, Cap) when is_list(Cap) ->
    sets:is_subset(sets:from_list(Val), sets:from_list(Cap)).

wait_until_owners_according_to(Node, Nodes) ->
    SortedNodes = lists:usort(Nodes),
    F = fun(N) ->
        owners_according_to(N) =:= SortedNodes
    end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

wait_until_nodes_agree_about_ownership(Nodes) ->
    ?LOG_INFO("Wait until nodes agree about ownership ~0p", [Nodes]),
    Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    ?assert(lists:all(fun(X) -> ok =:= X end, Results)).

%% AAE support
wait_until_aae_trees_built(Nodes) ->
    ?LOG_INFO("Wait until AAE builds all partition trees across ~0p", [Nodes]),
    BuiltFun = fun() -> lists:foldl(aae_tree_built_fun(), true, Nodes) end,
    ?assertEqual(ok, wait_until(BuiltFun)),
    ok.

aae_tree_built_fun() ->
    fun(Node, _AllBuilt = true) ->
            case get_aae_tree_info(Node) of
                {ok, TreeInfos} ->
                    case all_trees_have_build_times(TreeInfos) of
                        true ->
                            Partitions = [I || {I, _} <- TreeInfos],
                            all_aae_trees_built(Node, Partitions);
                        false ->
                            some_trees_not_built
                    end;
                Err ->
                    Err
            end;
       (_Node, Err) ->
            Err
    end.

% It is unlikely but possible to get a tree built time from compute_tree_info
% but an attempt to use the tree returns not_built. This is because the build
% process has finished, but the lock on the tree won't be released until it
% dies and the manager detects it. Yes, this is super freaking paranoid.
all_aae_trees_built(Node, Partitions) ->
    %% Notice that the process locking is spawned by the
    %% pmap. That's important! as it should die eventually
    %% so the lock is released and the test can lock the tree.
    IndexBuilts = rt:pmap(index_built_fun(Node), Partitions),
    BadOnes = [R || R <- IndexBuilts, R /= true],
    case BadOnes of
        [] ->
            true;
        _ ->
            BadOnes
    end.

get_aae_tree_info(Node) ->
    case rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []) of
        {badrpc, _} ->
            {error, {badrpc, Node}};
        Info  ->
            ?LOG_DEBUG("Entropy table on node ~0p : ~0p", [Node, Info]),
            {ok, Info}
    end.

all_trees_have_build_times(Info) ->
    not lists:keymember(undefined, 2, Info).

index_built_fun(Node) ->
    fun(Idx) ->
            case rpc:call(Node, riak_kv_vnode,
                                     hashtree_pid, [Idx]) of
                {ok, TreePid} ->
                    case rpc:call(Node, riak_kv_index_hashtree,
                                  get_lock, [TreePid, for_riak_test]) of
                        {badrpc, _} ->
                            {error, {badrpc, Node}};
                        TreeLocked when TreeLocked == ok;
                                        TreeLocked == already_locked ->
                            true;
                        Err ->
                            % Either not_built or some unhandled result,
                            % in which case update this case please!
                            {error, {index_not_built, Node, Idx, Err}}
                    end;
                {error, _}=Err ->
                    Err;
                {badrpc, _} ->
                    {error, {badrpc, Node}}
            end
    end.

%%%===================================================================
%%% Ring Functions
%%%===================================================================

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
check_singleton_node(Node) ->
    ?LOG_INFO("Check ~0p is a singleton", [Node]),
    ?assertEqual([Node], owners_according_to(Node)).

% @doc Get list of partitions owned by node (primary).
partitions_for_node(Node) ->
    Ring = get_ring(Node),
    [Idx || {Idx, Owner} <- riak_core_ring:all_owners(Ring), Owner == Node].

%% @doc Get the raw ring for `Node'.
-spec get_ring(Node :: node()) -> riak_core_ring:riak_core_ring().
get_ring(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Ring;
        {error, Reason} ->
            erlang:error(Reason);
        Unexpected ->
            erlang:error(Unexpected)
    end.

%% @doc Get the preflist for a Node, Bucket and Key.
get_preflist(Node, Bucket, Key) ->
    get_preflist(Node, Bucket, Key, 3).

get_preflist(Node, Bucket, Key, NVal) ->
    Chash = rpc:call(Node, riak_core_util, chash_key, [{Bucket, Key}]),
    UpNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
    PL = rpc:call(Node, riak_core_apl, get_apl_ann, [Chash, NVal, UpNodes]),
    PL.

assert_nodes_agree_about_ownership(Nodes) ->
    ?assertEqual(ok, wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, wait_until_all_members(Nodes)),
    [ ?assertEqual({Node, Nodes}, {Node, owners_according_to(Node)}) || Node <- Nodes].

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
-spec owners_according_to(Node :: node()) -> rtt:nodes() | rtt:std_error().
owners_according_to(Node) ->
    lists:usort([
        Owner || {_Idx, Owner} <- riak_core_ring:all_owners(get_ring(Node))]).

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
-spec members_according_to(Node :: node()) -> rtt:nodes() | rtt:std_error().
members_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            %% riak_core_ring:all_members/1 has no error conditions
            riak_core_ring:all_members(Ring);
        {badrpc, _} = RpcErr ->
            {error, RpcErr};
        {error, _} = Error ->
            Error
    end.

%% @doc Return an appropriate ringsize for the node count passed
%%      in. 24 is the number of cores on the bigger intel machines, but this
%%      may be too large for the single-chip machines.
nearest_ringsize(Count) ->
    nearest_ringsize(Count * 24, 2).

nearest_ringsize(Count, Power) ->
    case Count < trunc(Power * 0.9) of
        true ->
            Power;
        false ->
            nearest_ringsize(Count, Power * 2)
    end.

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
status_of_according_to(Member, Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Status = riak_core_ring:member_status(Ring, Member),
            Status;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Claimant = riak_core_ring:claimant(Ring),
            Claimant;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%%%===================================================================
%%% Cluster Utility Functions
%%%===================================================================

%% @doc Safely construct a new cluster and return a list of the deployed nodes
%% @todo Add -spec and update doc to reflect multi-version changes
build_cluster(Versions) when is_list(Versions) ->
    build_cluster(length(Versions), Versions, default);
build_cluster(NumNodes) ->
    build_cluster(NumNodes, default).

%% @doc Safely construct a `NumNodes' size cluster using
%%      `InitialConfig'. Return a list of the deployed nodes.
build_cluster(NumNodes, InitialConfig) ->
    build_cluster(NumNodes, [], InitialConfig).

build_cluster(NumNodes, Versions, InitialConfig) ->
    %% Deploy a set of new nodes
    Nodes =
        case Versions of
            [] ->
                deploy_nodes(NumNodes, InitialConfig);
            _ ->
                deploy_nodes(Versions)
        end,

    join_cluster(Nodes),
    ?LOG_INFO("Cluster built: ~0p", [Nodes]),
    Nodes.

-spec join_cluster(Nodes :: rtt:nodes() ) -> ok.
join_cluster([Node1 | OtherNodes] = Nodes) ->
    %% Ensure each node owns 100% of it's own ring
    lists:foreach(
        fun(Node) ->
            ?assertEqual([Node], owners_according_to(Node))
        end, Nodes),

    %% Join nodes
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit
            ok;
        _ ->
            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            lists:foreach(
                fun(Node) ->
                    staged_join(Node, Node1)
                end, OtherNodes),
            plan_and_commit(Node1),
            try_nodes_ready(Nodes)
    end,

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),

    %% Ensure each node owns a portion of the ring
    wait_until_nodes_agree_about_ownership(Nodes),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)).

%% @doc What are we talking to?
-spec product(node()) -> rtt:product() | rtt:std_error().
product(Node) ->
    case rpc:call(Node, application, which_applications, []) of
        [] ->
            unknown;
        [_|_] = Applications ->
            case proplists:is_defined(riak_cs, Applications) of
                true ->
                    riak_cs;
                _ ->
                    case proplists:is_defined(riak_kv, Applications) of
                        true ->
                            riak;
                        _ ->
                            unknown
                    end
            end;
        {badrpc, _} = RpcErr ->
            {error, RpcErr}
    end.

try_nodes_ready(Nodes) ->
    try_nodes_ready(Nodes, 10, 500).

try_nodes_ready([Node1 | _Nodes], Tries, _RetryDelay) when Tries < 1 ->
    ?LOG_INFO("Nodes not ready after initial plan/commit, retrying"),
    plan_and_commit(Node1);
try_nodes_ready(Nodes, Tries, RetryDelay) ->
    case lists:all(fun is_ready/1, Nodes) of
        true ->
            ok;
        _ ->
            timer:sleep(RetryDelay),
            try_nodes_ready(Nodes, (Tries - 1), RetryDelay)
    end.

%% @doc Stop nodes and wipe out their data directories
-spec clean_cluster(Nodes :: rtt:nodes()) -> ok | rtt:std_error().
clean_cluster([_|_] = Nodes) ->
    lists:foreach(fun stop_and_wait/1, Nodes),
    clean_data_dir(Nodes, "").

-spec clean_data_dir(Nodes :: rtt:nodes() | node(), SubDir :: string())
        -> ok | rtt:std_error().
clean_data_dir([_|_] = Nodes, SubDir) ->
    ?HARNESS:clean_data_dir(Nodes, SubDir);
clean_data_dir(Node, SubDir) when erlang:is_atom(Node) ->
    clean_data_dir([Node], SubDir).

restore_data_dir(Nodes, BackendFldr, BackupFldr) when not is_list(Nodes) ->
    restore_data_dir([Nodes], BackendFldr, BackupFldr);
restore_data_dir(Nodes, BackendFldr, BackupFldr) ->
    ?HARNESS:restore_data_dir(Nodes, BackendFldr, BackupFldr).

%% @doc Shutdown every node, this is for after a test run is complete.
-spec teardown() -> ok.
teardown() ->
    ?HARNESS:teardown().

%% @doc Return all configured versions.
-spec versions() -> nonempty_list(rtt:vsn_tag()).
versions() ->
    ?HARNESS:versions().

%%%===================================================================
%%% Basic Read/Write Functions
%%%===================================================================

systest_delete(Node, Size) ->
    systest_delete(Node, Size, 2).

systest_delete(Node, Size, W) ->
    systest_delete(Node, 1, Size, <<"systest">>, W).

%% @doc Delete `(End-Start)+1' objects on `Node'. Keys deleted will be
%% `Start', `Start+1' ... `End', each key being encoded as a 32-bit binary
%% (`<<Key:32/integer>>').
%%
%% The return value of this function is a list of errors
%% encountered. If all deletes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where `N' is the unencoded key of the object that failed to store.
systest_delete(Node, Start, End, Bucket, W) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Key = <<N:32/integer>>,
                try riak_client:delete(Bucket, Key, W, C) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

systest_verify_delete(Node, Size) ->
    systest_verify_delete(Node, Size, 2).

systest_verify_delete(Node, Size, R) ->
    systest_verify_delete(Node, 1, Size, <<"systest">>, R).

%% @doc Read a series of keys on `Node' and verify that the objects
%% do not exist. This could, for instance, be used as a followup to
%% `systest_delete' to ensure that the objects were actually deleted.
systest_verify_delete(Node, Start, End, Bucket, R) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Key = <<N:32/integer>>,
                try riak_client:get(Bucket, Key, R, C) of
                    {error, notfound} ->
                        [];
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

systest_write(Node, Size) ->
    systest_write(Node, Size, 2).

systest_write(Node, Size, W) ->
    systest_write(Node, 1, Size, <<"systest">>, W).

systest_write(Node, Start, End, Bucket, W) ->
    systest_write(Node, Start, End, Bucket, W, <<>>).

%% @doc Write (End-Start)+1 objects to Node. Objects keys will be
%% `Start', `Start+1' ... `End', each encoded as a 32-bit binary
%% (`<<Key:32/integer>>'). Object values are the same as their keys.
%%
%% The return value of this function is a list of errors
%% encountered. If all writes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where N is the unencoded key of the object that failed to store.
systest_write(Node, Start, End, Bucket, W, CommonValBin)
  when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Obj = riak_object:new(Bucket, <<N:32/integer>>,
                                      <<N:32/integer, CommonValBin/binary>>),
                try riak_client:put(Obj, W, C) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

systest_read(Node, Size) ->
    systest_read(Node, Size, 2).

systest_read(Node, Size, R) ->
    systest_read(Node, 1, Size, <<"systest">>, R).

systest_read(Node, Start, End, Bucket, R) ->
    systest_read(Node, Start, End, Bucket, R, <<>>).

systest_read(Node, Start, End, Bucket, R, CommonValBin)
  when is_binary(CommonValBin) ->
    systest_read(Node, Start, End, Bucket, R, CommonValBin, false).

%% Read and verify the values of objects written with
%% `systest_write'. The `SquashSiblings' parameter exists to
%% optionally allow handling of siblings whose value and metadata are
%% identical except for the dot. This goal is to facilitate testing
%% with DVV enabled because siblings can be created internally by Riak
%% in cases where testing with DVV disabled would not. Such cases
%% include writes that happen during handoff when a vnode forwards
%% writes, but also performs them locally or when a put coordinator
%% fails to send an acknowledgment within the timeout window and
%% another put request is issued.
systest_read(Node, Start, End, Bucket, R, CommonValBin, SquashSiblings)
  when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    lists:foldl(systest_read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings),
                [],
                lists:seq(Start, End)).

systest_read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings) ->
    fun(N, Acc) ->
            GetRes = riak_client:get(Bucket, <<N:32/integer>>, R, C),
            Val = object_value(GetRes, SquashSiblings),
            update_acc(value_matches(Val, N, CommonValBin), Val, N, Acc)
    end.

object_value({error, _}=Error, _) ->
    Error;
object_value({ok, Obj}, SquashSiblings) ->
    object_value(riak_object:value_count(Obj), Obj, SquashSiblings).

object_value(1, Obj, _SquashSiblings) ->
    riak_object:get_value(Obj);
object_value(_ValueCount, Obj, false) ->
    riak_object:get_value(Obj);
object_value(_ValueCount, Obj, true) ->
    ?LOG_DEBUG("Siblings detected for ~0p:~0p~n~p",
        [riak_object:bucket(Obj), riak_object:key(Obj), Obj]),
    Contents = riak_object:get_contents(Obj),
    case lists:foldl(fun sibling_compare/2, {true, undefined}, Contents) of
        {true, {_, _, _, Value}} ->
            ?LOG_DEBUG("Siblings determined to be a single value"),
            Value;
        {false, _} ->
            {error, siblings}
    end.

sibling_compare({MetaData, Value}, {true, undefined}) ->
    Dot = case dict:find(<<"dot">>, MetaData) of
              {ok, DotVal} ->
                  DotVal;
              error ->
                  {error, no_dot}
          end,
    VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
    LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
    {true, {element(2, Dot), VTag, LastMod, Value}};
sibling_compare(_, {false, _}=InvalidMatch) ->
    InvalidMatch;
sibling_compare({MetaData, Value}, {true, PreviousElements}) ->
    Dot = case dict:find(<<"dot">>, MetaData) of
              {ok, DotVal} ->
                  DotVal;
              error ->
                  {error, no_dot}
          end,
    VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
    LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
    ComparisonElements = {element(2, Dot), VTag, LastMod, Value},
    {ComparisonElements =:= PreviousElements, ComparisonElements}.

value_matches(<<N:32/integer, CommonValBin/binary>>, N, CommonValBin) ->
    true;
value_matches(_WrongVal, _N, _CommonValBin) ->
    false.

update_acc(true, _, _, Acc) ->
    Acc;
update_acc(false, {error, _}=Val, N, Acc) ->
    [{N, Val} | Acc];
update_acc(false, Val, N, Acc) ->
    [{N, {wrong_val, Val}} | Acc].

% @doc Reads a single replica of a value. This issues a get command directly
% to the vnode handling the Nth primary partition of the object's preflist.
get_replica(Node, Bucket, Key, I, N) ->
    BKey = {Bucket, Key},
    Chash = rpc:call(Node, riak_core_util, chash_key, [BKey]),
    Pl = rpc:call(Node, riak_core_apl, get_primary_apl, [Chash, N, riak_kv]),
    {{Partition, PNode}, primary} = lists:nth(I, Pl),
    Ref = Reqid = make_ref(),
    Sender = {raw, Ref, self()},
    rpc:call(PNode, riak_kv_vnode, get,
             [{Partition, PNode}, BKey, Ref, Sender]),
    receive
        {Ref, {r, Result, _, Reqid}} ->
            Result;
        {Ref, Reply} ->
            Reply
    after
        60000 ->
            ?LOG_ERROR("Replica ~0p get for ~0p/~0p timed out",
                        [I, Bucket, Key]),
            ?assert(false)
    end.

%%%===================================================================

%% @doc PBC-based version of {@link systest_write/1}
pbc_systest_write(Node, Size) ->
    pbc_systest_write(Node, Size, 2).

pbc_systest_write(Node, Size, W) ->
    pbc_systest_write(Node, 1, Size, <<"systest">>, W).

pbc_systest_write(Node, Start, End, Bucket, W) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                Obj = riakc_obj:new(Bucket, <<N:32/integer>>, <<N:32/integer>>),
                try riakc_pb_socket:put(Pid, Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

pbc_systest_read(Node, Size) ->
    pbc_systest_read(Node, Size, 2).

pbc_systest_read(Node, Size, R) ->
    pbc_systest_read(Node, 1, Size, <<"systest">>, R).

pbc_systest_read(Node, Start, End, Bucket, R) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                case riakc_pb_socket:get(Pid, Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riakc_obj:get_value(Obj) of
                            <<N:32/integer>> ->
                                Acc;
                            WrongVal ->
                                [{N, {wrong_val, WrongVal}} | Acc]
                        end;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

%%%===================================================================
%%% PBC & HTTPC Functions
%%%===================================================================

%% @doc get me a protobuf client process and hold the mayo!
-spec pbc(node()) -> pid().
pbc(Node) ->
    pbc(Node, [{auto_reconnect, true}]).

-spec pbc(node(), proplists:proplist()) -> pid().
pbc(Node, Options) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, {IP, Port}} = get_pb_conn_info(Node),
    {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
    Pid.

%% @doc does a read via the erlang protobuf client
-spec pbc_read(pid(), binary()|{binary(), binary()}, binary()) -> riakc:obj().
pbc_read(Pid, Bucket, Key) ->
    pbc_read(Pid, Bucket, Key, []).

-spec pbc_read(pid(), binary()|{binary(), binary()}, binary(), [any()]) -> riakc:obj().
pbc_read(Pid, Bucket, Key, Options) ->
    {ok, Value} = riakc_pb_socket:get(Pid, Bucket, Key, Options),
    Value.

-spec pbc_read_check(pid(), binary()|{binary(), binary()}, binary(), [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed) ->
    pbc_read_check(Pid, Bucket, Key, Allowed, []).

-spec pbc_read_check(pid(), binary()|{binary(), binary()}, binary(), [any()], [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed, Options) ->
    case riakc_pb_socket:get(Pid, Bucket, Key, Options) of
        {ok, _} ->
            true = lists:member(ok, Allowed);
        Other ->
            lists:member(Other, Allowed) orelse throw({failed, Other, Allowed})
    end.

-type put_response() :: ok|{ok, riakc:obj()}|{error, any()}.

%% @doc does a write via the erlang protobuf client
-spec pbc_write(
    pid(), binary()|{binary(), binary()}, binary(), binary()) ->
        put_response().
pbc_write(Pid, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Pid, Object).

%% @doc does a write via the erlang protobuf client plus content-type
-spec pbc_write(
    pid(), binary()|{binary(), binary()}, binary(), binary(), list()) ->
        put_response().
pbc_write(Pid, Bucket, Key, Value, CT) ->
    Object = riakc_obj:new(Bucket, Key, Value, CT),
    riakc_pb_socket:put(Pid, Object).

%% @doc does a write via the erlang protobuf client plus content-type
-spec pbc_write(
    pid(), binary()|{binary(), binary()}, binary(), binary(), list(), list()) ->
        put_response().
pbc_write(Pid, Bucket, Key, Value, CT, Opts) ->
    Object = riakc_obj:new(Bucket, Key, Value, CT),
    riakc_pb_socket:put(Pid, Object, Opts).

%% @doc sets a bucket property/properties via the erlang protobuf client
-spec pbc_set_bucket_prop(pid(), binary(), [proplists:property()]) -> atom().
pbc_set_bucket_prop(Pid, Bucket, PropList) ->
    riakc_pb_socket:set_bucket(Pid, Bucket, PropList).

%% @doc Puts the contents of the given file into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_file(Pid, Bucket, Key, Filename) ->
    {ok, Contents} = file:read_file(Filename),
    riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Contents, "text/plain")).

%% @doc Puts all files in the given directory into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_dir(Pid, Bucket, Dir) ->
    ?LOG_INFO("Putting files from dir ~0p into bucket ~0p", [Dir, Bucket]),
    {ok, Files} = file:list_dir(Dir),
    [pbc_put_file(Pid, Bucket, list_to_binary(F), filename:join([Dir, F]))
     || F <- Files].

%% @doc True if the given keys have been really, really deleted.
%% Useful when you care about the keys not being there. Delete simply writes
%% tombstones under the given keys, so those are still seen by key folding
%% operations.
pbc_really_deleted(Pid, Bucket, Keys) ->
    StillThere =
    fun(K) ->
            Res = riakc_pb_socket:get(Pid, Bucket, K,
                                      [{r, 1},
                                      {notfound_ok, false},
                                      {basic_quorum, false},
                                      deletedvclock]),
            case Res of
                {error, notfound} ->
                    false;
                _ ->
                    %% Tombstone still around
                    true
            end
    end,
    [] == lists:filter(StillThere, Keys).

%% @doc Returns HTTP URL(s) for one or a list of Nodes.
-ifdef(COVARIANT_SPECS).
-spec https_url(
    Node :: node()) -> URL :: nonempty_string() ; (
    Nodes :: rtt:nodes()) -> URLs :: nonempty_list(nonempty_string()).
-else.  % EDoc
-spec https_url(node() | rtt:nodes())
        -> nonempty_string() | nonempty_list(nonempty_string()).
-endif. % COVARIANT_SPECS
https_url(Node) when erlang:is_atom(Node) ->
    case get_https_conn_info(Node) of
        {ok, {Host, Port}} ->
            lists:flatten(io_lib:format("https://~s:~b", [Host, Port]));
        undefined ->
            erlang:error(undefined_https, [Node])
    end;
https_url([_|_] = Nodes) ->
    [http_url(Node) || Node <- Nodes].

%% @doc Returns HTTP URL(s) for one or a list of Nodes.
-ifdef(COVARIANT_SPECS).
-spec http_url(
    Node :: node()) -> URL :: nonempty_string() ; (
    Nodes :: rtt:nodes()) -> URLs :: nonempty_list(nonempty_string()).
-else.  % EDoc
-spec http_url(node() | rtt:nodes())
        -> nonempty_string() | nonempty_list(nonempty_string()).
-endif. % COVARIANT_SPECS
http_url(Node) when erlang:is_atom(Node) ->
    case get_http_conn_info(Node) of
        {ok, {Host, Port}} ->
            lists:flatten(io_lib:format("http://~s:~b", [Host, Port]));
        undefined ->
            erlang:error(undefined_http, [Node])
    end;
http_url([_|_] = Nodes) ->
    [http_url(Node) || Node <- Nodes].

%% @doc get me an http client.
-spec httpc(Node :: node()) -> Client :: rhc:rhc().
httpc(Node) ->
    rt:wait_for_service(Node, riak_kv),
    case get_http_conn_info(Node) of
        {ok, {IP, Port}} ->
            rhc:create(IP, Port, "riak", []);
        undefined ->
            erlang:error(undefined_http, [Node])
    end.

%% @doc does a read via the http erlang client.
-spec httpc_read(term(), binary()|{binary(), binary()}, binary()) -> binary().
httpc_read(C, Bucket, Key) ->
    {_, Value} = rhc:get(C, Bucket, Key),
    Value.

%% @doc does a write via the http erlang client.
-spec httpc_write(
    term(), binary()|{binary(), binary()}, binary()|undefined, binary()) ->
        ok|{error, any()}.
httpc_write(C, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    rhc:put(C, Object).

%% @doc does a write via the http erlang client.
-spec httpc_write(
    term(), binary()|{binary(), binary()}, binary()|undefined, binary(), list()) ->
        ok|{error, any()}.
httpc_write(C, Bucket, Key, Value, Opts) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    rhc:put(C, Object, Opts).

%%%===================================================================
%%% Command Line Functions
%%%===================================================================

%% @doc Call 'bin/riak admin' command on `Node' with arguments `Args'.
%% @equiv admin(Node, Args, [])
-spec admin(Node :: node(), Args :: rtt:cmd_args())
        -> {ok, rtt:output_line()}.
admin(Node, Args) ->
    admin(Node, Args, []).

%% @doc Call 'bin/riak admin' command on `Node' with arguments `Args'.
%% The third parameter is a list of options. Valid options are:
%%    * `return_exit_code' - Return the exit code along with the command output
-spec admin(
    Node :: node(), Args :: rtt:cmd_args(),
    Options :: proplists:proplist() )
        -> {ok, string() | {rtt:cmd_rc(), string()}}.
admin(Node, Args, Options) ->
    ?HARNESS:admin(Node, Args, Options).

%% @doc Call 'bin/riak admin status' command on `Node' and return the stats.
%%
%% Stats are returned as a list of 2-tuples:```
%%      [{<<"StatKey">>, <<"StatValue">>}, ...]'''
%% with the header being discarded.
%%
%% StatValues that split across lines, because that's how```
%%      .../riak/bin/riak admin status'''
%% prints them, are effectively unparseable, as only the portion printed
%% on the same line as the StatKey is included as StatValue.
%%
%% This function doesn't actually parse lines as Erlang terms, it just breaks
%% up strings - if you care about <i>ALL</i> StatValues being usable use
%% {@link rt:get_stats/1} instead.
-spec admin_stats(Node :: node() )
        -> {ok, nonempty_list({binary(), binary()})} | rtt:std_error().
admin_stats(Node) ->
    ?HARNESS:admin_stats(Node).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
-spec riak(Node :: node(), Args :: list(string()) ) -> {ok, string()}.
riak(Node, Args) ->
    ?HARNESS:riak(Node, Args).

%% @doc Call 'bin/riak repl' command on `Node' with arguments `Args'
-spec riak_repl(Node :: node(), Args :: list(string()) ) -> {ok, string()}.
riak_repl(Node, Args) ->
    ?HARNESS:riak_repl(Node, Args).

search_cmd(Node, Args) ->
    {ok, Cwd} = file:get_cwd(),
    rpc:call(Node, riak_search_cmd, command, [[Cwd | Args]]).

%% @doc Runs `riak attach' on `Node' and tests for expected behavior.
%%
%% <b>Change from historical behavior!</b>
%%
%% In Riak versions prior to 3+, this command attached to the node using the
%% `erl -remsh' option, whereas in 3+ it connects via a named pipe.
%% This disparity is the reason for its deprecation.
%%
%% @deprecated The `attach' command is deprecated by the `relx'-generated
%% Riak 3+ `riak' script. Use {@link interact/3} with the appropriate command
%% for the target Riak version:<dl>
%%  <dt>Riak 3+</dt><dd>`rt:interact(Node, "remote_console", Ops)'</dd>
%%  <dt>Riak 2-</dt><dd>`rt:interact(Node, "attach", Ops)'</dd>
%% </dl>
-spec attach(Node :: node(), Ops :: rtt:interact_list() )
        -> ok | no_return().
attach(Node, Ops) ->
    {ok, _} = interact(Node, "attach", Ops),
    ok.

%% @doc Runs `riak attach-direct' on `Node' and tests for expected behavior.
%% @deprecated The `attach-direct' command is not supported by the Riak 3+
%% `relx'-generated `riak' script. Use {@link interact/3} with the appropriate
%% command for the target Riak version:<dl>
%%  <dt>Riak 3+</dt><dd>`rt:interact(Node, "daemon_attach", Ops)'</dd>
%%  <dt>Riak 2-</dt><dd>`rt:interact(Node, "attach-direct", Ops)'</dd>
%% </dl>
-spec attach_direct(Node :: node(), Ops :: rtt:interact_list() )
        -> ok | no_return().
attach_direct(Node, Ops) ->
    {ok, _} = interact(Node, "attach-direct", Ops),
    ok.

%% @doc Runs `riak console' on `Node' and tests for expected behavior.
%% @deprecated Use {@link interact/3}: `rt:interact(Node, "console", Ops)'
%% for meaningful return values.
-spec console(Node :: node(), Ops :: rtt:interact_list() )
        -> ok | no_return().
console(Node, Ops) ->
    {ok, _} = interact(Node, "console", Ops),
    ok.

%% @doc Runs `riak <Command>' on `Node' and tests for expected behavior.
%%
%% Here's an example: ```
%%      rt:interact(Node, "attach", [
%%          {expect, "erlang.pipe.1 (^D to exit)"},
%%          {send, "riak_core_ring_manager:get_my_ring().\n"},
%%          {expect, "dict,"},
%%          {send, [4]},    %% 4 = Ctrl + D
%%          {expect, exit}
%%      ])'''
%%
%% <b>Important!</b>
%%
%% Note that supported commands differ by Riak version, and are <i>NOT</i>
%% checked for validity by this function. This means that issuing an
%% unsupported command <i>MAY</i> return `{ok, RC}' (where `RC' is non-zero)
%% rather than `{exit, RC}', depending on the `Operations' specified.
%%
%% For instance, the invocation```
%%      CtrlD = [4],
%%      %% Send ^D to exit the shell
%%      interact(Node, "garbage", [{send, CtrlD}, {expect, exit}])'''
%% can return either `{ok, 1}' <i>OR</i> `{exit, 1}', depending on arbitrary
%% timing factors within the VM (whether the `riak' script is still running
%% when the `{send, ...}' is executed). The moral of this story is that
%% `{ok, 0}' should always be considered the only valid result, with all other
%% patterns representing failure.
%%
%% @param Node
%%      The Riak node upon which to execute `Command'.
%%
%% @param Command
%%      The interactive `riak' command to execute on `Node', specified as a
%%      string consisting of a single word.
%%
%% @param Operations
%%      The sequence of inputs and expected results.
%%      Refer to {@link rtt:interact_list()} for details.
%%
%% @returns <dl>
%%  <dt>`{ok, ReturnCode}'</dt><dd>
%%      The command completed all specified operations within the allotted
%%      time and exited of its own volition, returning `ReturnCode'.<br/>
%%      If the `Cmd' program closed its `stdout' before exiting, or any
%%      other condition prevents retrieving the command's result code,
%%      `ReturnCode' will be set to `-1'.</dd>
%%  <dt>`{exit, ReturnCode}'</dt><dd>
%%      The command exited of its own volition before all specified
%%      operations were completed, returning `ReturnCode'.<br/>
%%      If the `Cmd' program closed its `stdout' before exiting, or any
%%      other condition prevents retrieving the command's result code,
%%      `ReturnCode' will be set to `-1'.</dd>
%%  <dt>`{timeout, Expected}'</dt><dd>
%%      An operation did not complete within its specified or default
%%      timeout.<br/>
%%      `Expected' is the predicate upon which the function was waiting
%%      when the timeout expired.</dd>
%%  <dt>`{timeout, Exe}'</dt><dd>
%%      The command did not complete within the specified `Timeout'.
%%      <br/>`Exe' is the executable's absolute path.</dd>
%%  <dt>`{error, {Reason, Path}}'</dt><dd>
%%      The command cannot be located or is not executable by the current
%%      user, or a nonexistent or inaccessible directory is specified.<br/>
%%      `Reason' is the Posix error constant representing the error and
%%      `Path' is the specified `Cmd' or `Dir' to which the error applies.
%%  </dd></dl>
%%
-spec interact(
    Node :: node(),
    Command :: nonempty_string(),
    Operations :: rtt:interact_list() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().
interact(Node, Command, Operations) ->
    ?HARNESS:interact(Node, Command, Operations).

%% @doc Copies config files from one set of nodes to another
copy_conf(NumNodes, FromVersion, ToVersion) ->
    ?HARNESS:copy_conf(NumNodes, FromVersion, ToVersion).

%%%===================================================================
%%% Search
%%%===================================================================

%% doc Enable the search KV hook for the given `Bucket'.  Any `Node'
%%     in the cluster may be used as the change is propagated via the
%%     Ring.
enable_search_hook(Node, Bucket) when is_binary(Bucket) ->
    ?LOG_INFO("Installing search hook for bucket ~0p", [Bucket]),
    ?assertEqual(ok, rpc:call(Node, riak_search_kv_hook, install, [Bucket])).

%%%===================================================================
%%% Test harness setup, configuration, and internal utilities
%%%===================================================================

%% @doc Sets the backend of ALL nodes that could be available to riak_test.
%%      this is not limited to the nodes under test, but any node that
%%      riak_test is able to find. It then queries each available node
%%      for it's backend, and returns it if they're all equal. If different
%%      nodes have different backends, it returns a list of backends.
%%      Currently, there is no way to request multiple backends, so the
%%      list return type should be considered an error.
-spec set_backend(atom()) -> atom()|[atom()].
set_backend(Backend) ->
    set_backend(Backend, []).

-spec set_backend(atom(), [{atom(), term()}]) -> atom()|[atom()].
set_backend(leveled, _) ->
    set_backend(riak_kv_leveled_backend);
set_backend(bitcask, _) ->
    set_backend(riak_kv_bitcask_backend);
set_backend(eleveldb, _) ->
    set_backend(riak_kv_eleveldb_backend);
set_backend(memory, _) ->
    set_backend(riak_kv_memory_backend);
set_backend(multi, Extras) ->
    set_backend(riak_kv_multi_backend, Extras);
set_backend(Backend, _) when Backend == riak_kv_bitcask_backend;
                             Backend == riak_kv_eleveldb_backend;
                             Backend == riak_kv_memory_backend;
                             Backend == riak_kv_leveled_backend ->
    ?LOG_INFO("rt:set_backend(~0p)", [Backend]),
    update_app_config(all, [{riak_kv, [{storage_backend, Backend}]}]),
    get_backends();
set_backend(Backend, Extras) when Backend == riak_kv_multi_backend ->
    MultiConfig = proplists:get_value(multi_config, Extras, default),
    Config = make_multi_backend_config(MultiConfig),
    update_app_config(all, [{riak_kv, Config}]),
    get_backends();
set_backend(Other, _) ->
    ?LOG_WARNING("rt:set_backend doesn't recognize ~0p as a legit backend, using the default.", [Other]),
    get_backends().

make_multi_backend_config(default) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []},
                      {<<"bitcask1">>, riak_kv_bitcask_backend, []},
                      {<<"leveled1">>, riak_kv_leveled_backend, []}]}];
make_multi_backend_config(indexmix) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []},
                      {<<"leveled1">>, riak_kv_leveled_backend, []}]}];
make_multi_backend_config(Other) ->
    ?LOG_WARNING("rt:set_multi_backend doesn't recognize ~0p as legit multi-backend config, using default", [Other]),
    make_multi_backend_config(default).

-spec get_backends() -> rtt:backend() | rtt:backends().
get_backends() ->
    Backends = ?HARNESS:get_backends(),
    case Backends of
        [riak_kv_bitcask_backend] -> bitcask;
        [riak_kv_eleveldb_backend] -> eleveldb;
        [riak_kv_memory_backend] -> memory;
        [riak_kv_leveled_backend] -> leveled;
        [Other] -> Other;
        MoreThanOne -> MoreThanOne
    end.

-spec get_backend(rtt:app_config()) -> rtt:backend() | error.
get_backend(AppConfigProplist) ->
    case kvc:path('riak_kv.storage_backend', AppConfigProplist) of
        [] -> error;
        Backend -> Backend
    end.

%% @doc Gets the string flavor of the version tag specified
%%      (current, legacy, previous, etc).
-spec get_version(Vsn :: rtt:vsn_tag()) -> rtt:vsn_str() | unknown.
get_version(Vsn) ->
    ?HARNESS:get_version(Vsn).

%% @doc Gets the string flavor of the `current' version.
-spec get_version() -> rtt:vsn_str() | unknown.
get_version() ->
    get_version(current).

%% @doc Gets the comparable version of the specified version tag.
%% @end
%% Note: `rt_vsn:tagged_version/1' relies on this API.
-spec get_vsn_rec(Vsn :: rtt:vsn_tag() ) -> rtt:vsn_rec()  | unknown.
get_vsn_rec(Vsn) ->
    ?HARNESS:get_vsn_rec(Vsn).

%% @doc Gets the comparable version of the `current' version.
-spec get_vsn_rec() -> rtt:vsn_rec()  | unknown.
get_vsn_rec() ->
    get_vsn_rec(current).

%% @doc Gets the integral major OTP release of the current VM.
-spec otp_release() -> pos_integer().
otp_release() ->
    case application:get_env(riak_test, otp_release) of
        {ok, Val} ->
            Val;
        _ ->
            {Rel, _} = string:to_integer(erlang:system_info(otp_release)),
            application:set_env(riak_test, otp_release, Rel),
            Rel
    end.

%% @doc Gets the integral major OTP release of a running Node.
-spec otp_release(Node :: node()) -> pos_integer() | unknown.
otp_release(Node) ->
    case rpc:call(Node, erlang, system_info, [otp_release]) of
        {badrpc, _} ->
            unknown;
        [$R | Str] ->
            {Rel, _} = string:to_integer(Str),
            Rel;
        Num ->
            {Rel, _} = string:to_integer(Num),
            Rel
    end.

%% @doc outputs some useful information about nodes that are up
whats_up() ->
    ?HARNESS:whats_up().

-spec get_ip(Node :: node() ) -> nonempty_string().
get_ip(Node) ->
    ?HARNESS:get_ip(Node).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses ?LOG_INFO/1 'String' semantics
-spec log_to_nodes(Nodes :: rtt:nodes(), String :: unicode:chardata()) -> ok.
log_to_nodes(Nodes, String) ->
    log_to_nodes(Nodes, "~s", [String]).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses ?LOG_INFO/2 'LFmt' and 'LArgs' semantics
-spec log_to_nodes(
    Nodes :: rtt:nodes(), LFmt :: io:format(), LArgs :: list()) -> ok.
log_to_nodes(Nodes, LFmt, LArgs) ->
    %% This logs to a node's info level, but if riak_test is running
    %% at debug level, we want to know when we send this and what
    %% we're saying
    ?LOG_DEBUG("log_to_nodes: " ++ LFmt, LArgs),
    Format = "---riak_test--- " ++ LFmt,
    lists:foreach(
        fun(Node) ->
            rt_logger:log_to_node(Node, info, Format, LArgs)
        end, lists:flatten(Nodes)).

%% @doc Spawns processes to invoke Fun(Arg) on each element of Args.
%% @equiv pmap(Fun, Args, rt_config:get(rt_max_wait_time))
-spec pmap(Fun :: fun((term()) -> term()), Args :: list(term()))
        -> list(term()).
pmap(Fun, []) when erlang:is_function(Fun, 1) ->
    [];
pmap(Fun, [_|_] = Args) when erlang:is_function(Fun, 1) ->
    pmap(Fun, Args, rt_config:get(rt_max_wait_time)).

%% @doc Spawns processes to invoke Fun(Arg) on each element of Args within
%%      Timeout milliseconds.
%%
%% @returns The result of each invocation in the same order as Args.
%% If an invocation times out, its result is `timeout'.
-spec pmap(
    Fun :: fun((term()) -> term()),
    Args :: list(term()),
    Timeout :: rtt:millisecs() )
        -> list(term()).
pmap(Fun, [], Timeout)
        when erlang:is_function(Fun, 1) andalso ?is_timeout(Timeout) ->
    [];
pmap(Fun, [_|_] = Args, Timeout)
        when erlang:is_function(Fun, 1) andalso ?is_timeout(Timeout) ->
    TrapExit = erlang:process_flag(trap_exit, true),
    Parent = erlang:self(),
    Spawn = fun(Arg, Pids) ->
        Proc = erlang:spawn_link(?MODULE, pmap_child, [Parent, Fun, Arg]),
        [Proc | Pids]
    end,
    %% Spawn in Args order, then keep the list of child pids in the same order
    %% for later results collection.
    Children = lists:reverse(lists:foldl(Spawn, [], Args)),
    %% Single instant point of reference for Timeout - we don't want to take
    %% (length(Args) x Timeout) overall.
    Start = erlang:monotonic_time(millisecond),
    Collect = fun(Child) ->
        Elapsed = (erlang:monotonic_time(millisecond) - Start),
        Remaining = erlang:max(0, (Timeout - Elapsed)),
        receive
            {pmap, Child, Result} ->
                Result;
            {'EXIT', Child, Reason} ->
                {'EXIT', Reason}
        after
            Remaining ->
                erlang:exit(Child, kill),
                timeout
        end
    end,
    %% Collect them in the order they were spawned.
    Results = [Collect(Child) || Child <- Children],
    %% Clean up 'EXIT' messages
    consume_exits(Children),
    TrapExit orelse erlang:process_flag(trap_exit, TrapExit),
    Results.

%% @private Child process spawned by pmap/2.
-spec pmap_child(
    Parent :: pid(),
    Fun :: fun((term()) -> term()),
    Arg :: term() ) -> ok.
pmap_child(Parent, Fun, Arg) ->
    Result = (catch Fun(Arg)),
    _ = Parent ! {pmap, erlang:self(), Result},
    erlang:exit(normal).

%% @hidden
-spec consume_exits(list(pid())) -> ok.
consume_exits([]) ->
    ok;
consume_exits([Pid | Pids]) ->
    receive
        {'EXIT', Pid, _} ->
            ok
    after
        0 ->
            ok
    end,
    consume_exits(Pids).

%% @doc Reset all harness-managed resources to default clean state.
%%
%% <b>Change from historical behavior!</b>
%%
%% Although this function has always specified its parameters as the module
%% under test and harness settings to apply to it, it hasn't ever been
%% enforced, leading to tests misusing the API.
%%
%% Since tests really shouldn't be calling this function in the first place,
%% the least they can do is call it correctly in case it matters someday, so
%% parameter types are now enforced.
-spec setup_harness(TestModule :: module(), HarnessArgs :: list(string()) )
        -> ok | rtt:std_error().
setup_harness(TestModule, HarnessArgs)
        when erlang:is_atom(TestModule) andalso erlang:is_list(HarnessArgs) ->
    ?HARNESS:setup_harness(TestModule, HarnessArgs).

%% @doc Processes log files from specified running nodes.
%%
%% Replaces `get_node_logs/0'
%%
%% <b>Change from historical behavior!</b>
%%
%% Prior implementations of get_node_logs/0 returned a file path with an open
%% file handle to it for each existing log file owned by any of the harness's
%% nodes, whether running or not. This had the following problems:<ul>
%%  <li>The only existing implementation returned log files from <i>ALL</i>
%%      nodes, not only running ones.</li>
%%  <li>It left an open file handle for each file returned.</li>
%%  <li>It was not easily implementable in a harness operating on nodes
%%      outside the local filesystem.</li>
%% </ul>
%%
%% @param Nodes
%%  Either the atom `all' for "all running nodes" or a list of explicit nodes
%%  upon which to operate. If a non-running node is specified the harness
%%  implementation <i>MAY</i> throw some manner of exception.
%%
%% @param FileFun
%%  For each log file on a specified node, ```
%%      FileFun(Node, LogFile, FileFunParam)''' is invoked.
%%  `FileFun' receives the fully-qualified local path of a log file owned by
%%  `Node' and may only read, not modify, the file. The result of `FileFun'
%%  can be any term meaningful to the caller, see the "returns" section.<br/>
%%  Note that `FileFun' may be invoked in parallel on multiple nodes and/or
%%  files, and no invocation order can be assumed.
%%
%% @param FileFunParam
%%  A caller-supplied term passed through to `FileFun'.
%%
%% @returns
%%  A list of all of the `FileFun' invocations' outputs in unspecified order.
%%  No attempt is made to correlate the input and output lists, so if it
%%  matters include suitable information in the `FileFun' result.
%% @end
%%
%% As of this writing, the functionality is only used by one test, so the API
%% has been changed here and the test revised to use it.
%%
%% Implementing harnesses can rely on the parameters matching the spec.
-spec process_node_logs(
    Nodes :: all | rtt:nodes(),
    FileFun :: rtt:proc_node_file_fun(),
    FileFunParam :: term() )
        -> nonempty_list(term()) | rtt:std_error().
%% Multiple heads for readability, doing it all in one guard sequence was too
%% convoluted - let the compiler sort it out.
process_node_logs(all = Nodes, FileFun, FFParam)
        when erlang:is_function(FileFun, 3) ->
    ?HARNESS:process_node_logs(Nodes, FileFun, FFParam);
process_node_logs([N | _] = Nodes, FileFun, FFParam) when erlang:is_atom(N)
        andalso erlang:is_function(FileFun, 3) ->
    ?HARNESS:process_node_logs(Nodes, FileFun, FFParam).

-spec get_node_debug_logs() -> list({ok, rtt:fs_path()} | rtt:std_error()).
get_node_debug_logs() ->
    ?HARNESS:get_node_debug_logs().

-spec get_node_id(Node :: node()) -> Result :: rtt:node_id().
get_node_id(Node) ->
    ?HARNESS:get_node_id(Node).

-spec get_node_path(Node :: node()) -> Result :: rtt:fs_path().
get_node_path(Node) ->
    ?HARNESS:get_node_path(Node).

-spec get_node_version(Node :: rtt:node_id() | node())
        -> Result :: rtt:vsn_tag().
get_node_version(Node) ->
    ?HARNESS:get_node_version(Node).

%% @doc Performs a search against the log files on `Node' and returns all
%% matching lines.
-spec search_logs(Node :: node(), Pattern :: iodata() )
        -> list(rtt:file_line_match() | rtt:std_error()).
search_logs(Node, Pattern) ->
    ?HARNESS:search_logs(Node, Pattern).

check_ibrowse() ->
    try sys:get_status(ibrowse) of
        {status, _Pid, {module, gen_server} ,_} -> ok
    catch
        Throws ->
            ?LOG_ERROR("ibrowse error ~0p", [Throws]),
            ?LOG_ERROR("Restarting ibrowse"),
            application:stop(ibrowse),
            application:start(ibrowse)
    end.

post_result(TestResult, #rt_webhook{url=URL, headers=HookHeaders, name=Name}) ->
    ?LOG_INFO("Posting result to ~s ~s", [Name, URL]),
    try ibrowse:send_req(URL,
            [{"Content-Type", "application/json"}],
            post,
            mochijson2:encode(TestResult),
            [{content_type, "application/json"}] ++ HookHeaders,
            300000) of  %% 5 minute timeout

        {ok, RC=[$2|_], Headers, _Body} ->
            {ok, RC, Headers};
        {ok, ResponseCode, Headers, Body} ->
            ?LOG_INFO("Test Result did not generate the expected 2XX HTTP response code."),
            ?LOG_DEBUG("Post"),
            ?LOG_DEBUG("Response Code: ~0p", [ResponseCode]),
            ?LOG_DEBUG("Headers: ~0p", [Headers]),
            ?LOG_DEBUG("Body: ~0p", [Body]),
            error;
        X ->
            ?LOG_WARNING("Some error POSTing test result: ~0p", [X]),
            error
    catch
        Class:Reason ->
            ?LOG_ERROR("Error reporting to ~s. ~0p:~0p", [Name, Class, Reason]),
            ?LOG_ERROR("Payload: ~0p", [TestResult]),
            error
    end.

%%%===================================================================
%%% Bucket Types Functions
%%%===================================================================

%% @doc create and immediately activate a bucket type
create_and_activate_bucket_type(Node, Type, Props) ->
    ok = rpc:call(Node, riak_core_bucket_type, create, [Type, Props]),
    wait_until_bucket_type_status(Type, ready, Node),
    ok = rpc:call(Node, riak_core_bucket_type, activate, [Type]),
    wait_until_bucket_type_status(Type, active, Node).

create_activate_and_wait_for_bucket_type([Node|_Rest]=Cluster, Type, Props) ->
    create_and_activate_bucket_type(Node, Type, Props),
    wait_until_bucket_type_visible(Cluster, Type).

wait_until_bucket_type_status(Type, ExpectedStatus, Nodes) when is_list(Nodes) ->
    [wait_until_bucket_type_status(Type, ExpectedStatus, Node) || Node <- Nodes];
wait_until_bucket_type_status(Type, ExpectedStatus, Node) ->
    F = fun() ->
                ActualStatus = rpc:call(Node, riak_core_bucket_type, status, [Type]),
                ExpectedStatus =:= ActualStatus
        end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec bucket_type_visible([atom()], binary()|{binary(), binary()}) -> boolean().
bucket_type_visible(Nodes, Type) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    IsVisible = fun erlang:is_list/1,
    {Res, NodesDown} = rpc:multicall(Nodes, riak_core_bucket_type, get, [Type], MaxTime),
    NodesDown == [] andalso lists:all(IsVisible, Res).

wait_until_bucket_type_visible(Nodes, Type) ->
    F = fun() -> bucket_type_visible(Nodes, Type) end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec see_bucket_props([atom()], binary()|{binary(), binary()},
                       proplists:proplist()) -> boolean().
see_bucket_props(Nodes, Bucket, ExpectProps) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    IsBad = fun({badrpc, _}) -> true;
               ({error, _}) -> true;
               (Res) when is_list(Res) -> false
            end,
    HasProps = fun(ResProps) ->
                       lists:all(fun(P) -> lists:member(P, ResProps) end,
                                 ExpectProps)
               end,
    case rpc:multicall(Nodes, riak_core_bucket, get_bucket, [Bucket], MaxTime) of
        {Res, []} ->
            % No nodes down, check no errors
            case lists:any(IsBad, Res) of
                true  ->
                    false;
                false ->
                    lists:all(HasProps, Res)
            end;
        {_, _NodesDown} ->
            false
    end.

wait_until_bucket_props(Nodes, Bucket, Props) ->
    F = fun() ->
                see_bucket_props(Nodes, Bucket, Props)
        end,
    ?assertEqual(ok, rt:wait_until(F)).


%% @doc Set up in memory log capture to check contents in a test.
setup_log_capture(Nodes) when is_list(Nodes) ->
    lists:foreach(fun rt_logger:plugin_logger/1, Nodes);
setup_log_capture(Node) when not is_list(Node) ->
    rt_logger:plugin_logger(Node).

expect_in_log(Node, Pattern) ->
    {Delay, Retry} = get_retry_settings(),
    expect_in_log(Node, Pattern, Retry, Delay).

expect_in_log(Node, Pattern, Retry, Delay) ->
    CheckLogFun = fun() ->
        Logs = rt_logger:get_logs(Node),
        ?LOG_INFO("looking for pattern ~s in logs for ~0p",
                   [Pattern, Node]),
        case re:run(Logs, Pattern, []) of
            {match, _} ->
                ?LOG_INFO("Found match"),
                true;
            nomatch    ->
                ?LOG_INFO("No match"),
                false
        end
    end,
    case rt:wait_until(CheckLogFun, Retry, Delay) of
        ok ->
            true;
        _ ->
            false
    end.

%% @doc Returns `true' if Pattern is _not_ found in the logs for `Node',
%% `false' if it _is_ found.
-spec expect_not_in_logs(Node::node(), Pattern::iodata()) -> boolean().
expect_not_in_logs(Node, Pattern) ->
    case search_logs(Node, Pattern) of
        [] ->
            true;
        _Matches ->
            false
    end.

%% @doc Wait for Riak Control to start on a single node.
%%
%% Non-optimal check, because we're blocking for the gen_server to start
%% to ensure that the routes have been added by the supervisor.
%%
wait_for_control(_Vsn, Node) when is_atom(Node) ->
    ?LOG_INFO("Waiting for riak_control to start on node ~0p.", [Node]),

    %% Wait for the gen_server.
    rt:wait_until(Node, fun(N) ->
                case rpc:call(N,
                              riak_control_session,
                              get_version,
                              []) of
                    {ok, _} ->
                        true;
                    Error ->
                        ?LOG_INFO("Error was ~0p.", [Error]),
                        false
                end
        end),

    %% Wait for routes to be added by supervisor.
    wait_for_any_webmachine_route(Node, [admin_gui, riak_control_wm_gui]).

wait_for_any_webmachine_route(Node, Routes) ->
    ?LOG_INFO("Waiting for routes ~0p to be added to webmachine.", [Routes]),
    rt:wait_until(Node, fun(N) ->
        case rpc:call(N, webmachine_router, get_routes, []) of
            {badrpc, Error} ->
                ?LOG_INFO("Error was ~0p.", [Error]),
                false;
            RegisteredRoutes ->
                case is_any_route_loaded(Routes, RegisteredRoutes) of
                    false ->
                        false;
                    _ ->
                        true
                end
        end
    end).

is_any_route_loaded(SearchRoutes, RegisteredRoutes) ->
    lists:any(fun(Route) -> is_route_loaded(Route, RegisteredRoutes) end, SearchRoutes).

is_route_loaded(Route, Routes) ->
    lists:keymember(Route, 2, Routes).

%% @doc Wait for Riak Control to start on a series of nodes.
wait_for_control(VersionedNodes) when is_list(VersionedNodes) ->
    [wait_for_control(Vsn, Node) || {Vsn, Node} <- VersionedNodes].

%% @doc Choose random in cluster, for example.
-spec select_random([any()]) -> any().
select_random(List) ->
    Length = length(List),
    Idx = random_uniform(Length),
    lists:nth(Idx, List).

%% @doc Returns a random element from a given list.
-spec random_sublist([any()], integer()) -> [any()].
random_sublist(List, N) ->
    % Assign a random value for each element in the list.
    List1 = [{random_uniform(), E} || E <- List],
    % Sort by the random number.
    List2 = lists:sort(List1),
    % Take the first N elements.
    List3 = lists:sublist(List2, N),
    % Remove the random numbers.
    [ E || {_,E} <- List3].

-spec random_uniform() -> float().
%% @equiv rand:uniform/0
random_uniform() ->
    rand:uniform().

-spec random_uniform(Range :: pos_integer()) -> pos_integer().
%% @equiv rand:uniform/1
random_uniform(Range) ->
    rand:uniform(Range).

%% @doc Get call count from ETS table, key being a {Module, Fun, Arity}.
-spec get_call_count([node()], {atom(), atom(), non_neg_integer()}) ->
                            non_neg_integer().
get_call_count(Cluster, MFA) when is_list(Cluster) ->
    case ets:lookup(?RT_ETS, MFA) of
        [{_,Count}] ->
            Count;
        [] ->
            0
    end.

%% @doc Count calls in a dbg:tracer process for various {Module, Fun, Arity}
%%      traces.
-spec count_calls([node()], [{atom(), atom(), non_neg_integer()}]) -> ok.
count_calls(Cluster, MFAs) when is_list(Cluster) ->
    ?LOG_INFO("count all calls to MFA ~0p across the cluster ~0p",
               [MFAs, Cluster]),
    RiakTestNode = node(),
    maybe_create_ets(),
    dbg:tracer(process, {fun trace_count/2, {RiakTestNode, Cluster}}),
    [{ok, Node} = dbg:n(Node) || Node <- Cluster],
    dbg:p(all, call),
    [{ok, _} = dbg:tpl(M, F, A, [{'_', [], [{return_trace}]}]) || {M, F, A} <- MFAs],
    ok.

%% @doc Maybe create an ETS table for storing function calls and counts.
-spec maybe_create_ets() -> ok.
maybe_create_ets() ->
    case ets:info(?RT_ETS) of
        undefined ->
            ets:new(?RT_ETS, ?RT_ETS_OPTS),
            ok;
        _ ->
            ets:delete(?RT_ETS),
            ets:new(?RT_ETS, ?RT_ETS_OPTS),
            ok
    end.

%% @doc Stop dbg tracing.
-spec stop_tracing() -> ok.
stop_tracing() ->?
    LOG_INFO("stop all dbg tracing"),
    stop_and_clear(),
    ok.

-if(?OTP_RELEASE >= 25).
stop_and_clear() ->
    dbg:stop().
-else.
stop_and_clear() ->
    dbg:stop_clear().
-endif.


get_primary_preflist(Node, Bucket, Key, NVal) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {ok, PL}.

%% @doc Trace fun calls and store their count state into an ETS table.
-spec trace_count({trace, pid(), call|return_from,
                   {atom(), atom(), non_neg_integer()}}, {node(), [node()]}) ->
                         {node(), [node()]}.
trace_count({trace, _Pid, call, {_M, _F, _A}}, Acc) ->
    Acc;
trace_count({trace, _Pid, return_from, MFA, _Result}, {RTNode, Cluster}) ->
    Count = get_call_count(Cluster, MFA),
    Count2 = Count + 1,
    rpc:call(RTNode, ets, insert, [?RT_ETS, {MFA, Count2}]),
    {RTNode, Cluster}.

-spec assert_capability(node(), rtt:capability(), atom()) -> ok.
assert_capability(CNode, Capability, Value) ->
    ?LOG_INFO("Checking Capability Setting ~0p =:= ~0p on ~0p",
               [Capability, Value, CNode]),
    ?assertEqual(ok, rt:wait_until_capability(CNode, Capability, Value)),
    ok.

-spec assert_supported(rtt:capabilities(), rtt:capability(), atom() | [atom()]) -> ok.
assert_supported(Capabilities, Capability, Value) ->
    ?LOG_INFO("Checking Capability Supported Values ~0p =:= ~0p",
               [Capability, Value]),
    ?assertEqual(Value, proplists:get_value(
                          Capability,
                          proplists:get_value('$supported', Capabilities))),
    ok.

-spec reset_cookie(Node :: node(), ToCookie :: atom(), FromCookie :: atom())
        -> true | tuple().
reset_cookie(Node, OldCookie, NewCookie) ->
    %% TODO: Will it break if we do the OTP 25+ calls on all versions?
    OtpRel = otp_release(),
    ?LOG_INFO(
        "Resetting cookie back to ~0p from ~0p for Node ~0p",
        [OldCookie, NewCookie, Node]),
    OtpRel < 25 orelse erlang:set_cookie(Node, NewCookie),
    R = rpc:call(Node, erlang, set_cookie, [OldCookie]),
    OtpRel < 25 orelse erlang:set_cookie(Node, OldCookie),
    ?LOG_INFO("RPC response ~0p for Node ~0p", [R, Node]),
    R.

-spec no_op(term()) -> ok.
no_op(_Params) ->
    ok.

-spec get_stats(Node :: node()) -> rtt:stats().
get_stats(Node) ->
    [Exe | Args] = CmdElems =
        ["curl", "-s", "-S", rt:http_url(Node) ++ "/stats"],
    ?LOG_INFO(
        "Retrieving stats from ~0p using command ~s",
        [Node, rt_exec:cmd_line(CmdElems)]),
    case cmd(Exe, Args) of
        {0, StatString} ->
            {struct, Stats} = mochijson2:decode(StatString),
            Stats;
        {error, What} ->
            erlang:error(What, CmdElems);
        Error ->
            erlang:error(Error, CmdElems)
    end.

get_stats(Node, 0) ->
    get_stats(Node);
get_stats(Node, WaitBeforeMS)
        when erlang:is_integer(WaitBeforeMS) andalso WaitBeforeMS > 0 ->
    timer:sleep(WaitBeforeMS),
    get_stats(Node).

-spec get_stat(
    NodeOrStats :: node() | nonempty_list({binary(), term()}),
    Key :: atom() | binary() | nonempty_string() )
        -> term() | undefined.
get_stat(Node, Key) when erlang:is_atom(Node) ->
    get_stat(get_stats(Node), Key);
get_stat([_|_] = Stats, Key)
        when erlang:is_binary(Key) andalso erlang:size(Key) > 0 ->
    %% lists:keyfind/3 is implemented directly as a NIF, so use it for speed
    %% over large Stats lists (as Riak produces).
    %% proplists:get_value/[23] is implemented in Erlang and takes much longer
    %% on usual Riak stats.
    case lists:keyfind(Key, 1, Stats) of
        {_Key, Val} ->
            Val;
        _ ->
            undefined
    end;
get_stat([_|_] = Stats, Key) when erlang:is_atom(Key) ->
    get_stat(Stats, erlang:atom_to_list(Key));
get_stat([_|_] = Stats, [_|_] = Key) ->
    get_stat(Stats, erlang:list_to_binary(Key));
get_stat(Stats, Key) ->
    erlang:error(badarg, [Stats, Key]).

expected_stat_values(Node, NameExpectedValues) ->
    Stats = get_stats(Node),
    lists:all(
        fun({Name, ExpectedValue}) ->
            case get_stat(Stats, Name) of
                ExpectedValue ->
                    true;
                CurrentValue ->
                    ?LOG_INFO(
                        "Waiting until ~0p stat equals ~0p (currently ~0p)",
                        [Name, ExpectedValue, CurrentValue]),
                    false
            end
        end,
        NameExpectedValues
    ).

-ifdef(TEST).

verify_product(Applications, ExpectedApplication) ->
    meck:new(rpc, [unstick]),
    RpcCall = fun(fakenode, application, which_applications, []) ->
        Applications
    end,
    meck:expect(rpc, call, RpcCall),
    ?assertMatch(ExpectedApplication, product(fakenode)),
    meck:unload(rpc).

product_test_() ->
    {foreach,
        fun() -> ok end,
        [
            ?_test(verify_product([riak_cs], riak_cs)),
            ?_test(verify_product([riak_repl, riak_kv, riak_cs], riak_cs)),
            ?_test(verify_product([riak_repl, riak_kv], riak)),
            ?_test(verify_product([riak_kv], riak)),
            ?_test(verify_product([kernel], unknown))
        ]}.

-endif.