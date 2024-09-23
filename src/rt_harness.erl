%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% @doc `rt_harness' behavior, for confirming harness implementations.
%%
%% EDoc can't, at present, generate actual documentation for callbacks,
%% so we're left with attempting to provide useful information with
%% descriptive parameter and type names.
%%
%% Implementing sources generate their callback export list with```
%%  erl -pa _build/default/lib/riak_test/ebin -noshell -eval \
%%      'lists:foreach(fun({F,A}) -> io:format("    ~s/~b,~n", [F, A]) end, \
%%          rt_harness:behaviour_info(callbacks)), halt(0).' '''
%%
%% @todo Document all callbacks to some reasonable extent.
%%
%% @end
-module(rt_harness).

%% ===================================================================
%% Behavior Callback API
%% ===================================================================

%% IMPORTANT:
%%  Returned results from callbacks with result pattern
%%      ok | rtt:std_error()
%%  are not to be trusted until they've all been verified.
%%  At present, that pattern is just a placeholder for implemented functions
%%  whose results weren't expected to be checked.

-callback admin(
    Node :: node(), Args :: rtt:cmd_args(),
    Options :: proplists:proplist() )
        -> {ok, string() | {rtt:cmd_rc(), string()}}.

-callback admin_stats(Node :: node() )
        -> {ok, nonempty_list({binary(), binary()})} | rtt:std_error().

-callback clean_data_dir(Nodes :: list(node()), SubDir :: string() )
        -> ok | rtt:std_error().

-callback cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args(),
    Form :: rtt:cmd_out_format() )
        -> Result :: rtt:cmd_result() | rtt:cmd_error().

-callback copy_conf(
    NumNodes :: pos_integer(),
    FromVersion :: rtt:vsn_tag(),
    ToVersion :: rtt:vsn_tag() ) ->
        ok | rtt:std_error().

%% Return the default node name if it's not configured.
%% The node MUST be reachable by all test nodes.
-callback default_node_name() -> node().

-callback deploy_clusters(ClusterConfigs :: nonempty_list(rtt:node_configs()))
        -> rtt:clusters().

-callback deploy_nodes(Configs :: rtt:node_configs()) -> rtt:nodes().

-callback get_backends() -> list(rtt:backend() | error).

-callback get_deps() -> rtt:fs_path().

-callback get_ip(Node :: node() ) -> nonempty_string().

-callback get_node_debug_logs() -> list({ok, rtt:fs_path()} | rtt:std_error()).

-callback get_node_id(Node :: node() ) -> rtt:node_id().

-callback get_node_path(Node :: node() ) -> rtt:fs_path().

-callback get_node_version(Node :: rtt:node_id() | node() ) -> rtt:vsn_tag().

%% Return the raw version string of the specified version tag.
-callback get_version(VsnTag :: rtt:vsn_tag() ) -> rtt:vsn_str()  | unknown.

%% Return the comparable version of the specified version tag.
-callback get_vsn_rec(VsnTag :: rtt:vsn_tag() ) -> rtt:vsn_rec()  | unknown.

-callback interact(
    Node :: node(),
    Command :: nonempty_string(),
    Operations :: rtt:interact_list() )
        -> {ok, rtt:cmd_rc()} | {exit, rtt:cmd_rc()} |
            {timeout, rtt:interact_pred() | rtt:cmd_exe()} | rtt:cmd_error().

-callback process_node_logs(
    Nodes :: all | rtt:nodes(),
    FileFun :: rtt:proc_node_file_fun(),
    FileFunParam :: term() )
        -> nonempty_list(term()) | rtt:std_error().

-callback restore_data_dir(
    Nodes :: rtt:nodes(),
    BackendFldr :: rtt:fs_path(),
    BackupFldr :: rtt:fs_path() ) ->
        ok | rtt:std_error().

-callback riak(Node :: node(), Args :: list(string()) ) -> {ok, string()}.

-callback riak_repl(Node :: node(), Args :: list(string()) ) -> {ok, string()}.

-callback search_logs(Node :: node(), Pattern :: iodata() )
        -> list(rtt:file_line_match() | rtt:std_error()).

-callback set_advanced_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback set_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback setup_harness(
    TestModule :: module(),
    HarnessArgs :: list(string()) ) ->
        ok | rtt:std_error().

-callback spawn_cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args() ) ->
        rtt:cmd_token() | rtt:cmd_exec_err().

-callback spawned_result(
    CmdToken :: rtt:cmd_token() ) ->
        {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_timeout_err().

-callback start(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback start_naked(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback stop(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback teardown() ->
        ok.

-callback update_app_config(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_tag(),
    UpgradeCallback :: rtt:upgrade_cb() )
        -> ok | rtt:std_error().

-callback upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_tag(),
    NewAppConfig :: rtt:app_config(),
    UpgradeCallback :: rtt:upgrade_cb() )
        -> ok | rtt:std_error().

-callback versions() ->
        nonempty_list(rtt:vsn_tag()).

-callback whats_up() ->
        list(rtt:fs_path()).
