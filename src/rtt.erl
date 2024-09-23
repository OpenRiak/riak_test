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
%% @doc Shared type definitions for riak_test.
%%
%% Many types are necessarily loosely defined - these are more placeholders
%% than real type definitions, where their names are (hopefully) descriptive
%% of the sort of data they represent.
%%
%% @todo Document all types to some reasonable extent.
%% @end
-module(rtt).

%% Shared types
%%
%% For generating the export list (massaging required):```
%%  sed -En 's!^-(type|opaque) *([^\(]+)\(\) *::.+$!\2/0,!p' src/rtt.erl
%%  sed -En 's!^-(type|opaque) *([^\(]+)\(([^\)]+)\) *::.+$!\2/\3,!p' src/rtt.erl
%% '''
%%
-export_type([
    any_vsn/0,
    app_config/0,
    app_vsn_rec/0,
    backend/0,
    backends/0,
    capabilities/0,
    capability/0,
    cfg_vsn/0,
    cluster/0,
    cluster_config/0,
    cluster_configs/0,
    clusters/0,
    cmd_arg/0,
    cmd_args/0,
    cmd_dir/0,
    cmd_error/0,
    cmd_exe/0,
    cmd_exec_err/0,
    cmd_list/0,
    cmd_out_format/0,
    cmd_rc/0,
    cmd_result/0,
    cmd_timeout_err/0,
    cmd_token/0,
    cmdline/0,
    config_elem/0,
    config_key/0,
    config_val/0,
    conn_info_list/0,
    cuttlefish_config/0,
    cuttlefish_elem/0,
    deploy_version/0,
    deploy_versions/0,
    env_var/0,
    env_vars/0,
    file_line_match/0,
    fs_path/0,
    interact_clear/0,
    interact_exit/0,
    interact_expect/0,
    interact_list/0,
    interact_match/0,
    interact_oper/0,
    interact_pause/0,
    interact_pred/0,
    interact_regex/0,
    interact_send/0,
    interface/0,
    interfaces/0,
    log_line/0,
    log_lines/0,
    millisecs/0,
    modules/0,
    net_endpoint/0,
    net_proto/0,
    node_config/0,
    node_configs/0,
    node_id/0,
    node_ids/0,
    nodes/0,
    output_line/0,
    output_lines/0,
    predicate/1,
    proc_node_file_fun/0,
    product/0,
    products/0,
    sem_vsn/0,
    service/0,
    services/0,
    stat/0,
    stat_key/0,
    stat_keys/0,
    stat_val/0,
    stats/0,
    std_error/0,
    strings/0,
    up_params/0,
    up_prm_dir/0,
    up_prm_vsn/0,
    upgrade_cb/0,
    vsn_rec/0,
    vsn_str/0,
    vsn_tag/0
]).

-type app_config() :: list(config_elem()).
-type backend() :: bitcask | leveldb | leveled | memory | multi.
-type backends() :: list(backend()).
-type capabilities() :: list(capability()).
-type capability() :: atom() | {atom(), tuple()}.

-type cmd_arg() :: string().
-type cmd_args() :: list(cmd_arg()).

-type cmd_dir() :: cwd | pwd | fs_path().
%% An indicator of a filesystem directory path.
%%
%% `cwd' and `pwd' are aliases for the current working directory.

-type cmd_error() :: cmd_exec_err() | cmd_timeout_err().

-type cmd_exe() :: fs_path().
%% The name of, or relative or absolute path to, an executable program.

-type cmd_exec_err() :: {error, {file:posix(), fs_path()}}.
%% An error with the filesystem element to which it applies.

-type cmd_list() :: [cmd_exe() | cmd_args()].
%% A list of an executable program identifier followed by its execution
%% parameters.

-type cmd_out_format() :: rlines | lines | string.
%% The format of output lines from program execution.
%% <dl>
%%  <dt>`rlines'</dt><dd>
%%      Output is returned as a list of lines in reverse output order,
%%      including blank lines, without their terminating newlines.</dd>
%%  <dt>`lines'</dt><dd>
%%      Output is returned as a list of lines in the order that the program
%%      output them, including blank lines, without their terminating
%%      newlines.</dd>
%%  <dt>`stream'</dt><dd>
%%      Output is returned in a single string, possibly with embedded
%%      newlines.<br/>
%%      Successive newlines (blank lines) are <i>NOT</i> stripped nor, by
%%      extension, are trailing newlines.</dd>
%% </dl>

-type cmd_rc() :: integer().
%% The return code from program execution.
%% <dl>
%%  <dt>`0'</dt><dd>Success.</dd>
%%  <dt>`1-255'</dt><dd>An error code returned by the operating system.</dd>
%%  <dt>`< 0'</dt><dd>An error result returned by the invoking function.</dd>
%% </dl>

-type cmd_result() ::
    {cmd_rc(), cmd_out_format(), output_lines() | output_line()}.
%% The result and output from program execution.

-type cmd_timeout_err() :: {error, {timeout, {cmd_list(), output_lines()}}}.
%% The error indicating that execution of the specified program did not
%% complete within its allowable timespan.
%%
%% Output collected before the allotted time expired is included, if
%% available, in the order received.

-type cmd_token() :: term().
%% An opaque token used to collect the result of asynchronous program
%% execution.

-type cmdline() :: nonempty_string().
%% A command-line string suitable for printing/logging, possibly including
%% quotes and/or escape characters.
%%
%% The string is <i>NOT</i> necessarily suitable for execution
%% (though it probably is).

-type config_elem() :: {config_key(), config_val()}.
-type config_key() :: atom().
-type config_val() :: term().
-type cuttlefish_elem() :: {nonempty_string(), nonempty_string()}.
-type cuttlefish_config() :: list(cuttlefish_elem()).
-type env_var() :: {os:env_var_name(), os:env_var_value() | false}.
-type env_vars() :: list(env_var()).
-type file_line_match() :: {fs_path(), pos_integer(), string()}.

-type fs_path() :: nonempty_string().
%% A relative or absolute path to a filesystem element.

-type interact_list() :: list(interact_oper()).
%% The list of operations to perform during an interactive program
%% invocation.
%%
%% Operations can appear in any order, with the sole restriction that an
%% {@link interact_exit()} may only appear as the last element of the list.

-type interact_oper() ::
    interact_clear() | interact_expect() | interact_pause() | interact_send().
%% An operation that can be submitted to an interactive program invocation.

-type interact_clear() :: clear.
%% Clears <i>ALL</i> previously received program output <i>EXCEPT</i> a
%% non-discardable terminal message (i.e. an exit status).

-type interact_pause() :: pause | {pause, millisecs()}.
%% Pauses operation processing for a default or specified number of
%% milliseconds.<br/>
%%
%% The default pause is one second.

-type interact_send() :: {send, nonempty_string() | binary()}.
%% Sends the specified data to the program.
%%
%% Newlines <i>MUST</i> be included as needed, none are appended.
%%
%% <b>Change from historical behavior!</b>
%%
%% The current implementation differs from the the prior `rt' `attach*' and
%% `console' APIs' `send' behavior:
%% <ul><li>
%%  The prior implementation sent data to the program only after clearing the
%%  received data buffer as if by {@link interact_clear()}, which they didn't
%%  provide a discreet command for.<br/>
%%  The current implementation provides the `clear' command, which must be
%%  included before `{send, Whatever}' to achieve equivalent behavior.
%% </li><li>
%%  The prior implementation always appended a newline to whatever data was
%%  sent, the current implementation does not - if you want a newline, include
%%  it in the data.
%% </li></ul>

-type interact_expect() ::
    {expect, interact_pred()} | {expect, interact_pred(), millisecs()}.
%% Waits for a line of program output to match the supplied
%% {@link interact_pred()}.
%%
%% A timeout may be specified within which the predicate must be met.
%% The default timeout is 5 seconds.

-type interact_pred() :: interact_match() | interact_regex() | interact_exit().
%% A predicate that can be used in an {@link interact_expect()} expression.

-type interact_exit() :: exit.
%% Waits for the executable to exit (or time out).
%%
%% All intervening program output is discarded.

-type interact_match() :: nonempty_string() | binary().
%% Matches an output line containing the specified sequence of bytes in
%% any position.

-type interact_regex() :: {re, iodata()} | re:mp().
%% Matches an output line matching the specified Regular Expression.
%%
%% If specified as `{re, iodata()}' the expression is compiled prior to
%% execution <i>without</i> any options.
%%
%% If using the same pattern repeatedly, it is recommended to pre-compile it.
%% If `re:compile_options()'s are needed, you <i>MUST</i> pre-compile.
%%
%% The compiled RE is applied to subject lines with only the
%% `{capture, none}' option.
%%
%% Refer to `re:compile/1' and `re:run/3' for details.

-type conn_info_list() :: list({node(), interfaces()}).
%% A list of nodes and the interfaces each exposes.

-type net_endpoint() :: {string()|inet:ip_address(), inet:port_number()}.
%% TCP/IP network connection endpoint.

-type net_proto() :: http | https | pb.
%% Network connection protocol.

-type interface() :: {net_proto(), net_endpoint()}.
%% One network connection endpoint with its protocol.

-type interfaces() :: list(interface()).
%% A list of protocol/endpoint pairs.

-type log_line() :: unicode:chardata().
%% As returned by the `logger:FModule:format/2' callback.

-type log_lines() :: list(log_line()).
%% A collection of log lines, generally in chronological order.

-type millisecs() :: 1..16#FFFFFFFF.
%% A timeout value, in milliseconds.

-type cluster() :: nonempty_list(node()).
-type clusters() :: nonempty_list(cluster()).
-type cluster_config() :: nonempty_list(app_config()) | pos_integer() |
    {pos_integer(), app_config()} | {pos_integer(), vsn_tag(), app_config()}.
-type cluster_configs() :: nonempty_list(cluster_config()).

-type modules() :: nonempty_list(module()).

-type deploy_version() :: vsn_tag() | node_config().
-type deploy_versions() :: nonempty_list(deploy_version()).
-type node_config() ::
    {vsn_tag(), default | app_config() | {cuttlefish, cuttlefish_config()}}.
-type node_configs() :: nonempty_list(node_config()).
-type node_id() :: pos_integer().
-type node_ids() :: nonempty_list(node_id()).
-type nodes() :: nonempty_list(node()).

-type output_line() :: string().
%% A line of output from an executed program.
%%
%% The operation specifies whether the string contains newlines.

-type output_lines() :: list(string()).
%% An ordered collection of lines of output from an executed program.
%%
%% Unless otherwise specified, the lines will not contain embedded or
%% trailing newlines.

-type proc_node_file_fun() :: fun((node(), fs_path(), term()) -> term()).
%% A function taking a Node, a filesystem path on that node, and a
%% caller-supplied parameter, that returns a term meaningful to the caller.

-type predicate(ArgType) :: fun((ArgType) -> boolean()).
-type product() :: riak | riak_cs | riak_ee | unknown.
-type products() :: list(product()).
-type service() :: atom().
-type services() :: list(service()).
-type stat() :: {stat_key(), stat_val()}.
-type stat_key() :: binary() | nonempty_string().
-type stat_keys() :: list(stat_key()).
-type stat_val() :: term().
-type stats() :: list(stat()).
-type std_error() :: {error, term()}.
-type strings() :: list(string()).

-type up_params() :: list(up_prm_dir() | up_prm_vsn()).
-type up_prm_dir() ::
    {new_conf_dir | old_conf_dir | new_data_dir | old_data_dir, fs_path()}.
-type up_prm_vsn() :: {new_version | old_version, vsn_tag()}.
-type upgrade_cb() :: fun((up_params()) -> ok | std_error()).

-type any_vsn() :: vsn_rec() | vsn_str() | app_vsn_rec().
%% Any version, before or after parsing.

-type app_vsn_rec() :: {vsn, nonempty_string()}.
%% Version representation in `.app' files, per (non-exported) type `application:application_opt()'.

-type cfg_vsn() :: {vsn_tag(), vsn_rec()}.
%% A version tag with the explicit version it maps to.

-type sem_vsn() :: nonempty_string().
%% The dotted decimal string representation of a version.
%%
%% Any properly formatted `sem_vsn()' always satisfies the requirements of
%% the {@link vsn_str()} type.

-type vsn_rec() :: {rtv, term()}.
%% An opaque comparable, strictly-ordered version representation, as produced
%% by {@link rt_vsn:new_version/1} or {@link rt_vsn:parse_version/1}, and
%% compared by {@link rt_vsn:compare_versions/2}

-type vsn_str() :: binary() | nonempty_string().
%% The supertype of all parsable version representations.
%%
%% Examples:<ul>
%%  <li>`<<"riak-3.0.10">>'</li>
%%  <li>`"2.1.0.1"'</li>
%%  <li>`"v1.0.7-5-gdc820cd9"'</li>
%%  <li>`"riak_kv-3.0.10+build.2069.ref3bddbc3"'</li>
%% </ul>

-type vsn_tag() :: atom() | binary() | nonempty_string().
%% The supertype of all recognized version tags in `riak_test.config'.
%%
%% Examples:<ul>
%%  <li>`current'</li>
%%  <li>`<<"3.0.10">>'</li>
%%  <li>`"2.1.6"'</li>
%%  <li>`mytest'</li>
%% </ul>
