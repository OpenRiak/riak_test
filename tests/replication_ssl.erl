%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
%% Copyright (c) 2022 Workday, Inc.
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
-module(replication_ssl).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    %% test requires allow_mult=false
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),

    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),
    BaseConf = [
            {riak_repl,
             [
                {ssl_enabled, false},
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    PrivDir = rt:priv_dir(),

    ?LOG_INFO("priv dir: ~0p -> ~0p", [code:priv_dir(riak_test), PrivDir]),

    SSLConfig1 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site1.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site1.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site1.basho.com"])}
            ]}
    ],

    SSLConfig2 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site2.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site2.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site2.basho.com"])}
            ]}
    ],

    SSLConfig3 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site3.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site3.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site3.basho.com"])}
            ]}
    ],

    %% same as above,with a depth of 0
    SSLConfig3A = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {ssl_depth, 0},
                {certfile, filename:join([CertDir,
                            "site3.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site3.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site3.basho.com"])}
            ]}
    ],

    SSLConfig4 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {ssl_depth, 0},
                {certfile, filename:join([CertDir,
                            "site4.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site4.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site4.basho.com"])}
            ]}
    ],

    SSLConfig5 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {ssl_depth, 1},
                {peer_common_name_acl, ["*.basho.com"]},
                {certfile, filename:join([CertDir,
                            "site1.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site1.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site1.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig6 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {ssl_depth, 1},
                {peer_common_name_acl, ["site1.basho.com"]},
                {certfile, filename:join([CertDir,
                            "site2.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site2.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site2.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig7 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {ssl_enabled, true},
                {peer_common_name_acl, ["ca.cataclysm-software.net"]},
                {certfile, filename:join([PrivDir,
                            "certs/cacert.org/ny-cert-old.pem"])},
                {keyfile, filename:join([PrivDir,
                            "certs/cacert.org/ny-key.pem"])},
                {cacertdir, filename:join([PrivDir,
                            "certs/cacert.org/ca"])}
            ]}
    ],

    ?LOG_INFO("===testing basic connectivity"),

    [Node1, Node2] = rt:deploy_nodes(2, BaseConf, [riak_kv, riak_repl]),

    Listeners = replication:add_listeners([Node1]),
    replication:verify_listeners(Listeners),

    {Ip, Port, _} = hd(Listeners),
    replication:add_site(Node2, {Ip, Port, "site1"}),

    replication:wait_for_site_ips(Node2, "site1", Listeners),

    rt:log_to_nodes([Node1, Node2], "Basic connectivity test"),
    ?assertEqual(ok, replication:wait_until_connection(Node1)),

    ?LOG_INFO("===testing you can't connect to a server with a cert with the same common name"),
    rt:log_to_nodes([Node1, Node2], "Testing identical cert is disallowed"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig1, BaseConf)}, false)),

    ?LOG_INFO("===testing you can't connect when peer doesn't support SSL"),
    rt:log_to_nodes([Node1, Node2], "Testing missing ssl on peer fails"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, BaseConf}, false)),

    ?LOG_INFO("===testing you can't connect when local doesn't support SSL"),
    rt:log_to_nodes([Node1, Node2], "Testing missing ssl locally fails"),
    ?assertMatch({fail, _}, test_connection({Node1, BaseConf},
            {Node2, merge_config(SSLConfig2, BaseConf)}, false)),

    ?LOG_INFO("===testing simple SSL connectivity"),
    rt:log_to_nodes([Node1, Node2], "Basic SSL test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig2, BaseConf)}, true)),

    ?LOG_INFO("===testing SSL connectivity with an intermediate CA"),
    rt:log_to_nodes([Node1, Node2], "Intermediate CA test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig3, BaseConf)}, true)),

    ?LOG_INFO("===testing disallowing intermediate CAs works"),
    rt:log_to_nodes([Node1, Node2], "Disallowing intermediate CA test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig3A, BaseConf)},
            {Node2, merge_config(SSLConfig4, BaseConf)}, true)),

    ?LOG_INFO("===testing disallowing intermediate CAs disallows connections"),
    rt:log_to_nodes([Node1, Node2], "Disallowing intermediate CA test 2"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig3A, BaseConf)},
            {Node2, merge_config(SSLConfig1, BaseConf)}, false)),

    ?LOG_INFO("===testing wildcard and strict ACLs"),
    rt:log_to_nodes([Node1, Node2], "wildcard and strict ACL test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig5, BaseConf)},
            {Node2, merge_config(SSLConfig6, BaseConf)}, true)),

    ?LOG_INFO("===testing expired certificates fail"),
    rt:log_to_nodes([Node1, Node2], "expired certificates test"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig5, BaseConf)},
            {Node2, merge_config(SSLConfig7, BaseConf)}, false)),

    ?LOG_INFO("Connectivity tests passed"),

    ?LOG_INFO("Re-deploying 6 nodes"),

    Nodes = rt:deploy_nodes(6, BaseConf, [riak_kv, riak_repl]),

    [rt:wait_until_pingable(N) || N <- Nodes],

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),

    ?LOG_INFO("Reconfiguring nodes with SSL options"),
    [rt:update_app_config(N, merge_config(SSLConfig5, BaseConf)) || N <-
        ANodes],

    [rt:update_app_config(N, merge_config(SSLConfig6, BaseConf)) || N <-
        BNodes],

    [rt:wait_until_pingable(N) || N <- Nodes],

    ?LOG_INFO("Build cluster A"),
    repl_util:make_cluster(ANodes),

    ?LOG_INFO("Build cluster B"),
    repl_util:make_cluster(BNodes),

    replication:replication(ANodes, BNodes, false),

    pass.

merge_config(Mixin, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Mixin), lists:keysort(1, Base)).

%% test_connection(NodeConf1 :: tuple(), NodeConf2 :: tuple(), ShouldConnect :: boolean())
test_connection(Left, Right, true) ->
    test_connection(Left, Right);
test_connection(Left, Right, false) ->
    DefaultTimeout = rt_config:get(rt_max_wait_time),
    ConnFailTimeout = rt_config:get(conn_fail_time, 60000),
    rt_config:set(rt_max_wait_time, ConnFailTimeout),
    Result = test_connection(Left, Right),
    rt_config:set(rt_max_wait_time, DefaultTimeout),
    Result.

test_connection({Node1, Config1}, {Node2, Config2}) ->
    rt:update_app_config(Node1, Config1),
    rt:wait_until_pingable(Node1),
    rt:update_app_config(Node2, Config2),
    rt:wait_until_pingable(Node2),
    rt:wait_for_service(Node1, [riak_kv, riak_repl]),
    rt:wait_for_service(Node2, [riak_kv, riak_repl]),
    replication:wait_until_connection(Node1).
