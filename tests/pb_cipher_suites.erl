%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(pb_cipher_suites).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(assertDenied(Op), ?assertMatch({error, <<"Permission",_/binary>>}, Op)).

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(inets),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/pb_cipher_suites_certs",

    %% make a bunch of crypto keys
    ?LOG_INFO("running make_certs with version ~p", [make_certs:version()]),

    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:intermediateCA(CertDir, "revokedCA", "rootCA"),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com", "site5.basho.com"]),
    make_certs:enduser(CertDir, "revokedCA", "site6.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "site5.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "revokedCA"),

    %% start a HTTP server to serve the CRLs
    %%
    %% NB: we use the 'stand_alone' option to link the server to the
    %% test process, so it exits when the test process exits.
    {ok, _HTTPPid} = inets:start(httpd, [{port, 8000}, {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}], stand_alone),

    ?LOG_INFO("Deploy some nodes"),
    Conf = [{riak_core, [
                {ssl, [
                    {certfile, filename:join([CertDir,"site3.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir, "site3.basho.com/key.pem"])},
                    {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
                    ]}
                ]},
            {riak_search, [
                           {enabled, true}
                          ]}
           ],

    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),


    [_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),

    ?LOG_INFO("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    ?LOG_INFO("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "password"]]),

    CipherList =
        "ECDHE-RSA-AES256-SHA384:ECDH-ECDSA-AES128-SHA:ECDH-ECDSA-AES256-SHA384",


    %% set a simple default cipher list, one good one a and one shitty one
    rpc:call(Node, riak_core_security, set_ciphers, [CipherList]),
    rpc:call(Node, application, set_env, [riak_api, honor_cipher_order, true]),

    GoodCiphers = element(1, riak_core_ssl_util:parse_ciphers(CipherList)),
    ?LOG_INFO("Good ciphers: ~0p", [GoodCiphers]),
    [AES256, AES128, _ECDSA] =
        ParsedCiphers =
            lists:map(fun(PC) -> cipher_format(PC) end, GoodCiphers),

    ?LOG_INFO("Parsed Ciphers ~0p", [ParsedCiphers]),

    ?LOG_INFO("Check that the server's preference for ECDHE-RSA-AES256-SHA384"
               " is honored"),
    AES256T = convert_suite_to_tuple(AES256),
    {ok, {'tlsv1.2', AES256R}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                            {cacertfile,
                                filename:join([CertDir, "rootCA/cert.pem"])},
                            {ssl_opts,
                                [{ciphers, ParsedCiphers}]}
                            ]),
    ?LOG_INFO("With cipher order - ~0p", [AES256R]),
    ?assertEqual(AES256T, {element(1, AES256R),
                            element(2, AES256R),
                            element(3, AES256R)}),
    ?LOG_INFO("Ignoring reversal of cipher order!!"),
    {ok, {'tlsv1.2', AES256R}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                            {cacertfile,
                                filename:join([CertDir, "rootCA/cert.pem"])},
                            {ssl_opts,
                                [{ciphers,
                                    lists:reverse(ParsedCiphers)}]}
                            ]),

    ?LOG_INFO("Do we assume that cipher order is not honoured?"),

    SingleCipherProps =
        [{credentials, "user", "password"},
            {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
            {ssl_opts, [{ciphers, [AES128]}]}],
    ?LOG_INFO("Setting weak cipher now throws insufficient security"),
    insufficient_check(Port, SingleCipherProps),

    ?LOG_INFO("check that connections trying to use tls 1.1 fail"),
    {error,{tcp,{tls_alert,ProtocolVersionError}}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                {ssl_opts, [{versions, ['tlsv1.1']}]}
                            ]),

    ?LOG_INFO("check that connections trying to use tls 1.0 fail"),
    {error,{tcp,{tls_alert,ProtocolVersionError}}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                {ssl_opts, [{versions, ['tlsv1']}]}
                            ]),
    ?LOG_INFO("check that connections trying to use ssl 3.0 fail"),
    OTP24SSL3Error =
        {error,{tcp,{options,{sslv3,{versions,[sslv3]}}}}},
    OTP22SSL3Error =
        {error,{tcp,{tls_alert,ProtocolVersionError}}},
    SSL3Error =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                    {ssl_opts, [{versions, ['sslv3']}]}
                                    ]),
    ?assert(lists:member(SSL3Error, [OTP22SSL3Error, OTP24SSL3Error])),

    ?LOG_INFO("Enable ssl 3.0, tls 1.0 and tls 1.1 and disable tls 1.2"),
    rpc:call(Node, application, set_env, [riak_api, tls_protocols,
                                            [sslv3, tlsv1, 'tlsv1.1']]),

    ?LOG_INFO("check that connections trying to use tls 1.2 fail"),
    ?assertMatch({error,{tcp,{options,{'tls1.2',{versions,['tls1.2']}}}}},
                    pb_connection_info(Port,
                                    [{credentials, "user",
                                        "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                    "rootCA/cert.pem"])},
                                        {ssl_opts, [{versions, ['tls1.2']}]}
                                    ])),

    ?LOG_INFO("Re-enabling old protocols will work in OTP 22"),
    check_with_reenabled_protools(Port, CertDir),

    ?LOG_INFO("Reset tls protocols back to the default"),
    rpc:call(Node, application, set_env, [riak_api, tls_protocols,
                                          ['tlsv1.2']]),

    ?LOG_INFO("checking CRLs are checked for client certificates by"
              " default"),

    ok = rpc:call(Node, riak_core_console, add_user, [["site5.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site5.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    ?LOG_INFO("Checking revoked certificates are denied"),
    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site5.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                       {certfile, filename:join([CertDir, "site5.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site5.basho.com/key.pem"])}
                                      ])),

    ?LOG_INFO("Disable CRL checking"),
    rpc:call(Node, application, set_env, [riak_api, check_crl,
                                          false]),

    ?LOG_INFO("Checking revoked certificates are allowed"),
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                     [{credentials, "site5.basho.com",
                                       ""},
                                      {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                      {certfile, filename:join([CertDir, "site5.basho.com/cert.pem"])},
                                      {keyfile, filename:join([CertDir, "site5.basho.com/key.pem"])}
                                     ]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),
    riakc_pb_socket:stop(PB),

    ok = check_reasons(ProtocolVersionError),

    pass.

pb_get_socket(PB) ->
    %% XXX this peeks into the pb_socket internal state and plucks out the
    %% socket. If the internal representation ever changes, this will break.
    element(6, sys:get_state(PB)).

pb_connection_info(Port, Config) ->
    case riakc_pb_socket:start("127.0.0.1", Port, Config) of
        {ok, PB} ->
            ?assertEqual(pong, riakc_pb_socket:ping(PB)),
            {ok, ConnInfo} = ssl:connection_information(pb_get_socket(PB)),
            {protocol, P} = lists:keyfind(protocol, 1, ConnInfo),
            {selected_cipher_suite, CS} =
                lists:keyfind(selected_cipher_suite, 1, ConnInfo),
            riakc_pb_socket:stop(PB),
            {ok, {P, convert_suite_to_tuple(CS)}};
        Error ->
            Error
    end.


convert_suite_to_tuple(CS) when is_tuple(CS) ->
    CS;
convert_suite_to_tuple(CS) when is_map(CS) ->
    {maps:get(key_exchange, CS),
        maps:get(cipher, CS),
        maps:get(mac, CS)}.

%% Require OTP-22+

cipher_format(Cipher) ->
    Cipher.

insufficient_check(Port, SingleCipherProps) ->
    {error,
        {tcp,
            {tls_alert,
                {insufficient_security, _ErrorMsg}}}} =
        pb_connection_info(Port, SingleCipherProps).

check_reasons({protocol_version,
                "TLS client: In state hello received SERVER ALERT:"
                " Fatal - Protocol Version\n"}) ->
    ok;
check_reasons(ProtocolVersionError) ->
    ?LOG_INFO("Unexpected error ~0p", [ProtocolVersionError]),
    error.

check_with_reenabled_protools(Port, CertDir) ->
    case rt:otp_release() of
        AtLeast23 when AtLeast23 >= 23 ->
            ok;
        _Pre23 ->
            ?LOG_INFO("Check tls 1.1 succeeds - OK as long as cipher good?"),
            ?LOG_INFO("Note that only 3-tuple returned for 1.1 - and sha not sha384"),
            ?assertMatch({ok,{'tlsv1.1',{ecdhe_rsa,aes_256_cbc,sha}}},
                pb_connection_info(Port, [
                    {credentials, "user", "password"},
                    {cacertfile, filename:join([CertDir, "rootCA", "cert.pem"])},
                    {ssl_opts, [{versions, ['tlsv1.1']}]}
                ])),

            ?LOG_INFO("check tls 1.0 succeeds - OK as long as cipher is good?"),
            ?assertMatch({ok,{'tlsv1',{ecdhe_rsa,aes_256_cbc,sha}}},
                pb_connection_info(Port, [
                    {credentials, "user", "password"},
                    {cacertfile, filename:join([CertDir, "rootCA", "cert.pem"])},
                    {ssl_opts, [{versions, ['tlsv1']}]}
                ]))
    end.
