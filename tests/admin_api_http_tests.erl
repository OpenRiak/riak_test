%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 TI Tokyo.
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
-module(admin_api_http_tests).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ADMIN_USERNAME, "admin").
-define(ADMIN_PASSWD, "damin").

confirm() ->
    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site1.basho.com"]),

    Conf1 =
        [{riak_core, [{ssl,
                       [{certfile,
                         filename:join(
                           [CertDir, "site1.basho.com/cert.pem"])
                        },
                        {keyfile,
                         filename:join(
                           [CertDir, "site1.basho.com/key.pem"])
                        },
                        {cacertfile,
                         filename:join(
                           [CertDir, "site1.basho.com/cacerts.pem"])
                        }
                       ]
                      }
                     ]
         },
         {riak_kv, [{anti_entropy, {off, []}},
                    {tictacaae_active, active}
                   ]
         },
         {riak_admin_api, [{admin_api_enabled, true},
                           {sec_group,[{superuser, true},
                                       {admin, true},
                                       {monitoring, true}]}
                          ]
         }
        ],
    [Node1, _Node2] = Nodes = rt:build_cluster([Conf1, []]),

    {ok, _} = rt:admin(Node1, ["admin-api", "reset"]),

    US1 = filename:join([rt_config:get(rt_scratch_dir), "userspec-1"]),
    file:write_file(US1, new_admin_blob()),
    {ok, _} = rt:admin(Node1, ["admin-api", "add-user", US1]),

    {ok, [{IP, Port}]} =
        erpc:call(Node1, application, get_env, [riak_admin_api, https]),

    application:ensure_all_started([ssl]),

    C = {IP, Port},
    general_tests(C),

    permissions_class_switch_tests(C, Node1),
    group_permissions_tests(C, Nodes),

    node_config_getset_tests(C, Nodes),

    cluster_observer_tests(C, Nodes),
    cluster_admin_tests(C, Nodes),

    security_user_tests(C),
    security_group_crud_tests(C),
    security_user_add_remove_group_tests(C),
    security_user_add_remove_permission_tests(C),

    vnode_tests(C, Nodes),
    tictacaae_tests(C, Nodes),

    pass.


general_tests(C) ->
    ?LOG_INFO("* general_tests"),
    assert_req(
      C, {<<"SecurityListPermissions">>, #{}},
      {"200", [<<"cluster_observer">>, <<"cluster_admin">>, <<"security">>]}),

    assert_req(
      C, {<<"SummonClaude">>, #{}},
      {"400", <<"Invalid request action">>}),

    ok.


node_config_getset_tests(C, [Node1|_]) ->
    ?LOG_INFO("* node_config_getset_tests"),
    assert_req(
      C, {<<"NodeGetAppEnv">>,
          #{<<"node">> => atom_to_binary(Node1)}},
      {"200",
       fun(A) ->
               case erl_parse:parse_term(
                     element(2, erl_scan:string(binary_to_list(A) ++ "."))) of
                   {ok, EE} when is_list(EE) ->
                       K = proplists:get_value(riak_admin_api, EE),
                       ?assert(K /= undefined),
                       ok;
                   _ ->
                       ?assert(false)
               end
       end}),

    assert_req(
      C, {<<"NodePutAppEnv">>, #{<<"node">> => atom_to_binary(Node1),
                                 <<"config">> => <<"[{riak_admin_api, [{who, me}]}]">>}},
      {"200", <<"ok">>}),

    ?assertEqual({ok, me}, erpc:call(Node1, application, get_env, [riak_admin_api, who])),

    assert_req(
      C, {<<"NodeGetAdvancedConfig">>, #{<<"node">> => atom_to_binary(Node1)}},
      {"200",
       fun(A) ->
               persistent_term:put(advanced_config_on_Node1, A),
               case erl_parse:parse_term(
                     element(2, erl_scan:string(binary_to_list(A) ++ "."))) of
                   {ok, EE} when is_list(EE) ->
                       K = proplists:get_value(riak_admin_api, EE),
                       ?assert(K /= undefined),
                       ok;
                   _ ->
                       ?assert(false)
               end
       end}),

    AdvancedConfigOnNode1 = persistent_term:get(advanced_config_on_Node1),
    NewAdvConf = binary:replace(AdvancedConfigOnNode1, <<"[">>, <<"[{riak_ts, [{is_alive, yes}]},">>),
    assert_req(
      C, {<<"NodePutAdvancedConfig">>, #{<<"node">> => atom_to_binary(Node1),
                                         <<"config">> => NewAdvConf}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"NodeGetAdvancedConfig">>, #{<<"node">> => atom_to_binary(Node1)}},
      {"200",
       fun(A) ->
               case erl_parse:parse_term(
                     element(2, erl_scan:string(binary_to_list(A) ++ "."))) of
                   {ok, EE} when is_list(EE) ->
                       K = proplists:get_value(riak_ts, EE),
                       ?assert(K /= undefined),
                       ok;
                   _ ->
                       ?assert(false)
               end
       end}),

    ok.


cluster_observer_tests(C, [Node1|_]) ->
    ?LOG_INFO("* cluster_observer_tests"),
    assert_req(
      C, {<<"ClusterGetStatus">>,
          #{<<"node">> => atom_to_binary(Node1)}},
      {"200", fun is_original_cluster/1}),
    ok.

cluster_admin_tests(C, Nodes) ->
    ?LOG_INFO("* cluster_admin_tests"),

    assert_req(
      C, {<<"ClusterStageLeave">>, #{<<"node">> => <<"dev2@127.0.0.1">>}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"ClusterPlan">>, #{}},
      {"200",
       fun(#{<<"actions">> := [#{<<"action">> := <<"leave">>,
                                 <<"node">> := <<"dev2@127.0.0.1">>}]}) -> ok;
          (_) -> ?assert(false) end}),

    assert_req(
      C, {<<"ClusterCommitPlan">>, #{}},
      {"200", <<"ok">>}),

    [Node1, Node2] = Nodes,
    rt:wait_until_ready(Node1),
    rt:wait_until_no_pending_changes([Node1]),
    %% there would be transfers still ongoing

    assert_req(
      C, {<<"ClusterGetStatus">>,
          #{<<"node">> => atom_to_binary(Node1)}},
      {"200", fun is_original_cluster_with_one_node_leaving/1}),

    rt:wait_until_unpingable(Node2),
    rt:start(Node2),
    rt:wait_until_ready(Node2),
    patient_assert_req(
      C, {<<"ClusterStageJoin">>, #{<<"node">> => <<"dev2@127.0.0.1">>}},
      {"200", <<"ok">>}),

    patient_assert_req(
      C, {<<"ClusterPlan">>, #{}},
      {"200",
       fun(#{<<"actions">> := [#{<<"action">> := <<"join">>,
                                 <<"node">> := <<"dev1@127.0.0.1">>}]}) -> ok;
          (_) -> not_ok end}),

    assert_req(
      C, {<<"ClusterCommitPlan">>, #{}},
      {"200", <<"ok">>}),

    rt:wait_until_ready(Node1),
    rt:wait_until_ready(Node2),
    rt:wait_until_no_pending_changes(Nodes),

    assert_req(
      C, {<<"ClusterGetStatus">>,
          #{<<"node">> => atom_to_binary(Node1)}},
      {"200", fun is_original_cluster/1}),

    ok.


is_original_cluster(
  #{<<"current_cluster">> :=
        [
         #{
           <<"is_me">> := true,
           <<"low_mem">> := LowMem1,
           <<"mem_erlang">> := MemErlang1,
           <<"mem_total">> := MemTotal1,
           <<"mem_used">> := MemUsed1,
           <<"name">> := <<"dev1@127.0.0.1">>,
           <<"pending_pct">> := PendingPct1,
           <<"reachable">> := true,
           <<"replacement">> := null,
           <<"ring_pct">> := RingPct1,
           <<"services">> := [<<"riak_kv">>],
           <<"staged_action">> := null,
           <<"status">> := <<"valid">>,
           <<"system_info">> :=
               #{
                 <<"nodename">> := <<"dev1@127.0.0.1">>,
                 <<"riak_version">> := RiakVersion1,
                 <<"system_version">> := SystemVersion1,
                 <<"uptime">> := Uptime1,
                 <<"uptime_str">> := UptimeStr1
                }
          },
         #{
           <<"is_me">> := false,
           <<"low_mem">> := LowMem2,
           <<"mem_erlang">> := MemErlang2,
           <<"mem_total">> := MemTotal2,
           <<"mem_used">> := MemUsed2,
           <<"name">> := <<"dev2@127.0.0.1">>,
           <<"pending_pct">> := PendingPct2,
           <<"reachable">> := true,
           <<"replacement">> := null,
           <<"ring_pct">> := RingPct2,
           <<"services">> := [<<"riak_kv">>],
           <<"staged_action">> := null,
           <<"status">> := <<"valid">>,
           <<"system_info">> :=
               #{
                 <<"nodename">> := <<"dev2@127.0.0.1">>,
                 <<"riak_version">> := RiakVersion2,
                 <<"system_version">> := SystemVersion2,
                 <<"uptime">> := Uptime2,
                 <<"uptime_str">> := UptimeStr2
                }
          }
        ],
    <<"down_nodes">> := [],
    <<"final_cluster">> := [],
    <<"staged_changes">> := []
   }) when is_boolean(LowMem1),
           is_boolean(LowMem2),
           is_integer(MemErlang1),
           is_integer(MemErlang2),
           is_integer(MemTotal1),
           is_integer(MemTotal2),
           is_integer(MemUsed1),
           is_integer(MemUsed2),
           is_number(PendingPct1),
           is_number(PendingPct2),
           RingPct1 + RingPct2 == 1,
           is_binary(RiakVersion1),
           RiakVersion1 == RiakVersion2,
           is_binary(SystemVersion1),
           SystemVersion1 == SystemVersion2,
           is_integer(Uptime1),
           is_binary(UptimeStr1),
           is_integer(Uptime2),
           is_binary(UptimeStr2) ->
    ok;
is_original_cluster(_) ->
    ?assert(false).

is_original_cluster_with_one_node_leaving(
  #{<<"current_cluster">> :=
        [
         #{
           <<"name">> := <<"dev1@127.0.0.1">>,
           <<"staged_action">> := null,
           <<"status">> := <<"valid">>
          },
         #{
           <<"name">> := <<"dev2@127.0.0.1">>,
           <<"status">> := <<"leaving">>
          }
        ],
    <<"down_nodes">> := [],
    <<"final_cluster">> := [],
    <<"staged_changes">> := []
   }) -> ok;
is_original_cluster_with_one_node_leaving(_) ->
    ?assert(false).



vnode_tests(C, [Node1|_]) ->
    ?LOG_INFO("* vnode_tests"),
    assert_req(
      C, {<<"VnodeGetStatus">>, #{<<"node">> => atom_to_binary(Node1),
                                  <<"preflists">> => <<"all">>}},
      {"200",
       fun([#{<<"backend_status">> := #{},
              <<"counter">> := Counter,
              <<"counter_lease">> := CounterLease,
              <<"counter_lease_size">> := CounterLeaseTime,
              <<"counter_leasing">> := CounterLeasing,
              <<"idx">> := Idx,
              <<"vnodeid">> := Vnodeid
             }|_]) when is_integer(Counter),
                        is_integer(CounterLease),
                        is_integer(CounterLeaseTime),
                        is_boolean(CounterLeasing),
                        is_binary(Idx),
                        is_binary(Vnodeid) -> true;
          (_) -> ?assert(false)
       end}),
    ok.

tictacaae_tests(C, [Node1|_]) ->
    ?LOG_INFO("* tictacaae_tests"),
    assert_req(
      C, {<<"TictacaaeGetStatus">>, #{<<"node">> => atom_to_binary(Node1)}},
      {"200",
       fun([#{<<"controller_pid">> := ControllerPid,
              <<"last_rebuild">> := <<"never">>,
              <<"next_rebuild">> := NextRebuild,
              <<"partition">> := Partition,
              <<"status">> := Status,
              <<"total_dirty_segments">> := 0
             }|_]) when is_binary(ControllerPid),
                        is_binary(NextRebuild),
                        is_binary(Partition),
                        is_binary(Status) -> true;
          (_) -> ?assert(false)
       end}),
    ok.


-define(USER1, <<"Soren">>).
-define(USER1AUTH, #{<<"method">> => <<"password">>,
                     <<"password">> => <<"Password">>}).
security_user_tests(C) ->
    ?LOG_INFO("* security_user_tests"),

    %% there's only one admin, created above via CLI
    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun([
            #{<<"auth_method">> := <<"password">>,
              <<"created">> := Created,
              <<"expires">> := <<"never">>,
              <<"groups">> := [],
              <<"modified">> := Modified,
              <<"name">> := <<"admin">>,
              <<"permissions">> := [<<"cluster_observer">>,
                                    <<"cluster_admin">>,
                                    <<"security">>]
             }
           ]) when Created == Modified ->
               ok;
          (_) ->
               ?assert(false)
       end}),

    assert_req(
      C, {<<"SecurityCreateUser">>,
          #{<<"name">> => ?USER1,
            <<"auth_details">> => ?USER1AUTH
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityCreateUser">>,
          #{<<"name">> => ?USER1,
            <<"auth_details">> => ?USER1AUTH
           }
         },
      {"412", <<"User or group already exists">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun([
            #{<<"name">> := <<"admin">>
             },
            #{<<"auth_method">> := <<"password">>,
              <<"created">> := Created,
              <<"expires">> := <<"never">>,
              <<"groups">> := [],
              <<"modified">> := Modified,
              <<"name">> := ?USER1,
              <<"permissions">> := []
             }
           ]) when Created == Modified ->
               ok;
          (_) ->
               ?assert(false)
       end}),

    NewExpires = os:system_time(millisecond) + 10_000,
    assert_req(
      C, {<<"SecuritySetUserExpiry">>,
          #{<<"name">> => ?USER1,
            <<"expires">> => NewExpires
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) when is_list(UU) ->
               [#{<<"expires">> := Expires,
                 <<"created">> := Created,
                 <<"modified">> := Modified}] =
                   lists:filter(fun(#{<<"name">> := A}) -> A == ?USER1 end, UU),
               case (calendar:rfc3339_to_system_time(
                       binary_to_list(Expires), [{unit, millisecond}])
                     == NewExpires) and
                   (Created < Modified) of
                   true -> ok;
                   false -> ?assert(false)
               end;
           (_) -> ?assert(false)
       end}),

    assert_req(
      C, {<<"SecuritySetUserExpiry">>,
          #{<<"name">> => ?USER1,
            <<"expires">> => <<"never">>
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun([_, #{<<"expires">> := <<"never">>}]) -> ok;
          (_) -> ?assert(false)
       end}),

    assert_req(
      C, {<<"SecuritySetUserExpiry">>,
          #{<<"name">> => <<"NotSoren">>,
            <<"expires">> => <<"never">>
           }
         },
      {"404", <<"No such user or group">>}),

    assert_req(
      C, {<<"SecuritySetUserExpiry">>,
          #{<<"name">> => <<"NotSoren">>,
            <<"expires">> => NewExpires - 10_000
           }
         },
      {"400", <<"Invalid parameter">>}),


    assert_req(
      C, {<<"SecurityDeleteUser">>,
          #{<<"name">> => ?USER1
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun([#{<<"name">> := <<"admin">>}]) ->
               ok;
          (_) ->
               ?assert(false)
       end}),

    ok.

security_group_crud_tests(C) ->
    ?LOG_INFO("* security_group_crud_tests"),

    assert_req(
      C, {<<"SecurityListGroups">>, #{}},
      {"200", []}),

    assert_req(
      C, {<<"SecurityCreateGroup">>,
          #{<<"name">> => <<"Serena">>}
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityCreateGroup">>,
          #{<<"name">> => <<"Serena">>}
         },
      {"412", <<"User or group already exists">>}),

    assert_req(
      C, {<<"SecurityListGroups">>, #{}},
      {"200",
       fun([
            #{<<"created">> := Created,
              <<"modified">> := Modified,
              <<"name">> := <<"Serena">>
             }
           ]) when is_binary(Created),
                   Created == Modified ->
               ok;
          (_) ->
               ?assert(false)
       end}),

    assert_req(
      C, {<<"SecurityDeleteGroup">>,
          #{<<"name">> => <<"NotSerena">>}
         },
      {"404", <<"No such user or group">>}),

    assert_req(
      C, {<<"SecurityDeleteGroup">>,
          #{<<"name">> => <<"Serena">>}
         },
      {"200", <<"ok">>}),

    ok.


security_user_add_remove_group_tests(C) ->
    ?LOG_INFO("* security_user_add_remove_group_tests"),

    assert_req(
      C, {<<"SecurityCreateUser">>,
          #{<<"name">> => ?USER1,
            <<"auth_details">> => ?USER1AUTH
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityCreateGroup">>,
          #{<<"name">> => <<"Serena">>}
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityAddUserGroups">>, #{<<"user">> => ?USER1,
                                         <<"groups">> => [<<"Serena">>]}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityAddUserGroups">>, #{<<"user">> => ?USER1,
                                         <<"groups">> => [<<"NotSerena">>]}},
      {"412", <<"No such user or group">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"groups">> := [<<"Serena">>],
                            <<"name">> := ?USER1
                           }) -> true;
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),

    %% adding existing group, idempotent
    assert_req(
      C, {<<"SecurityAddUserGroups">>, #{<<"user">> => ?USER1,
                                         <<"groups">> => [<<"Serena">>]}},
      {"200", <<"ok">>}),
    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"groups">> := [<<"Serena">>],
                            <<"name">> := ?USER1
                           }) -> true;
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),


    assert_req(
      C, {<<"SecurityDeleteUserGroups">>, #{<<"user">> => ?USER1,
                                            <<"groups">> => [<<"Serena">>]}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"groups">> := [],
                            <<"name">> := ?USER1
                           }) -> true;
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),

    %% idempotent delete
    assert_req(
      C, {<<"SecurityDeleteUserGroups">>, #{<<"user">> => ?USER1,
                                            <<"groups">> => [<<"Serena">>]}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"groups">> := [],
                            <<"name">> := ?USER1
                           }) -> true;
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),

    assert_req(
      C, {<<"SecurityDeleteUser">>,
          #{<<"name">> => ?USER1
           }
         },
      {"200", <<"ok">>}),

    ok.


security_user_add_remove_permission_tests(C) ->
    ?LOG_INFO("* security_user_add_remove_permission_tests"),

    assert_req(
      C, {<<"SecurityCreateUser">>,
          #{<<"name">> => ?USER1,
            <<"auth_details">> => ?USER1AUTH
           }
         },
      {"200", <<"ok">>}),

    NewPerms = [<<"security">>, <<"cluster_observer">>],
    assert_req(
      C, {<<"SecurityAddUserPermissions">>, #{<<"user">> => ?USER1,
                                              <<"permissions">> => NewPerms}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityAddUserPermissions">>, #{<<"user">> => ?USER1,
                                              <<"permissions">> => [<<"idkfa">>]}},
      {"400", <<"Invalid parameter">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"permissions">> := PP,
                            <<"name">> := ?USER1
                           }) -> lists:sort(PP) == lists:sort(NewPerms);
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),

    %% adding existing perms is idempotent
    assert_req(
      C, {<<"SecurityAddUserPermissions">>, #{<<"user">> => ?USER1,
                                              <<"permissions">> => NewPerms}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"permissions">> := PP,
                            <<"name">> := ?USER1
                           }) -> lists:sort(PP) == lists:sort(NewPerms);
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),


    PermsToDelete = [<<"security">>],
    RemainingPerms = NewPerms -- PermsToDelete,
    assert_req(
      C, {<<"SecurityDeleteUserPermissions">>, #{<<"user">> => ?USER1,
                                                 <<"permissions">> => PermsToDelete}},
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"200",
       fun(UU) ->
               case lists:any(
                      fun(#{<<"permissions">> := PP,
                            <<"name">> := ?USER1
                           }) when PP == RemainingPerms -> true;
                         (_) -> false
                      end, UU) of
                   true ->
                       ok;
                   (_) ->
                       ?assert(false)
               end
       end}),

    assert_req(
      C, {<<"SecurityDeleteUser">>,
          #{<<"name">> => ?USER1
           }
         },
      {"200", <<"ok">>}),

    ok.


permissions_class_switch_tests(C, Node) ->
    ?LOG_INFO("* permissions_class_switch_tests"),
    ok = rpc:call(
           Node, application, set_env,
           [riak_admin_api, sec_group, [{superuser, false},
                                        {admin, true},
                                        {monitoring, true}]]),
    assert_req(
      C, {<<"SecurityListUsers">>, #{}},
      {"403", <<"Request disabled">>}),

    ok = rpc:call(
           Node, application, set_env,
           [riak_admin_api, sec_group, [{superuser, true},
                                        {admin, true},
                                        {monitoring, false}]]),
    assert_req(
      C, {<<"ClusterGetStatus">>, #{}},
      {"403", <<"Request disabled">>}),

    ok = rpc:call(
           Node, application, set_env,
           [riak_admin_api, sec_group, [{superuser, true},
                                        {admin, true},
                                        {monitoring, true}]]),
    ok.


group_permissions_tests(C, _) ->
    ?LOG_INFO("* group_permissions_tests"),
    U2n = <<"u2">>,
    U2p = <<"kkk">>,
    assert_req(
      C, {<<"SecurityCreateUser">>,
          #{<<"name">> => U2n,
            <<"auth_details">> => #{<<"method">> => <<"password">>,
                                    <<"password">> => U2p},
            <<"permissions">> => [<<"cluster_observer">>]
           }
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"SecurityCreateGroup">>,
          #{<<"name">> => <<"g2">>,
            <<"permissions">> => [<<"cluster_admin">>]}
         },
      {"200", <<"ok">>}),

    assert_req(
      C, {<<"ClusterPlan">>, #{}},
      {"403", <<"Not authorised">>}, {U2n, U2p}),

    assert_req(
      C, {<<"SecurityAddUserGroups">>, #{<<"user">> => U2n,
                                         <<"groups">> => [<<"g2">>]}},
      {"200", <<"ok">>}),

    patient_assert_req(
      C, {<<"ClusterPlan">>, #{}},
      {"200",
       fun(#{}) -> ok; (_) -> not_ok end},
      {U2n, U2p}),

    assert_req(
      C, {<<"SecurityDeleteUser">>, #{<<"name">> => U2n}},
      {"200", <<"ok">>}),
    assert_req(
      C, {<<"SecurityDeleteGroup">>, #{<<"name">> => <<"g2">>}},
      {"200", <<"ok">>}),

    ok.



httpc_options({N, P}) when is_binary(N) ->
    httpc_options({binary_to_list(N), binary_to_list(P)});
httpc_options(Creds) ->
    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",
    [{is_ssl, true},
     {basic_auth, Creds},
     {ssl_options,
      [{cacertfile,
        filename:join([CertDir, "rootCA/cert.pem"])
       },
       {verify, verify_none},
       {reuse_sessions, false}
      ]
     }
    ].


assert_req(IPPort, Req, Exp) ->
    assert_req(IPPort, Req, Exp, {?ADMIN_USERNAME, ?ADMIN_PASSWD}).
assert_req({IP, Port}, {Action, Params}, {ExpStatusCode, ExpResult}, Creds) ->
    ?LOG_INFO("~s", [Action]),
    {ok, StatusCode, _, RespBody} =
        ibrowse:send_req(
          ff("https://~s:~b/ctl/~s", [IP, Port, Action]), [], post,
          mochijson2:encode(#{params => Params}),
          httpc_options(Creds)
         ),
    Key =
        case ExpStatusCode of
            GoodStatus when GoodStatus == "200" ->
                <<"result">>;
            _BadStatus ->
                <<"error">>
        end,
    TopObj = maps:get(Key, mochijson2:decode(RespBody, [{format, map}])),
    ?assertEqual(ExpStatusCode, StatusCode),
    if is_function(ExpResult) ->
            ExpResult(TopObj);
       el/=se ->
            ?assertMatch(ExpResult, TopObj)
    end.

patient_assert_req(IPPort, Req, Exp) ->
    patient_assert_req(IPPort, Req, Exp, {?ADMIN_USERNAME, ?ADMIN_PASSWD}).
patient_assert_req({IP, Port} = C, {Action, Params}, {ExpStatusCode, ExpResult}, Creds) ->
    ?LOG_INFO("~s", [Action]),
    {ok, StatusCode, _, RespBody} =
        ibrowse:send_req(
          ff("https://~s:~b/ctl/~s", [IP, Port, Action]), [], post,
          mochijson2:encode(#{params => Params}),
          httpc_options(Creds)
         ),
    Key =
        case ExpStatusCode of
            GoodStatus when GoodStatus == "200" ->
                <<"result">>;
            _BadStatus ->
                <<"error">>
        end,
    TopObj = maps:get(Key, mochijson2:decode(RespBody, [{format, map}])),
    if is_function(ExpResult) ->
            case ExpResult(TopObj) of
                ok -> ok;
                _ ->
                    ?LOG_INFO("."),
                    timer:sleep(1000),
                    patient_assert_req(C, {Action, Params}, {ExpStatusCode, ExpResult})
            end;
       el/=se ->
            if ExpResult =:= TopObj,
               ExpStatusCode == StatusCode ->
                    ok;
               el/=se ->
                    ?LOG_INFO("."),
                    timer:sleep(1000),
                    patient_assert_req(C, {Action, Params}, {ExpStatusCode, ExpResult})
            end
    end.

new_admin_blob() ->
    iolist_to_binary(
      ff("{\"name\": \"~s\",
  \"password\": \"~s\",
  \"expires\": \"never\",
  \"permissions\": \"all\"}", [?ADMIN_USERNAME, ?ADMIN_PASSWD]
        )
     ).


ff(F, A) ->
    lists:flatten(io_lib:format(F, A)).
