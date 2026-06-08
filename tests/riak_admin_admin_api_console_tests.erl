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
-module(riak_admin_admin_api_console_tests).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

confirm() ->
    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site1.basho.com"]),

    _PrivDir = rt:priv_dir(),
    Conf =
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
         {riak_admin_api, [{admin_api_enabled, true}]}
        ],
    [Node1] = rt:build_cluster(1, Conf),

    status_tests(Node1),
    user_crud_tests(Node1),
    user_various_perms_and_expiry(Node1),
    user_add_errors_test(Node1),

    pass.


status_tests(N) ->
    ?LOG_INFO("status_tests"),
    check_admin_cmd(
      N, "admin-api status",
      {re, "| *true *| *true *| *0 *| *0 *|"}),
    check_admin_cmd(
      N, "admin-api status disable",
      empty),
    check_admin_cmd(
      N, "admin-api status",
      {re, "| *false *| *true *| *0 *| *0 *|"}),
    check_admin_cmd(
      N, "admin-api status enable",
      empty),
    check_admin_cmd(
      N, "admin-api status",
      {re, "| *true *| *true *| *0 *| *0 *|"}),

    check_admin_cmd(
      N, "admin-api reset",
      {re, "All groups and users deleted"}),
    ok.

user_crud_tests(N) ->
    ?LOG_INFO("user_crud_tests"),
    Tmp = rt_config:get(rt_scratch_dir),
    check_admin_cmd(
      N, "admin-api list-users",
      empty),

    U1 = {"john", "john's PassWoRd", never, all},
    US1 = filename:join([Tmp, "userspec-1"]),
    file:write_file(US1, new_user_blob(U1)),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      empty),
    check_admin_cmd(
      N, "admin-api status",
      {re, "| *true *| *true *| *1 *| *0 *|"}),
    check_admin_cmd(
      N, "admin-api list-users", fun(A) -> assert_user_in_clique_table(A, U1) end),

    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      fun assert_user_exists_error/1),

    check_admin_cmd(
      N, ff("admin-api del-user ~s", [element(1, U1)]),
      empty),

    check_admin_cmd(
      N, ff("admin-api del-user ~s", [element(1, U1)]),
      fun assert_no_such_user/1),

    check_admin_cmd(
      N, "admin-api list-users",
      empty),
    ok.

user_various_perms_and_expiry(N) ->
    ?LOG_INFO("user_various_perms_and_expiry"),
    Tmp = rt_config:get(rt_scratch_dir),

    U1 = {"jack1", "john's PassWoRd", never, [cluster_admin]},
    US1 = filename:join([Tmp, "userspec-1"]),
    file:write_file(US1, new_user_blob(U1)),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      empty),
    check_admin_cmd(
      N, "admin-api list-users", fun(A) -> assert_user_in_clique_table(A, U1) end),

    U2 = {"jack2", "john's PassWoRd", os:system_time(millisecond) + 10_000, [cluster_admin]},
    US2 = filename:join([Tmp, "userspec-2"]),
    file:write_file(US2, new_user_blob(U2)),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US2]),
      empty),
    check_admin_cmd(
      N, "admin-api list-users", fun(A) -> assert_user_in_clique_table(A, U2) end),

    U3 = {"jack3", "john's PassWoRd", os:system_time(millisecond) + 10_000, [cluster_admin]},
    US3 = filename:join([Tmp, "userspec-2"]),
    file:write_file(US3, new_user_blob(U3)),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US3]),
      empty),
    check_admin_cmd(
      N, "admin-api list-users", fun(A) -> assert_user_in_clique_table(A, U3) end),

    U4 = {"jack4", "john's PassWoRd", calendar:system_time_to_rfc3339(
                                        os:system_time(second) + 10, [{unit, second}]), [cluster_admin]},
    US4 = filename:join([Tmp, "userspec-2"]),
    file:write_file(US4, new_user_blob(U4)),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US4]),
      empty),
    check_admin_cmd(
      N, "admin-api list-users", fun(A) -> assert_user_in_clique_table(A, U4) end),

    check_admin_cmd(
      N, "admin-api reset",
      {re, "All groups and users deleted"}),
    ok.

assert_user_exists_error(A) ->
    ["User already exists"|_] = string:split(A, "\n", all).

assert_no_such_user(A) ->
    ["No such user"|_] = string:split(A, "\n", all).


user_add_errors_test(N) ->
    ?LOG_INFO("user_add_errors_test"),
    Tmp = rt_config:get(rt_scratch_dir),
    US1 = filename:join([Tmp, "bad-userspec"]),

    file:write_file(
      US1,
      <<"Bad Json[ where's my wool?">>),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      fun assert_badjson_error/1),
    check_admin_cmd(
      N, "admin-api list-users",
      empty),

    file:write_file(
      US1,
      <<"{\"name\": \"john\",
          \"password\": \"fafa\",
          \"expires\": \"jamais\",
          \"permissions\": [\"cluster_observer\"]
         }">>),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      fun assert_bad_perms_or_expiry_error/1),

    file:write_file(
      US1,
      <<"{\"name\": \"john\",
          \"password\": \"fafa\",
          \"expires\": \"never\",
          \"permissions\": [\"iddqd\"]
         }">>),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      fun assert_bad_perms_or_expiry_error/1),

    NowSeconds = os:system_time(second),
    Expired = calendar:system_time_to_rfc3339(NowSeconds - 1, [{unit, second}]),
    file:write_file(
      US1,
      iolist_to_binary(
        [<<"{\"name\": \"john\",
            \"password\": \"fafa\",
            \"expires\": \"">>, Expired, <<"\",
            \"permissions\": [\"cluster_observer\"]
            }">>])),
    check_admin_cmd(
      N, ff("admin-api add-user ~s", [US1]),
      fun assert_bad_perms_or_expiry_error/1),

    ok.

assert_badjson_error(A) ->
    ["Invalid user specs"|_] = string:split(A, "\n", all).

assert_bad_perms_or_expiry_error(A) ->
    ["Invalid user permissions or expiry"|_] = string:split(A, "\n", all).


new_user_blob({Name, Password, Expires, Perms}) ->
    iolist_to_binary(
      ff("{\"name\": \"~s\",
  \"password\": \"~s\",
  \"expires\": ~s,
  \"permissions\": ~s}", [Name, Password, expires_s(Expires), perms_s(Perms)]
        )
     ).

expires_s(never) ->
    "\"never\"";
expires_s(TS) when is_integer(TS) ->
    integer_to_list(TS);
expires_s(Rfc3339String) ->
    "\"" ++ Rfc3339String ++ "\"".

perms_s(all) ->
    "\"all\"";
perms_s(Some) ->
    "[" ++ string:join(["\"" ++ atom_to_list(A) ++ "\"" || A <- Some], ",") ++ "]".


assert_user_in_clique_table("", _) -> false;
assert_user_in_clique_table(Out, UserSpec) ->
    SS = lists:nthtail(
           3,  %% skip table header
           string:split(Out, "\n", all)),
    lists:any(
      fun(S) ->
              Row = [string:strip(A, both, $ ) || A <- string:split(S, "|", all)],
              user_matches_human_printed_record(UserSpec, Row)
      end, SS
     ).

user_matches_human_printed_record({Name, _, ExpiresIn, PermsIn},
                                  [Name, _, _Created, Modified, ExpiresOut, PermsOut, "password"]) ->
    expires_matches(ExpiresIn, ExpiresOut) and perms_match(PermsIn, PermsOut)
        and recent_enough(Modified);
user_matches_human_printed_record(_, _) ->
    false.


expires_matches(never, "never") ->
    true;
expires_matches(_, "never") ->
    false;
expires_matches(never, _) ->
    false;
expires_matches(TS, Rfc) ->
    TS == calendar:rfc3339_to_system_time(Rfc, [{unit, millisecond}]).

perms_match(all, "*") ->
    true;
perms_match(all, _) ->
    false;
perms_match(_, "*") ->
    false;
perms_match(PP, SS_) ->
    lists:usort([fmtp(P) || P <- PP]) == lists:usort(string:split(SS_, ",", all)).

fmtp(cluster_admin) -> "adm";
fmtp(cluster_observer) -> "obs";
fmtp(security) -> "sec".

recent_enough(Rfc) ->
    Now = os:system_time(second),
    TS = calendar:rfc3339_to_system_time(Rfc, [{unit, second}]),
    if Now > TS ->
            Now - TS < 2;
       el/=se ->
            TS - Now < 2
    end.



check_admin_cmd(Node, Cmd, any) ->
    S = string:tokens(Cmd, " "),
    {ok, _} = rt:admin(Node, S),
    ok;
check_admin_cmd(Node, Cmd, AssertFun) when is_function(AssertFun) ->
    S = string:tokens(Cmd, " "),
    {ok, Out} = rt:admin(Node, S),
    AssertFun(Out);
check_admin_cmd(Node, Cmd, empty) ->
    S = string:tokens(Cmd, " "),
    {ok, Out} = rt:admin(Node, S),
    true = ((Out == "\nok\n") or (Out == "ok\n"));
check_admin_cmd(Node, Cmd, {re, Expect}) ->
    S = string:tokens(Cmd, " "),
    {ok, Out} = rt:admin(Node, S),
    case re:run(Out, Expect) of
        nomatch ->
            fail;
        _ ->
            ok
    end.

ff(F, A) ->
    lists:flatten(io_lib:format(F, A)).
