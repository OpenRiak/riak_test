%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
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
-module(replication).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% export functions shared with other replication tests...
-export([
    add_listeners/1,
    add_site/2,
    do_write/5,
    make_bucket/3,
    replication/3,
    verify_listeners/1,
    wait_for_site_ips/3,
    wait_until_connection/1
]).

confirm() ->
    Conf = [
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {diff_batch_size, 10}
        ]}
    ],
    rt:set_advanced_conf(all, Conf),
    [ANodes, BNodes] = rt:build_clusters([3, 3]),
    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),
    replication(ANodes, BNodes, false),
    pass.

replication([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    AllNodes = ANodes ++ BNodes,

    rt:log_to_nodes(AllNodes, "Starting replication test"),

    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,
    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    NoRepl = <<TestHash/binary, "-no_repl">>,

    case Connected of
        false ->
            %% clusters are not connected, connect them

            %% write some initial data to A
            ?LOG_INFO("Writing 100 keys to ~0p", [AFirst]),
            ?assertEqual([], do_write(AFirst, 1, 100, TestBucket, 2)),

            rt:log_to_nodes(AllNodes, "Adding listeners"),
            %% setup servers/listeners on A
            Listeners = add_listeners(ANodes),
            rt:wait_until_ring_converged(ANodes),

            %% verify servers are visible on all nodes
            verify_listeners(Listeners),

            ?LOG_INFO("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, wait_until_leader_converge(ANodes)),
            ?LOG_INFO("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, wait_until_leader_converge(BNodes)),

            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),

            %% list of listeners not on the leader node
            NonLeaderListeners = lists:keydelete(LeaderA, 3, Listeners),

            rt:log_to_nodes(AllNodes, "Setup replication sites"),
            %% setup sites on B
            %% TODO: make `NumSites' an argument
            NumSites = 4,
            {Ip, Port, _} = hd(NonLeaderListeners),
            add_site(hd(BNodes), {Ip, Port, "site1"}),
            rt:wait_until_ring_converged(BNodes),
            FakeListeners = gen_fake_listeners(NumSites-1),
            add_fake_sites(BNodes, FakeListeners),
            rt:wait_until_ring_converged(BNodes),

            rt:log_to_nodes(AllNodes, "Verify replication sites"),
            %% verify sites are distributed on B
            verify_sites_balanced(NumSites, BNodes),

            wait_until_connection(LeaderA),
            %% check the listener IPs were all imported into the site
            wait_for_site_ips(BFirst, "site1", Listeners);
        _ ->
            ?LOG_INFO("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, wait_until_leader_converge(ANodes)),
            ?LOG_INFO("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),
            ?LOG_INFO("Leader on cluster A is ~0p", [LeaderA]),
            [{Ip, Port, _}|_] = get_listeners(LeaderA)
    end,

    rt:log_to_nodes(AllNodes, "Write data to A"),
    %% write some data on A
    ?assertEqual(ok, wait_until_connection(LeaderA)),
    ?LOG_DEBUG("~0p", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    ?LOG_INFO("Writing 100 more keys to ~0p", [LeaderA]),
    ?assertEqual([], do_write(LeaderA, 101, 200, TestBucket, 2)),

    rt:log_to_nodes(AllNodes, "Verify data received on B"),
    %% verify data is replicated to B
    ?LOG_INFO("Reading 100 keys written to ~0p from ~0p", [LeaderA, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    case Connected of
        false ->
            %% check that the keys we wrote initially aren't replicated yet, because
            %% we've disabled fullsync_on_connect
            ?LOG_INFO("Check keys written before repl was connected are not present"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            start_and_wait_until_fullsync_complete(LeaderA),

            ?LOG_INFO("Check keys written before repl was connected are present"),
            ?assertEqual(0, wait_for_reads(BFirst, 1, 200, TestBucket, 2));
        _ ->
            ok
    end,

    ASecond = hd(ANodes -- [LeaderA]),

    %% disconnect the other cluster, so realtime doesn't happen
    ?LOG_INFO("disconnect the 2 clusters"),
    del_site(BNodes, "site1"),
    ?assertEqual(ok, wait_until_no_connection(LeaderA)),

    ?LOG_INFO("write 2000 keys"),
    ?assertEqual([], do_write(ASecond, 50000, 52000,
            TestBucket, 2)),

    ?LOG_INFO("reconnect the 2 clusters"),
    add_site(hd(BNodes), {Ip, Port, "site1"}),
    ?assertEqual(ok, wait_until_connection(LeaderA)),

    start_and_wait_until_fullsync_complete(LeaderA),

    ?LOG_INFO("read 2000 keys"),
    ?assertEqual(0, wait_for_reads(BFirst, 50000, 52000, TestBucket, 2)),

    %%
    %% Failover tests
    %%

    rt:log_to_nodes(AllNodes, "Testing master failover: stopping ~0p", [LeaderA]),
    ?LOG_INFO("Testing master failover: stopping ~0p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

    ?LOG_INFO("New leader is ~0p", [LeaderA2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    ?LOG_INFO("Writing 100 more keys to ~0p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 201, 300, TestBucket, 2)),

    %% verify data is replicated to B
    ?LOG_INFO("Reading 100 keys written to ~0p from ~0p", [ASecond, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 201, 300, TestBucket, 2)),

    %% get the leader for the first cluster
    LeaderB = rpc:call(BFirst, riak_repl_leader, leader_node, []),

    ?LOG_INFO("Testing client failover: stopping ~0p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_repl_leader, leader_node, []),

    ?LOG_INFO("New leader is ~0p", [LeaderB2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    ?LOG_INFO("Writing 100 more keys to ~0p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 301, 400, TestBucket, 2)),

    %% verify data is replicated to B
    rt:wait_until_pingable(BSecond),
    ?LOG_INFO("Reading 101 keys written to ~0p from ~0p", [ASecond, BSecond]),
    ?assertEqual(0, wait_for_reads(BSecond, 301, 400, TestBucket, 2)),

    %% Testing fullsync with downed nodes
    ?LOG_INFO("Re-running fullsync with ~0p and ~0p down", [LeaderA, LeaderB]),

    start_and_wait_until_fullsync_complete(LeaderA2),

    %%
    %% Per-bucket repl settings tests
    %%

    ?LOG_INFO("Restarting down node ~0p", [LeaderA]),
    rt:start(LeaderA),
    rt:wait_until_pingable(LeaderA),
    rt:wait_until_no_pending_changes(ANodes),
    wait_until_leader_converge(ANodes),
    start_and_wait_until_fullsync_complete(LeaderA2),

    case nodes_all_have_version(ANodes, "1.2.2") of
        true ->

            ?LOG_INFO("Starting Joe's Repl Test"),

            %% At this point, realtime sync should still work, but, it doesn't
            %% because of a bug in 1.2.1.
            %% Check that repl leader is LeaderA
            %% Check that LeaderA2 has ceeded socket back to LeaderA

            ?LOG_INFO("Leader: ~0p", [rpc:call(ASecond, riak_repl_leader, leader_node, [])]),
            ?LOG_INFO("LeaderA: ~0p", [LeaderA]),
            ?LOG_INFO("LeaderA2: ~0p", [LeaderA2]),

            ?assertEqual(ok, wait_until_connection(LeaderA)),

            ?LOG_INFO("Simulation partition to force leader re-election"),

            PInfo = rt:partition([LeaderA2], ANodes -- [LeaderA2]),

            ?LOG_INFO("Waiting for new leader"),
            wait_until_new_leader(hd(ANodes -- [LeaderA2]), LeaderA2),
            InterimLeader = rpc:call(LeaderA, riak_repl_leader, leader_node, []),
            ?LOG_INFO("Interim leader: ~0p", [InterimLeader]),

            ?LOG_INFO("Heal partition"),

            rt:heal(PInfo),

            ?LOG_INFO("Wait for leader convergence"),
            %% there's no point in writing anything until the leaders
            %% converge, as we can drop writes in the middle of an election
            wait_until_leader_converge(ANodes),

            LeaderA3 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

            wait_until_connection(LeaderA3),

            ?LOG_INFO("Leader: ~0p", [LeaderA3]),
            ?LOG_INFO("Writing 2 more keys to ~0p", [LeaderA3]),
            ?assertEqual([], do_write(LeaderA3, 1301, 1302, TestBucket, 2)),

            %% verify data is replicated to B
            ?LOG_INFO("Reading 2 keys written to ~0p from ~0p", [LeaderA3, BSecond]),
            ?assertEqual(0, wait_for_reads(BSecond, 1301, 1302, TestBucket, 2)),

            ?LOG_INFO("Finished Joe's Section"),

            ?LOG_INFO("Nodes restarted");
        _ ->
            ?LOG_INFO("Skipping Joe's Repl Test")
    end,

    ?LOG_INFO("Restarting down node ~0p", [LeaderB]),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderB),

    case nodes_all_have_version(ANodes, "1.1.0") of
        true ->

            make_bucket(ANodes, NoRepl, [{repl, false}]),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    make_bucket(ANodes, RealtimeOnly, [{repl, realtime}]),
                    make_bucket(ANodes, FullsyncOnly, [{repl, fullsync}]),

                    %% disconnect the other cluster, so realtime doesn't happen
                    ?LOG_INFO("disconnect the 2 clusters"),
                    del_site(BNodes, "site1"),
                    ?assertEqual(ok, wait_until_no_connection(LeaderA)),

                    ?LOG_INFO("write 100 keys to a realtime only bucket"),
                    ?assertEqual([], do_write(ASecond, 1, 100,
                            RealtimeOnly, 2)),

                    ?LOG_INFO("reconnect the 2 clusters"),
                    add_site(LeaderB, {Ip, Port, "site1"}),
                    ?assertEqual(ok, wait_until_connection(LeaderA));
                _ ->
                    timer:sleep(1000)
            end,

            LeaderA4 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

            ?LOG_INFO("write 100 keys to a {repl, false} bucket"),
            ?assertEqual([], do_write(ASecond, 1, 100, NoRepl, 2)),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    ?LOG_INFO("write 100 keys to a fullsync only bucket"),
                    ?assertEqual([], do_write(ASecond, 1, 100,
                            FullsyncOnly, 2)),

                    ?LOG_INFO("Check the fullsync only bucket didn't replicate the writes"),
                    Res6 = rt:systest_read(BSecond, 1, 100, FullsyncOnly, 2),
                    ?assertEqual(100, length(Res6)),

                    ?LOG_INFO("Check the realtime only bucket that was written to offline "
                        "isn't replicated"),
                    Res7 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res7));
                _ ->
                    timer:sleep(1000)
            end,

            ?LOG_INFO("Check the {repl, false} bucket didn't replicate"),
            Res8 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
            ?assertEqual(100, length(Res8)),

            %% do a fullsync, make sure that fullsync_only is replicated, but
            %% realtime_only and no_repl aren't
            start_and_wait_until_fullsync_complete(LeaderA4),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    ?LOG_INFO("Check fullsync only bucket is now replicated"),
                    ?assertEqual(0, wait_for_reads(BSecond, 1, 100,
                            FullsyncOnly, 2)),

                    ?LOG_INFO("Check realtime only bucket didn't replicate"),
                    Res10 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res10)),


                    ?LOG_INFO("Write 100 more keys into realtime only bucket"),
                    ?assertEqual([], do_write(ASecond, 101, 200,
                            RealtimeOnly, 2)),

                    timer:sleep(5000),

                    ?LOG_INFO("Check the realtime keys replicated"),
                    ?assertEqual(0, wait_for_reads(BSecond, 101, 200,
                                RealtimeOnly, 2)),

                    ?LOG_INFO("Check the older keys in the realtime bucket did not replicate"),
                    Res12 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res12));
                _ ->
                    ok
            end,

            ?LOG_INFO("Check {repl, false} bucket didn't replicate"),
            Res13 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
            ?assertEqual(100, length(Res13));
        _ ->
            ok
    end,

    ?LOG_INFO("Test passed"),
    fin.

verify_sites_balanced(NumSites, BNodes0) ->
    Leader = rpc:call(hd(BNodes0), riak_repl_leader, leader_node, []),
    case node_has_version(Leader, "1.2.0") of
        true ->
            BNodes = nodes_with_version(BNodes0, "1.2.0") -- [Leader],
            NumNodes = length(BNodes),
            case NumNodes of
                0 ->
                    %% only leader is upgraded, runs clients locally
                    ?assertEqual(NumSites, client_count(Leader));
                _ ->
                    NodeCounts = [{Node, client_count(Node)} || Node <- BNodes],
                    ?LOG_NOTICE("nodecounts ~0p", [NodeCounts]),
                    ?LOG_NOTICE("leader ~0p", [Leader]),
                    Min = NumSites div NumNodes,
                    [?assert(Count >= Min) || {_Node, Count} <- NodeCounts]
            end;
        false ->
            ok
    end.

%% does the node meet the version requirement?
node_has_version(Node, Version) ->
    {_, NodeVersion} =  rpc:call(Node, init, script_id, []),
    case NodeVersion of
        current ->
            %% current always satisfies any version check
            true;
        _ ->
            NodeVersion >= Version
    end.

nodes_with_version(Nodes, Version) ->
    [Node || Node <- Nodes, node_has_version(Node, Version)].

nodes_all_have_version(Nodes, Version) ->
    Nodes == nodes_with_version(Nodes, Version).

client_count(Node) ->
    Clients = rpc:call(Node, supervisor, which_children, [riak_repl_client_sup]),
    length(Clients).

gen_fake_listeners(Num) ->
    Ports = gen_ports(11000, Num),
    IPs = lists:duplicate(Num, "127.0.0.1"),
    Nodes = [fake_node(N) || N <- lists:seq(1, Num)],
    lists:zip3(IPs, Ports, Nodes).

fake_node(Num) ->
    lists:flatten(io_lib:format("fake~b@127.0.0.1", [Num])).

add_fake_sites([Node|_], Listeners) ->
    [add_site(Node, {IP, Port, fake_site(Port)})
     || {IP, Port, _} <- Listeners].

add_site(Node, {IP, Port, Name}) ->
    ?LOG_INFO("Add site ~0p ~0p:~b at node ~0p", [Name, IP, Port, Node]),
    Args = [IP, integer_to_list(Port), Name],
    Res = rpc:call(Node, riak_repl_console, add_site, [Args]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

del_site([Node|_]=Nodes, Name) ->
    ?LOG_INFO("Del site ~0p at ~0p", [Name, Node]),
    Res = rpc:call(Node, riak_repl_console, del_site, [[Name]]),
    ?assertEqual(ok, Res),
    rt:wait_until_ring_converged(Nodes),
    timer:sleep(timer:seconds(5)).

fake_site(Port) ->
    lists:flatten(io_lib:format("fake_site_~b", [Port])).

verify_listeners(Listeners) ->
    Strs = [IP ++ ":" ++ integer_to_list(Port) || {IP, Port, _} <- Listeners],
    [?assertEqual(ok, verify_listener(Node, Strs)) || {_, _, Node} <- Listeners].

verify_listener(Node, Strs) ->
    ?LOG_INFO("Verify listeners ~0p ~0p", [Node, Strs]),
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                lists:all(fun(Str) ->
                            lists:keymember(Str, 2, Status)
                    end, Strs)
        end).

add_listeners(Nodes=[FirstNode|_]) ->
    Ports = gen_ports(9010, length(Nodes)),
    IPs = lists:duplicate(length(Nodes), "127.0.0.1"),
    PN = lists:zip3(IPs, Ports, Nodes),
    [add_listener(FirstNode, Node, IP, Port) || {IP, Port, Node} <- PN],
    timer:sleep(timer:seconds(5)),
    PN.

add_listener(N, Node, IP, Port) ->
    ?LOG_INFO("Adding repl listener to ~0p ~s:~b", [Node, IP, Port]),
    Args = [[atom_to_list(Node), IP, integer_to_list(Port)]],
    Res = rpc:call(N, riak_repl_console, add_listener, Args),
    ?assertEqual(ok, Res).

get_listeners(Node) ->
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    %% *sigh*
    [
        begin
                NodeName = list_to_atom(string:substr(K, 10)),
                [IP, Port] = string:tokens(V, ":"),
                {IP, list_to_integer(Port), NodeName}
        end || {K, V} <- Status, is_list(K), string:substr(K, 1, 9) == "listener_"
    ].

gen_ports(Start, Len) ->
    lists:seq(Start, Start + Len - 1).

wait_for_site_ips(Leader, Site, Listeners) ->
    rt:wait_until(verify_site_ips_fun(Leader, Site, Listeners)).

verify_site_ips_fun(Leader, Site, Listeners) ->
    fun() ->
            Status = rpc:call(Leader, riak_repl_console, status, [quiet]),
            Key = lists:flatten([Site, "_ips"]),
            IPStr = proplists:get_value(Key, Status),
            ?LOG_INFO("IPSTR: ~0p", [IPStr]),
            IPs = lists:sort(re:split(IPStr, ", ")),
            ExpectedIPs = lists:sort(
                            [list_to_binary([IP, ":", integer_to_list(Port)]) || {IP, Port, _Node} <-
                                                                                     Listeners]),
            ?LOG_INFO("ExpectedIPs: ~0p IPs: ~0p", [ExpectedIPs, IPs]),
            ExpectedIPs =:= IPs
    end.

make_bucket([Node|_]=Nodes, Name, Args) ->
    Res = rpc:call(Node, riak_core_bucket, set_bucket, [Name, Args]),
    rt:wait_until_ring_converged(Nodes),
    ?assertEqual(ok, Res).

start_and_wait_until_fullsync_complete(Node) ->
    start_and_wait_until_fullsync_complete(Node, 20).

start_and_wait_until_fullsync_complete(Node, Retries) ->
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0) + 1,
    ?LOG_INFO("waiting for fullsync count to be ~0p", [Count]),

    ?LOG_INFO("Starting fullsync on ~0p (~0p)",
        [Node, rt:get_node_version(Node)]),
    rpc:call(Node, riak_repl_console, start_fullsync, [[]]),

    %% sleep because of the old bug where stats will crash if you call it too
    %% soon after starting a fullsync
    timer:sleep(500),

    case rt:wait_until(make_fullsync_wait_fun(Node, Count), 100, 1000) of
        ok ->
            ok;
        _  when Retries > 0 ->
            ?assertEqual(ok, wait_until_connection(Node)),
            ?LOG_WARNING("Node failed to fullsync, retrying"),
            start_and_wait_until_fullsync_complete(Node, Retries-1)
    end,
    ?LOG_INFO("Fullsync on ~0p complete", [Node]).

make_fullsync_wait_fun(Node, Count) ->
    fun() ->
            Status = rpc:call(Node, riak_repl_console, status, [quiet]),
            case Status of
                {badrpc, _} ->
                    false;
                _ ->
                    case proplists:get_value(server_fullsyncs, Status) of
                        C when C >= Count ->
                            true;
                        _ ->
                            false
                    end
            end
    end.

wait_until_leader(Node) ->
    wait_until_new_leader(Node, undefined).

wait_until_new_leader(Node, OldLeader) ->
    Res = rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(leader, Status) of
                            undefined ->
                                false;
                            OldLeader ->
                                false;
                            _Other ->
                                true
                        end
                end
        end),
    ?assertEqual(ok, Res).

wait_until_leader_converge([Node|_] = Nodes) ->
    rt:wait_until(Node,
        fun(_) ->
                LeaderResults =
                    [get_leader(rpc:call(N, riak_repl_console, status, [quiet])) ||
                        N <- Nodes],
                {Leaders, Errors} =
                    lists:partition(leader_result_filter_fun(), LeaderResults),
                UniqueLeaders = lists:usort(Leaders),
                Errors == [] andalso length(UniqueLeaders) == 1
        end).

get_leader({badrpc, _}=Err) ->
    Err;
get_leader(Status) ->
    case proplists:get_value(leader, Status) of
        undefined ->
            false;
        L ->
            ?LOG_DEBUG("Leader is ~0p", [L]),
            L
    end.

leader_result_filter_fun() ->
    fun(L) ->
            case L of
                undefined ->
                    false;
                {badrpc, _} ->
                    false;
                _ ->
                    true
            end
    end.

wait_until_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            [] ->
                                false;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                true;
                            Conns ->
                                ?LOG_WARNING("multiple connections detected: ~0p",
                                    [Conns]),
                                true
                        end
                end
        end). %% 40 seconds is enough for repl

wait_until_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            [] ->
                                true;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                false;
                            Conns ->
                                ?LOG_WARNING("multiple connections detected: ~0p",
                                    [Conns]),
                                false
                        end
                end
        end). %% 40 seconds is enough for repl


wait_for_reads(Node, Start, End, Bucket, R) ->
    rt:wait_until(Node,
        fun(_) ->
                rt:systest_read(Node, Start, End, Bucket, R) == []
        end),
    Reads = rt:systest_read(Node, Start, End, Bucket, R),
    ?LOG_INFO("Reads: ~0p", [Reads]),
    length(Reads).

do_write(Node, Start, End, Bucket, W) ->
    case rt:systest_write(Node, Start, End, Bucket, W) of
        [] ->
            [];
        Errors ->
            ?LOG_WARNING("~b errors while writing: ~0p",
                [length(Errors), Errors]),
            timer:sleep(1000),
            lists:flatten([rt:systest_write(Node, S, S, Bucket, W) ||
                    {S, _Error} <- Errors])
    end.
