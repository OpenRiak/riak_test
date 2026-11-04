%% -------------------------------------------------------------------
%%
%% Copyright (c) 2023 Workday, Inc.
%%
%%-------------------------------------------------------------------

-module(riak_repl_fullsync_helper_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

-define(M, riak_repl_fullsync_helper_orig).

count_is_fullsync_replicated({Bucket, Key}) when is_binary(Bucket) ->
    count_is_fullsync_replicated({{<<"default">>, Bucket}, Key});
count_is_fullsync_replicated({_Bucket, _Key} = BKey) ->
    IsReplicated = ?M:is_fullsync_replicated_orig(BKey),
    Ref = erlang:make_ref(),
    global:send(verify_repl_filter, {is_fullsync_replicated, {self(), Ref}, BKey, IsReplicated}),
    receive
        {Ref, ok} ->
            ok
    after 1000 ->
        error(timeout_waiting_repl_filter_reply)
    end,
    IsReplicated.
