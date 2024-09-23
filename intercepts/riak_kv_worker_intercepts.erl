%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(riak_kv_worker_intercepts).
-compile([export_all, nowarn_export_all]).

-include_lib("kernel/include/logger.hrl").
-include("intercept.hrl").

-define(M, riak_kv_worker_orig).

%
% Okay, this is an interesting intercept.  The intention here is to insert some
% code into the point where the vnode has completed its fold, but before it invokes the finish
% command, which will inform the riak_core_vnode that handoff has completed for this node.
% This is a magic time, when handoff is running, and the fold has completed.
% In this case, we send a message to a process that is running in riak test
% (see verify_handoff_write_once, for an example), which will do a write during this
% magic time.  We wait for said process to return us an ok.
%
% The objective is to force the vnode to trigger a handle_handoff_command,
% thus exercising the runtime/forwarding handoff logic in the vnode.
%
handle_work_intercept({fold, FoldFun, FinishFun}, Sender, State) ->
    FinishWrapperFun = fun(X) ->
        catch global:send(rt_ho_w1c_proc, {write, self()}),
        receive
            ok -> ok
        end,
        FinishFun(X)
    end,
    ?M:handle_work_orig({fold, FoldFun, FinishWrapperFun}, Sender, State).

%%
%% This intercept works in tandem with the rt_kv_worker_proc module,
%% which will be informed of when work starts and when work ends.
%% Riak tests can install callbacks into the start and stop points,
%% to test various conditions.
%%
handle_work_handoff_intercept({fold, FoldFun, FinishFun}, Sender, State) ->
    FoldWrapperFun = fun() ->
        Ref = erlang:make_ref(),
        catch global:send(rt_kv_worker_proc, {work_started, {node(), Ref}, self()}),
        receive
            {Ref, ok} ->
                ok
            after 1000 ->
                ?LOG_WARNING("Timed out waiting for ok from rt_kv_worker_proc during start"),
                timeout
        end,
        FoldFun()
    end,
    FinishWrapperFun = fun(X) ->
        Ref = erlang:make_ref(),
        catch global:send(rt_kv_worker_proc, {work_completed, {node(), Ref}, self()}),
        receive
            {Ref, ok} -> ok
        after 1000 ->
            ?LOG_WARNING("Timed out waiting for ok from rt_kv_worker_proc during complete"),
            timeout
        end,
        FinishFun(X)
    end,
    ?M:handle_work_orig({fold, FoldWrapperFun, FinishWrapperFun}, Sender, State).
