%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2014 Basho Technologies, Inc.
%% Copyright (c) 2023 Workday, Inc.
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
%% @doc The `behavior' that test modules are to implement.
%%
%% Test Modules MUST export
%% ```
%%      confirm() -> pass | fail | no_return()
%% '''
%% Returning any value other than `pass' or `fail' will itself cause an
%% exception to be raised, failing the test.
%%
%% == Support API ==
%% Test modules should rely on the test API exported by the {@link rt} module
%% for operations on riak nodes and clusters.
%%
%% Common utility functions are exported from the {@link rt_util} module.
%%
%% Access to test environment metadata is provided by the
%% {@link riak_test_runner:metadata/1. riak_test_runner:metadata(Pid)}
%% function.
%% Note that this access has historically been provided by the
%% {@link riak_test_runner:metadata/0. riak_test_runner:metadata()}
%% function (and still is), but that function does not operate as you might
%% expect in all cases and <i>CAN</i> hang your test.
%%
%% <i>The way in which tests interact with their metadata environment is
%% changing in an upcoming release and while these functions will be supported
%% during a deprecation period they will be going away in their current form.
%% </i>
%%
%% == OTP Version ==
%% Test modules are <i>STRONGLY</i> discouraged from using OTP preprocessor
%% version macros. Instead, use the {@link rt:otp_release/0. rt:otp_release()}
%% and {@link rt:otp_release/1. rt:otp_release(Node)} functions, as
%% appropriate, to decide what test scenarios to execute, as it is not safe to
%% assume that the modules are compiled with the version of OTP by which
%% they're executed.
%%
%% == Riak Version ==
%% Runtime access to and evaluation of the specific versions of Riak available
%% in the test environment is provided through the {@link rt_vsn} module.
%%
%% == Logging ==
%% <i><b>NOTE:</b>
%% Testing has shown that the below narrative may not work as expected.
%% Only one test (that I know of) was using this functionality, and it hung
%% when running. In any event, removing its filtering altogether solved the
%% problem without adding any log clutter, and I don't want to dig further
%% at present, so consider this functionality "experimental".</i>
%%
%% Test modules can filter what events are logged by setting the logger
%% primary filter(s) via
%% ```
%%      logger:set_primary_config(filters, Filters)
%% '''
%% where `Filters' ia a list of filters, most easily obtained from
%% ```
%%      rt_config:logger_filters(Selectors)
%% '''
%% On entry to a test, no primary filters are applied, and any applied by the
%% test are reset upon its completion. OTOH, tests ARE NOT to change the
%% configuration of ANY existing log handlers during their execution - it's
%% too onerous to enforce this rule within the riak_test framework, but bad
%% things are likely to happen if it's violated.
%%
%% During a test's run, all of the active log handlers are configured with a
%% sane set of filters appropriate to their roles, so it's rare that a test
%% should need to restrict log events further. However, if a test causes a lot
%% of crashes of proc_lib-spawned processes (for example, supervisors or gen_*
%% processes) it may be worthwhile for it to invoke
%% ```
%%      logger:set_primary_config(filters,
%%          rt_config:logger_filters(proclib_crash))
%% '''
%% Should the need arise, tests can see what logs have been generated during
%% their own execution by calling
%% ```
%%      riak_test_logger_backend:get_logs()
%% '''
%% which returns all logged events in a chronologically-ordered list.
%%
%% @end
-module(riak_test).

%% Define the riak_test behavior
-callback confirm() -> pass | fail.
