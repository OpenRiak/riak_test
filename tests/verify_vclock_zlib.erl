%% -------------------------------------------------------------------
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
-module(verify_vclock_zlib).
-behavior(riak_test).

-export([confirm/0]).

%% At present, the verify_vclock_<encoding> tests are functionally useless,
%% as verify_vclock:run_test/3 ignores the specified encoding.
confirm() ->
    NTestItems    = 10,     %% How many test items to write/verify?
    TestMode      = false,  %% Set to false for "production tests", true if too slow.

    verify_vclock:run_test(TestMode, NTestItems, encode_zlib).
