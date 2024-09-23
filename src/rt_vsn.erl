%% -------------------------------------------------------------------
%%
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
%% @doc Functions for version parsing and comparison.
%%
%% The motivation for this module is finding Riak versions in the configured
%% RT staging tree that do or don't have features introduced at a specific
%% version in order to determine what test scenarios are supported.
%%
%% The {@link parse_version/1} function, though, can digest almost any
%% representation of a semver-ish version string, including those produced by
%% `git describe' and Rebar, as well as those found in devrel VERSION files,
%% into a version representation that is strictly ordered and efficiently
%% comparable, should the need arise to compare library, application, or other
%% versions.
%%
%% @todo The version parsing and comparison functions might belong in Riak.
%%
%% @end
%%
-module(rt_vsn).

-compile(inline).

% Public API
-export([
    compare_versions/2,
    configured_versions/0,
    find_version_at_least/1, find_version_at_least/2,
    find_version_before/1, find_version_before/2,
    new_version/1,
    parse_version/1,
    tagged_version/1,
    version_to_string/1
]).

-include_lib("kernel/include/logger.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

%% A comparable, strictly-ordered version representation.
%% MUST be kept in sync with `rtt:vsn_rec()'
-record(rtv, {
    vsn :: nonempty_list(non_neg_integer())
}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec compare_versions(Left :: rtt:any_vsn(), Right :: rtt:any_vsn())
        -> integer().
%% @doc Compares two versions' semver representations and returns an integer
%%      result indicator.
%%
%% @returns <dl>
%%  <dt>`= 0'</dt><dd>`semver(Left) =:= semver(Right)'.</dd>
%%  <dt>`< 0'</dt><dd>`semver(Left) < semver(Right)'.</dd>
%%  <dt>`> 0'</dt><dd>`semver(Left) > semver(Right)'.</dd>
%% </dl>
compare_versions(#rtv{vsn = Same}, #rtv{vsn = Same}) ->
    0;
compare_versions(#rtv{vsn = Left}, #rtv{vsn = Right}) ->
    compare_version_vals(Left, Right);
compare_versions(Vsn, Vsn) ->
    0;
compare_versions(Left, Right) ->
    compare_versions(parse_version(Left), parse_version(Right)).

-spec configured_versions() -> list(rtt:cfg_vsn()).
%% @doc Returns an ordered list of the versions configured in
%%      `riak_test.config'.
%%
%% The returned  list is ordered by descending version, and picks up
%% <i>ALL</i> versions returned by {@link rt:versions()} whose explicit
%% version (semver) can be ascertained, not just `current/previous/legacy'.
configured_versions() ->
    configured_tagged_versions(rt:versions(), []).

-spec find_version_at_least(TargetVsn :: rtt:any_vsn())
        -> rtt:cfg_vsn() | false.
%% @doc Return the first version (by priority) in {@link configured_versions()}
%%      that is `>= TargetVsn'.
find_version_at_least(TargetVsn) ->
    find_version_at_least(TargetVsn, configured_versions()).

-spec find_version_at_least(
    TargetVsn :: rtt:any_vsn(), Versions :: list(rtt:cfg_vsn()))
        -> rtt:cfg_vsn() | false.
%% @doc Return the first version (by priority) in `Versions'
%%      that is `>= TargetVsn'.
find_version_at_least(_TargetVsn, []) ->
    false;
find_version_at_least(
    #rtv{} = TargetVsn, [{current, #rtv{} = CurVsn} = CurVsnRec | Vsns]) ->
    % In most scenarios, this should be the most common case.
    case compare_versions(CurVsn, TargetVsn) of
        I when I >= 0 ->
            CurVsnRec;
        _ ->
            find_version_at_least(TargetVsn, Vsns)
    end;
find_version_at_least(
    #rtv{} = TargetVsn, [{_HdTag, #rtv{} = HdVsn} = HdVsnRec | Vsns]) ->
    % If we fell through to here, it likely means that we're dealing with
    % some weird config where either a) 'current' isn't at the head of the
    % list OR b) 'current' isn't the highest version OR c) the list isn't
    % ordered as returned by configured_versions/0.
    % Prefer 'current' if it meets the predicate.
    case compare_versions(HdVsn, TargetVsn) of
        I when I >= 0 ->
            case lists:keyfind(current, 1, Vsns) of
                {current, #rtv{} = CurVsn} = CurVsnRec ->
                    case compare_versions(CurVsn, TargetVsn) of
                        I when I >= 0 ->
                            CurVsnRec;
                        _ ->
                            HdVsnRec
                    end;
                _ ->
                    HdVsnRec
            end;
        _ ->
            find_version_at_least(TargetVsn, Vsns)
    end;
find_version_at_least(#rtv{} = TargetVsn, Versions) ->
    %% Versions isn't a [proper] list.
    erlang:error(badarg, [TargetVsn, Versions]);
find_version_at_least(TargetVsn, Versions) ->
    find_version_at_least(parse_version(TargetVsn), Versions).

-spec find_version_before(TargetVsn :: rtt:any_vsn())
        -> rtt:cfg_vsn() | false.
%% @doc Return the first version in {@link configured_versions()} that is
%%      `< TargetVsn'.
find_version_before(TargetVsn) ->
    find_version_before(TargetVsn, configured_versions()).

-spec find_version_before(
    TargetVsn :: rtt:any_vsn(), Versions :: list(rtt:cfg_vsn()))
        -> rtt:cfg_vsn() | false.
%% @doc Return the first version in `Versions' that is `< TargetVsn'.
find_version_before(_TargetVsn, []) ->
    false;
find_version_before(
    #rtv{} = TargetVsn, [{_HdTag, #rtv{} = HdVsn} = HdVsnRec | Vsns]) ->
    case compare_versions(HdVsn, TargetVsn) of
        I when I < 0 ->
            HdVsnRec;
        _ ->
            find_version_before(TargetVsn, Vsns)
    end;
find_version_before(#rtv{} = TargetVsn, Versions) ->
    erlang:error(badarg, [TargetVsn, Versions]);
find_version_before(TargetVsn, Versions) ->
    find_version_before(parse_version(TargetVsn), Versions).

-spec new_version(nonempty_list(non_neg_integer())) -> rtt:vsn_rec().
%% @doc Create a {@link rtt:vsn_rec()} from a list of integers.
%%
%% Note that this function will happily create a version from a string,
%% because it's a list of integers, but that's probably not what you want
%% - use {@link parse_version/1} for that.
new_version(Ints) when erlang:is_list(Ints) andalso erlang:length(Ints) > 0 ->
    case lists:all(fun(I) -> erlang:is_integer(I) andalso I >= 0 end, Ints) of
        true ->
            #rtv{vsn = Ints};
        _ ->
            erlang:error(badarg, Ints)
    end;
new_version(Arg) ->
    erlang:error(badarg, [Arg]).

-spec parse_version(VsnIn :: rtt:any_vsn()) -> rtt:vsn_rec() | no_return().
%% @doc Parses [almost] anything into a {@link rtt:vsn_rec()}.
%% @end
%%  This is likely overkill within riak_test, but I already had the tested
%%  code so just lifting it verbatim.
parse_version(#rtv{} = VsnIn) ->
    VsnIn;
parse_version({vsn, VsnIn})
    when erlang:is_list(VsnIn) orelse erlang:is_binary(VsnIn) ->
    parse_version(VsnIn);
parse_version(VsnIn) when erlang:is_binary(VsnIn) ->
    parse_version(binary:bin_to_list(VsnIn));
parse_version(VsnIn) when erlang:is_list(VsnIn) ->
    Str = case string:trim(lists:flatten(VsnIn)) of
        % Handle the case of a exactly one leading 'v' before the first digit.
        [$v | [C | _] = Rest] when C >= $0 andalso C =< $9 ->
            Rest;
        VS ->
            VS
    end,
    % It's tempting to use lists:foldr/2, assuming only a few segments, but
    % we really don't know what may be passed in so play it safe.
    parse_version(string:lexemes(Str, ".-+"), []);
parse_version(VsnIn) ->
    erlang:error(badarg, [VsnIn]).

-spec version_to_string(Vsn :: rtt:vsn_rec()) -> rtt:sem_vsn().
%% @doc Returns the dotted decimal (semver) string representation of a
%%      {@link rtt:vsn_rec()}.
version_to_string(#rtv{vsn = Vsn}) ->
    lists:flatten(lists:join(".", [erlang:integer_to_list(Seg) || Seg <- Vsn]));
version_to_string(Arg) ->
    erlang:error(badarg, [Arg]).

-spec tagged_version(Tag :: rtt:vsn_tag())
        -> {rtt:vsn_tag(), rtt:vsn_rec()} | undefined | unknown.
%% @doc Returns the version associated with `Tag' in a tuple,
%%      or a failure indicator.
%%
%% @returns <ul>
%%  <li>`undefined' if `Tag' isn't found by `rt:get_vsn_rec(Tag)'.</li>
%%  <li>`unknown' if `rt:get_vsn_rec(Tag)' can't determine the version.</li>
%% </ul>
tagged_version(Tag) ->
    try rt:get_vsn_rec(Tag) of
        #rtv{} = Vsn ->
            {Tag, Vsn};
        Unknown ->
            Unknown
    catch
        _:_ ->
            undefined
    end.

%% ===================================================================
%% Internal
%% ===================================================================

-spec compare_version_tags(Left :: rtt:vsn_tag(), Right :: rtt:vsn_tag())
        -> integer().
%% @hidden Version tag priority comparison
compare_version_tags(Tag, Tag) -> 0;
% Impose explicit order on the known tags
compare_version_tags(current, _) -> 1;
compare_version_tags(_, current) -> -1;
compare_version_tags(previous, _) -> 1;
compare_version_tags(_, previous) -> -1;
compare_version_tags(legacy, _) -> 1;
compare_version_tags(_, legacy) -> -1;
% Compare like kinds directly
compare_version_tags(L, R)
    when erlang:is_atom(L) andalso erlang:is_atom(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_atom(L) andalso erlang:is_atom(R) andalso L < R -> -1;
compare_version_tags(L, R)
    when erlang:is_list(L) andalso erlang:is_list(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_list(L) andalso erlang:is_list(R) andalso L < R -> -1;
compare_version_tags(L, R)
    when erlang:is_binary(L) andalso erlang:is_binary(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_binary(L) andalso erlang:is_binary(R) andalso L < R -> -1;
% For everything else apply a priority before comparison
compare_version_tags(L, R) ->
    {PL, VL} = compare_prioritize(L),
    {PR, VR} = compare_prioritize(R),
    if
        VL > VR -> 1;
        VL < VR -> -1;
        %% Must be equal values, but from different source types.
        %% Compare priorities, which are in descending order.
        true -> (PR - PL)
    end.

-spec compare_prioritize(Tag :: term()) -> {integer(), string()}.
%% @hidden compare_version_tags helper
compare_prioritize(Tag) when erlang:is_atom(Tag) ->
    {1, erlang:atom_to_list(Tag)};
compare_prioritize(Tag) when erlang:is_list(Tag) ->
    {3, Tag};
compare_prioritize(Tag) when erlang:is_binary(Tag) ->
    {5, binary:bin_to_list(Tag)};
compare_prioritize(Tag) ->
    {9, lists:flatten(io_lib:format("~0p", [Tag]))}.

-spec compare_version_vals(
    Left :: list(non_neg_integer()), Right :: list(non_neg_integer())) -> integer().
%% @hidden Integer list comparison
compare_version_vals([], []) ->
    0;
compare_version_vals([], _) ->
    -1;
compare_version_vals(_, []) ->
    1;
compare_version_vals([I], [I]) ->
    0;
compare_version_vals([I | Left], [I | Right]) ->
    compare_version_vals(Left, Right);
compare_version_vals([L | _], [R | _]) ->
    (L - R).

-spec configured_tagged_versions(
    Tags :: list(rtt:vsn_tag()), Result :: list(rtt:cfg_vsn()) )
        -> list(rtt:cfg_vsn()).
%% @hidden Convert Tags into an ordered list of tagged explicit versions.
configured_tagged_versions([], Result) ->
    lists:reverse(lists:sort(fun compare_tagged_versions/2, Result));
configured_tagged_versions([Tag | Tags], Result) ->
    case tagged_version(Tag) of
        {_Tag, #rtv{}} = TV ->
            configured_tagged_versions(Tags, [TV | Result]);
        NoVer ->
            % Drop anything without a version
            ?LOG_WARNING("Tag ~0p has no explicit version: ~0p", [Tag, NoVer]),
            configured_tagged_versions(Tags, Result)
    end.

-spec compare_tagged_versions(
    L :: {rtt:vsn_tag(), #rtv{}}, R :: {rtt:vsn_tag(), #rtv{}}) -> boolean().
%% @hidden lists:sort/2 comparator
compare_tagged_versions({LT, #rtv{vsn = LV}}, {RT, #rtv{vsn = RV}}) ->
    case compare_version_vals(LV, RV) of
        0 ->
            compare_version_tags(LT, RT) =< 0;
        N ->
            N < 0
    end.

-spec parse_version(
    Segments :: list(string()), Result :: list(non_neg_integer())) -> #rtv{}.
%% @hidden The safe way to handle any number of segments.
parse_version([], Result) ->
    #rtv{vsn = lists:reverse(Result)};
parse_version([Segment | Segments], Result) ->
    case string:to_integer(Segment) of
        {error, _} ->
            parse_version(Segments, Result);
        {Seg, _} ->
            parse_version(Segments, [Seg | Result])
    end.

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

%% Records look like:
%%  {[N1,N2...Nx], ["N1.N2...Nx" | StringRepresentations]}
%% in ascending order
-define(VSN_DATA, [
    {[1,2,3],   ["1.2.3",   "foo-1.2-tag-3-suffix"]},
    {[1,2,3,4], ["1.2.3.4", "foo-1.2-tag-3.rc-4", "foo-1.2.3-patch-4"]},
    {[2,3],     ["2.3",     "v2.3"]}
]).

new_version_test() ->
    ?assertEqual({rtv, [1,2]},          new_version([1,2])),
    ?assertEqual({rtv, [3,4,5]},        new_version([3,4,5])),
    ?assertEqual({rtv, [7,0,8,0,9]},    new_version([7,0,8,0,9])).

parse_version_test() ->
    lists:foreach(
        fun({IntVsn, StrVsns}) ->
            Vsn = new_version(IntVsn),
            lists:foreach(
                fun(StrVsn) ->
                    ?assertEqual(Vsn, parse_version(StrVsn))
                end, StrVsns)
        end, ?VSN_DATA).

version_to_string_test() ->
    lists:foreach(
        fun({IntVsn, [StrVsn | _]}) ->
            Vsn = new_version(IntVsn),
            ?assertEqual(StrVsn, version_to_string(Vsn))
        end, ?VSN_DATA).

compare_versions_1_test() ->
    lists:foreach(
        fun({IntVsn, StrVsns}) ->
            Vsn = new_version(IntVsn),
            lists:foreach(
                fun(StrVsn) ->
                    ?assertEqual(0, compare_versions(Vsn, StrVsn)),
                    ?assertEqual(0, compare_versions(StrVsn, StrVsn))
                end, StrVsns)
        end, ?VSN_DATA).

compare_versions_2_test() ->
    [LoVsn, MidVsn, HiVsn | _] = [new_version(I) || {I, _} <- ?VSN_DATA],
    ?assert(compare_versions(LoVsn, MidVsn) < 0),
    ?assert(compare_versions(LoVsn, HiVsn) < 0),
    ?assert(compare_versions(MidVsn, LoVsn) > 0),
    ?assert(compare_versions(HiVsn, MidVsn) > 0).

compare_version_tags_test() ->
    ?assert(compare_version_tags('current', "10") > 0),
    ?assert(compare_version_tags('legacy', '9.9.9') > 0),
    ?assert(compare_version_tags("9.9.9", '9.9.9') < 0).

-endif. % TEST
