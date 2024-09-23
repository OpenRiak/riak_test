%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2015 Basho Technologies, Inc.
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

-module(rt_util).
-include_lib("eunit/include/eunit.hrl").

-export([
    convert_to_atom/1,
    convert_to_atom_list/1,
    convert_to_string/1,
    find_atom_or_string/2,
    find_atom_or_string_dict/2,
    parse_term/1,
    random_boolean/0,
    random_element/1,
    shuffle/1
]).

%% @doc Look up values by both atom and by string
-spec find_atom_or_string(term(), proplists:proplist()) -> term() | undefined.
find_atom_or_string(Key, Table) ->
    case {Key, proplists:get_value(Key, Table)} of
        {_, undefined} when is_atom(Key) ->
            proplists:get_value(atom_to_list(Key), Table);
        {_, undefined} when is_list(Key) ->
            proplists:get_value(list_to_atom(Key), Table);
        {Key, Value} ->
            Value
    end.

%% @doc Look up values in an orddict by both atom and by string
-spec find_atom_or_string_dict(term(), orddict:orddict()) -> term() | undefined.
find_atom_or_string_dict(Key, Dict) ->
    case {Key, orddict:is_key(Key, Dict)} of
        {_, false} when is_atom(Key) ->
            case orddict:is_key(atom_to_list(Key), Dict) of
                true -> orddict:fetch(atom_to_list(Key), Dict);
                _ -> undefined
            end;
        {_, false} when is_list(Key) ->
            case orddict:is_key(list_to_atom(Key), Dict) of
                true -> orddict:fetch(list_to_atom(Key), Dict);
                _ -> undefined
            end;
        {_, true} ->
            orddict:fetch(Key, Dict)
    end.

%% @doc Convert an atom to a string if it is not already
-spec convert_to_string(string()|atom()) -> string().
convert_to_string(Val) when is_atom(Val) ->
    atom_to_list(Val);
convert_to_string(Val) when is_list(Val) ->
    Val.

%% @doc Convert a string to an atom, otherwise return unchanged.
-spec convert_to_atom(term()) -> term().
convert_to_atom(Val) when is_list(Val) ->
    list_to_atom(Val);
convert_to_atom(Val) ->
    Val.

%% @doc Convert string or atom to list of atoms
-spec convert_to_atom_list(atom()|string()) -> undefined | list().
convert_to_atom_list(undefined) ->
    undefined;
convert_to_atom_list(Values) when is_atom(Values) ->
    ListOfValues = atom_to_list(Values),
    case lists:member($, , ListOfValues) of
        true ->
            [list_to_atom(X) || X <- string:tokens(ListOfValues, ", ")];
        _ ->
            [Values]
    end;
convert_to_atom_list(Values) when is_list(Values) ->
    case lists:member($, , Values) of
        true ->
            [list_to_atom(X) || X <- string:tokens(Values, ", ")];
        _ ->
            [list_to_atom(Values)]
    end.

%% @doc Given a string (or binary) containing the text representation of a
%% single constant term, returns the represented term or an error if it
%% cannot be parsed.
-spec parse_term(String :: nonempty_string() | binary())
        -> {ok, term()} | rtt:std_error().
parse_term([_|_] = Str) ->
    case io_lib:char_list(Str) of
        true ->
            parse_term_str(Str);
        _ ->
            case io_lib:deep_char_list(Str) of
                true ->
                    parse_term_str(lists:flatten(Str));
                _ ->
                    {error, {badarg, [Str]}}
            end
    end;
parse_term(Bin) when erlang:is_binary(Bin) andalso erlang:size(Bin) > 0 ->
    Str = erlang:binary_to_list(Bin),
    case io_lib:char_list(Str) of
        true ->
            parse_term_str(Str);
        _ ->
            {error, {badarg, [Bin]}}
    end;
parse_term(Arg) ->
    {error, {badarg, [Arg]}}.

%% @hidden
-spec parse_term_str(String :: nonempty_string())
        -> {ok, term()} | rtt:std_error().
parse_term_str(Str) ->
    case string:trim(Str) of
        [] ->
            {error, {badarg, [Str]}};
        "." ->
            {error, {badarg, [Str]}};
        Trimmed ->
            TermStr = case string:find(Trimmed, ".", trailing) of
                "." ->
                    Trimmed;
                _ ->
                    Trimmed ++ "."
            end,
            case erl_scan:string(TermStr) of
                {ok, Toks, _} ->
                    case erl_parse:parse_term(Toks) of
                        {ok, _Term} = Result ->
                            Result;
                        ParseErr ->
                            ParseErr
                    end;
                {error, ScanInfo, ScanLoc} ->
                    {error, {ScanInfo, ScanLoc}}
            end
    end.

%% @doc Return elements of a list shuffled randomly.
%% This function is implemented to achieve as random a distribution as can
%% be had with any of the `rand' algorithms as well as `crypto:rand_seed()'.
-spec shuffle(list()) -> list().
shuffle([] = List) ->
    List;
shuffle([_] = List) ->
    List;
shuffle([_|_] = List) ->
    %% rand:uniform/1 is a bit more efficient than rand:uniform/0, so pick
    %% a ceiling that should avoid duplicates while staying well below the
    %% standard algorithms' 58-bit precision for efficiency.
    N = ((1 bsl 48) - 1),
    shuffle(List, [], N);
shuffle(Arg) ->
    erlang:error(badarg, [Arg]).

%% @hidden
shuffle([Elem | Elems], Pairs, N) ->
    %% We don't care about list reversal, it's going to be sorted.
    shuffle(Elems, [{rand:uniform(N), Elem} | Pairs], N);
shuffle([], Pairs, _N) ->
    deshuffle(lists:keysort(1, Pairs), []).

%% @hidden
deshuffle([{_, Elem} | Pairs], Elems) ->
    %% We don't care about list reversal, it'll be in random order anyway.
    deshuffle(Pairs, [Elem | Elems]);
deshuffle([], Elems) ->
    Elems.

%% @doc Return a random boolean value.
%% This function is implemented to achieve as even a distribution as can be
%% had with any of the `rand' algorithms as well as `crypto:rand_seed()'.
-spec random_boolean() -> boolean().
random_boolean() ->
    %% Accommodate algorithms that have low-bit issues.
    rand:uniform(16) > 8.

%% dict:dict() is opaque, don't complain about treating it as a tuple.
-dialyzer({no_opaque, random_element/1}).
%% @doc Return a randomly selected elements of a list or dict.
%% This function is implemented to achieve as random a selection as can be
%% had with any of the `rand' algorithms as well as `crypto:rand_seed()'.
-spec random_element(list() | dict:dict()) -> term().
random_element([Elem]) ->
    Elem;
random_element([_|_] = List) ->
    %% Some of the algorithms are kinda funky with small ranges, so
    %% maybe disregard some low bits.
    N = case erlang:length(List) of
        Short when Short < 8 ->
            (((rand:uniform(Short bsl 3) - 1) bsr 3) + 1);
        LongEnuf ->
            rand:uniform(LongEnuf)
    end,
    lists:nth(N, List);
random_element(Dict) when erlang:is_tuple(Dict)
        andalso erlang:size(Dict) > 1
        andalso erlang:element(1, Dict) =:= dict ->
    random_element(dict:to_list(Dict));
random_element(Arg) ->
    erlang:error(badarg, [Arg]).

-ifdef(TEST).

%% Look up values in a proplist by either a string or atom
find_atom_to_string_test() ->
    ?assertEqual(undefined, find_atom_or_string(value, [{a,1},{b,2}])),
    ?assertEqual(undefined, find_atom_or_string("value", [{a,1},{b,2}])),
    ?assertEqual(1, find_atom_or_string(a, [{a,1},{b,2}])),
    ?assertEqual(1, find_atom_or_string("a", [{a,1},{b,2}])).

%% Look up values in an orddict by either a string or atom
find_atom_to_string_dict_test() ->
    Dict = [{a,"a"},{"b",b}],
    ?assertEqual(undefined, find_atom_or_string_dict(value, Dict)),
    ?assertEqual(undefined, find_atom_or_string_dict("value", Dict)),
    ?assertEqual("a", find_atom_or_string(a, Dict)),
    ?assertEqual(b, find_atom_or_string("b", Dict)).

%% Convert a string to an atom, otherwise it remains unchanged
convert_to_atom_test() ->
    ?assertEqual(a, convert_to_atom(a)),
    ?assertEqual(a, convert_to_atom("a")),
    ?assertEqual(1, convert_to_atom(1)).

%% Properly covert backends to atoms
convert_to_atom_list_test() ->
    ?assertEqual(undefined, convert_to_atom_list(undefined)),
    ?assertEqual([memory], convert_to_atom_list(memory)),
    ?assertEqual([memory], convert_to_atom_list("memory")),
    ?assertEqual([bitcask, eleveldb, leveled, memory], lists:sort(convert_to_atom_list("memory, bitcask,eleveldb, leveled"))),
    ?assertEqual([bitcask, eleveldb, leveled, memory], lists:sort(convert_to_atom_list('memory, bitcask,eleveldb, leveled'))).

-endif.
