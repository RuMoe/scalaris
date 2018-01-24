% @copyright 2012-2015 Zuse Institute Berlin,

%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Benchmark for master thesis
%% @end
%% @version $Id$
-module(basho_bench_on_cseq).
-author('skrzypczak@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([write/2]).

%% read filters
-export([rf_val/1]).
-export([rf_none/1]).

%% content checks
-export([cc_noop/3]).
-export([cc_noop_non_commute/3]).

%% write filters
-export([wf_add/3]).

-export([get_commuting_wf_for_rf/1]).
-export([get_cmd_class_by_filter/1]).

-type value()     :: non_neg_integer().

-define(KEY_MODE, {fixed, "123"}). % basho | {fixed, X}
-define(VALUE_MODE, {fixed, 1}). % bash | {fixed, X}


%% %%%%%%%%%%%%%%%%%%%%%%
%% API for basho bench
%% %%%%%%%%%%%%%%%%%%%%%%
read(K) ->
    Key = map_to_key(K),
    rbrcseq:qread(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                  get_benchmark_read_filter()),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                           end
                      )
    after 1000 ->
        log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                            case Value of
                                no_value_yet -> {fail, not_found};
                                _ -> {ok, Value}
                            end
                          )
       end
    end.

write(K, V) ->
    Key = map_to_key(K),
    Value = map_to_value(V),
    {RF, CC, WF} = get_benchmark_write_filter(),
    rbrcseq:qwrite(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                   RF, CC, WF, Value),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet}, {ok}); %%;
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                       begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                       {ok} end) %% TODO: extend write_result type {fail, Reason} )
    after 1000 ->
            log:log("~p write hangs at key ~p, ~p~n",
                    [self(), Key, erlang:process_info(self(), messages)]),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, Value, _WriteRet},
                               begin
                                   log:log("~p write was only slow at key ~p~n",
                                           [self(), Key]),
                                   {ok}
                               end); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                               begin log:log("~p Write failed: ~p~n",
                                             [self(), Reason]),
                                     {ok} end)
                end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Mapping to specific behaviour (too lazy to modify .sh scripts and recompile basho bensh driver)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
map_to_key(Key) ->
    case ?KEY_MODE of
        basho -> Key;
        {fixed, X} -> X
    end.

map_to_value(Value) ->
    case ?VALUE_MODE of
        basho -> Value;
        {fixed, X} -> X
    end.

get_benchmark_read_filter() ->
    fun ?MODULE:rf_val/1.

get_benchmark_write_filter_series(Rep) ->
    % from 0% - 90% in 5 percent steps, afterwards in 1 percent steps
    CommutePercentage = case Rep =< 19 of
                                true -> 5 * (Rep - 1) / 100.0;
                                false -> (90 + (Rep - 19)) / 100.0
                        end,
    case rand:uniform() > CommutePercentage of
        true ->
            {fun ?MODULE:rf_none/1, fun ?MODULE:cc_noop_non_commute/3, fun ?MODULE:wf_add/3};
        false ->
            {fun ?MODULE:rf_none/1, fun ?MODULE:cc_noop/3, fun ?MODULE:wf_add/3}
    end.

get_benchmark_write_filter() ->
    {fun ?MODULE:rf_none/1, fun ?MODULE:cc_noop/3, fun ?MODULE:wf_add/3}.

-spec rf_val(value() | prbr_bottom) -> client_value().
rf_val(prbr_bottom) -> 0;
rf_val(X)          -> X.
rf_none(_Value) -> none.

cc_noop(_ReadVal, _WriteFilter, _Val) -> {true, none}.
cc_noop_non_commute(_ReadVal, _WriteFilter, _Val) -> {true, none}.

wf_add(prbr_bottom, none, ToAdd) -> {ToAdd, none};
wf_add(Val, none, ToAdd) -> {Val + ToAdd, none}.

%%%
% commutiong op definitions
%%%

-spec get_commuting_wf_for_rf(prbr:read_filter()) ->
        [prbr:write_filter()].
get_commuting_wf_for_rf(_RF) -> [].

-spec get_cmd_class_by_filter({prbr:read_filter(), any(),
                               prbr:write_filter()}) -> cset:class().
get_cmd_class_by_filter(Filter={_RF, _CC, _WF}) ->
    AddCmd = {fun ?MODULE:rf_none/1,
              fun ?MODULE:cc_noop/3,
              fun ?MODULE:wf_add/3},
    case Filter of
        AddCmd ->
            addition;
        _ ->
            cset:non_commuting_class()
    end.
