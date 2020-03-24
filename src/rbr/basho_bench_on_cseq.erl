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
%% @doc    Benchmarking interface called by basho-bench.
%% @end
%% @version $Id$
-module(basho_bench_on_cseq).
-author('skrzypczak@zib.de').
-vsn('$Id:$ ').

-define(TRACE(X,Y), ok).

%% assumes that all writes are done for the same key
-define(FAST_WRITES, true). %% emulates leader

% for hanging writer latency benchmark
-define(HANGING_WRITER_PROP, 0).
-define(HANGING_WRITER_DURATION, trunc(60*1.7)). %% how long it is hanging in seconds

-include("scalaris.hrl").
-include("client_types.hrl").

-on_load(fast_write_init/0).

-export([read/1]).
-export([write/2]).

%% read filters
-export([rf_val/1]).
-export([rf_int/1]).
-export([rf_size_counter/1]).

%% content checks
-export([cc_noop/3]).

%% write filters
-export([wf_add/3]).
-export([wf_add_size_counter/3]).


%% %%%%%%%%%%%%%%%%%%%%%%
%% API for basho bench
%% %%%%%%%%%%%%%%%%%%%%%%
-spec read(client_key()) -> {ok, any()} | {fail, not_found}.
read(Key) ->
    ReadOp = get_read_op_full_value(),

    rbrcseq:qread(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                  ReadOp),
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

-spec write(client_key(), client_value()) -> {ok}.
write(Key, Val) ->
  write(Key, Val, ?HANGING_WRITER_PROP > 0).

write(Key, Val, _MaybeHanging=true) ->
  case rand:uniform(?HANGING_WRITER_PROP) =:= 1 of
    true ->
      timer:sleep(1000 * ?HANGING_WRITER_DURATION),
      ok;
    false -> ok
  end,
  write(Key, Val, false);
write(Key, Val, _MaybeHanging=false) ->
    case ?FAST_WRITES of
        true ->
            fast_write(Key, Val);
        false ->
            {RF, CC, WF} = get_write_op_addition(),

            rbrcseq:qwrite(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                           RF, CC, WF, 1),
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet}, {ok}); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                               begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                               {ok} end) %% TODO: extend write_result type {fail, Reason} )
            after 1000 ->
                    log:log(info, "~p write hangs at key ~p, ~p~n",
                            [self(), Key, erlang:process_info(self(), messages)]),
                    receive
                        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet},
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
            end
    end.

-spec fast_write(client_key(), client_value()) -> {ok}.
fast_write(Key, Val) ->
    RequestSequenzer =
        fun() ->
            StartRound = receive {init_round, R} -> R end,
            ProcessFun =
                fun(F, Round) ->
                    receive {next_request, Client} ->
                        Client ! {next_round, Round}
                    end,
                    receive {do_next} -> ok end,
                    NewRound = pr:new(pr:get_r(Round) + 1, pr:get_id(Round)),
                    F(F, NewRound)
                end,
            ProcessFun(ProcessFun, StartRound)
        end,

    Init =
      case whereis(request_handler) of
        undefined ->
            is_first ! {first, self()},
            IsFirst = receive {first, Bool} -> Bool end,
            case IsFirst of
                false -> timer:sleep(100); %% hack to ensure that request process is registered
                true -> ok
            end,
            IsFirst;
        _ ->
            false
      end,

    RoundToUse =
        case Init of
            true ->
                Pid = spawn(fun() -> RequestSequenzer() end),
                register(request_handler, Pid),
                pr:new(1, '_');
            false ->
                request_handler ! {next_request, self()},
                receive {next_round, NewRound} -> NewRound end
        end,

    {RF, CC, WF} =
        case Val == <<>> of
            true -> get_write_op_addition();
            false -> get_write_op_size_counter_addition()
        end,
    case pr:get_r(RoundToUse) of
        1 ->
            rbrcseq:qwrite_fast(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                   RF, CC, WF, Val, RoundToUse, 0),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, NextFastWriteRound, _Value, _WriteRet}, {ok}); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, NextFastWriteRound, _Value, Reason},
                               begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                               {ok} end)
            end,
            request_handler ! {init_round, NextFastWriteRound};
        _ ->
            rbrcseq:qwrite_fast(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                RF, CC, WF, Val, RoundToUse, 0),
            request_handler ! {do_next},
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastRound, _Value, _WriteRet}, ok); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastRound, _Value, Reason},
                               begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                               ok end)
            end
    end,

    {ok}.

fast_write_init() ->
    P = spawn(fun() ->
                receive {first, Pi} ->
                    Pi ! {first, true}
                end,
                L = fun(Loop) ->
                        receive {first, Pid} ->
                            Pid ! {first, false}
                        end,
                        Loop(Loop)
                    end,
                L(L)
            end),
    register(is_first, P),
    ok.


%% ------------------------------- READ OPERATIONS -----------------------------
%% @doc Reads full value.
-spec get_read_op_full_value() -> prbr:read_filter().
get_read_op_full_value() -> fun ?MODULE:rf_val/1.

%% ------------------------------ WRITE OPERATIONS -----------------------------
%% @doc Addition.
-spec get_write_op_addition() -> {prbr:write_filter(), any(), prbr:read_filter()}.
get_write_op_addition() ->
    {fun ?MODULE:rf_int/1, fun ?MODULE:cc_noop/3, fun ?MODULE:wf_add/3}.

%% @doc Counter addition with payload for cmd size benchmarks.
-spec get_write_op_size_counter_addition() -> {prbr:write_filter(), any(), prbr:read_filter()}.
get_write_op_size_counter_addition() ->
    {fun ?MODULE:rf_size_counter/1, fun ?MODULE:cc_noop/3, fun ?MODULE:wf_add_size_counter/3}.

%% ------------------------- FILTER -------------------------

%% @doc Readfilter returning the complete value or no_value_yet if key does not exist.
-spec rf_val(client_value() | prbr_bottom) -> client_value() | no_value_yet.
rf_val(prbr_bottom) -> no_value_yet;
rf_val(X)           -> X.

%% @doc Readfilter returning the complete value or 0 if key does not exist.
%% Assumens that value is a key
-spec rf_int(integer() | prbr_bottom) -> integer().
rf_int(prbr_bottom) -> 0;
rf_int(X) when is_integer(X) -> X.

-spec rf_size_counter(integer() | prbr_bottom) -> {integer(), binary()}.
rf_size_counter(prbr_bottom) -> {0, <<>>};
rf_size_counter(Counter={X,_Y}) when is_integer(X) -> Counter.

%% @doc Does nothing.
-spec cc_noop(any(), any(), any()) -> {true, none}.
cc_noop(_ReadVal, _WriteFilter, _Val) -> {true, none}.

%% @doc Writefilter that adds a value to an integer. Returns no value to client.
-spec wf_add(integer() | prbr_bottom, any(), integer()) -> {integer(), none}.
wf_add(prbr_bottom, _UI, _ToAdd) -> {1, none};
wf_add(Val, _UI, _ToAdd) -> {Val + 1, none}.

-spec wf_add_size_counter(integer() | prbr_bottom, any(), integer()) -> {integer(), none}.
wf_add_size_counter(prbr_bottom, _UI, Payload) -> {{1, Payload}, none};
wf_add_size_counter({Val, _OldPayload}, _UI, Payload) -> {{Val + 1, Payload}, none}.

