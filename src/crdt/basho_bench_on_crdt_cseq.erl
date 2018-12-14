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
-module(basho_bench_on_crdt_cseq).
-author('skrzypczak@zib.de').

-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1, write/2]).

%% %%%%%%%%%%%%%%%%%%%%%%
%% API for basho bench
%% %%%%%%%%%%%%%%%%%%%%%%
-spec read(client_key()) -> {ok, any()} | {fail, not_found}.
read(Key) -> gcounter_on_cseq:read(Key).

-spec write(client_key(), client_value()) -> ok.
write(Key, _Val) ->  gcounter_on_cseq:inc(Key).
