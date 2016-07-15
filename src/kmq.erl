%%
%%   Copyright (c) 2013 - 2015, Dmitry Kolesnikov
%%   Copyright (c) 2013 - 2015, Mario Cardona
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   light-weight message queue
-module(kmq).

-export([start/0, start/1]).
-export([
   queue/2
  ,enq/2
  ,enq/3
  ,deq/1
  ,deq/2
  ,deq/3
]).

%%
%% RnD application start
start() ->
   start(code:where_is_file("app.config")).
start(Config) ->
   applib:boot(?MODULE, Config).

%%
%% create message queue
%%  Options
%%    {mq, ...} - message queue specification
%%    {in, ...} - ingress queue specification
-spec queue(any(), list()) -> {ok, pid()} | {error, any()}.

queue(Queue, Opts) ->
   supervisor:start_child(kmq_sup, [scalar:s(Queue), Opts]).

%%
%% enqueue message
-spec enq(any(), any()) -> ok.
-spec enq(any(), any(), timeout()) -> ok.

enq(Queue, E) ->
   enq(Queue, E, 5000).

enq(Queue, E, Timeout) ->
   Name = scalar:s(Queue),
   clue:inc({kmq, Name, enq}),
   pipe:call(pns:whereis(kmq, {in, Name}), {enq, Name, E}, Timeout).

%%
%% dequeue message
-spec deq(any()) -> [any()].
-spec deq(any(), integer()) -> [any()].
-spec deq(any(), any(), timeout()) -> [any()].

deq(Queue) ->
   deq(Queue, 1).

deq(Queue, N) ->
   deq(Queue, N, 5000).

deq(Queue, N, Timeout) ->
   Name = scalar:s(Queue),
   clue:inc({kmq, Name, deq}),   
   pipe:call(pns:whereis(kmq, {mq, Name}), {deq, N}, Timeout).
   
