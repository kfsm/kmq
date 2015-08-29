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
-spec(queue/2 :: (any(), list()) -> {ok, pid()} | {error, any()}).

queue(Queue, Opts) ->
   supervisor:start_child(kmq_sup, [scalar:s(Queue), Opts]).

%%
%% enqueue message
-spec(enq/2 :: (any(), any()) -> ok).
-spec(enq/3 :: (any(), any(), timeout()) -> ok).

enq(Queue, E) ->
   enq(Queue, E, 5000).

enq(Queue, E, Timeout) ->
   pipe:call(pns:whereis(kmq, {in, scalar:s(Queue)}), {enq, E}, Timeout).

%%
%% dequeue message
-spec(deq/1 :: (any()) -> [any()]).
-spec(deq/2 :: (any(), integer()) -> [any()]).
-spec(deq/3 :: (any(), any(), timeout()) -> [any()]).

deq(Queue) ->
   deq(Queue, 1).

deq(Queue, N) ->
   deq(Queue, N, 5000).

deq(Queue, N, Timeout) ->
   pipe:call(pns:whereis(kmq, {mq, scalar:s(Queue)}), {deq, N}, Timeout).
   
