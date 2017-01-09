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
%%   pipe container for esq data structure
-module(kmq_queue).
-behavior(pipe).

-export([
   start_link/2
  ,init/1
  ,free/2
  ,handle/3
]).

-record(state, {
   id  = undefined :: _,        %% queue name 
   q   = undefined :: _,        %% queue data structure
   idle = []       :: [_],      %% list of idle subscribers
   busy = []       :: [_]       %% list of busy subscribers
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Name, Opts) ->
   pipe:start_link(?MODULE, [Name, Opts], []).

init([{_, Id} = Name, Opts]) ->
   pns:register(kmq, Name, self()),
   {ok, handle, 
      #state{
         id   = Id,
         q    = esq:new(Opts)
      }
   }.

free(_Reason, #state{q = Queue}) ->
   esq:free(Queue).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({enq, _, Msg}, Pipe, #state{q = Queue0} = State) ->
   Queue1 = esq:enq(Msg, Queue0),
   pipe:a(Pipe, ok),
   {next_state, handle, pub(State#state{q = Queue1})};

handle({deq,  N}, Pipe, #state{q = Queue0} = State) ->
   {List, Queue1} = esq:deq(N, Queue0),
   pipe:a(Pipe, [X || {_, X} <- List]),
   {next_state, handle, State#state{q = Queue1}};

handle({sub, Pid, N}, Pipe, #state{idle = Idle} = State) ->
   erlang:monitor(process, Pid),
   pipe:ack(Pipe, ok),
   {next_state, handle, pub(State#state{idle = [{Pid, N}|Idle]})};

handle({Tx, ok}, _Pipe, #state{idle = Idle, busy = Busy0} = State) ->
   case lists:keytake(Tx, 1, Busy0) of
      {value, Sub, Busy1} ->
         {next_state, handle, pub(State#state{idle = [Sub|Idle], busy = Busy1})};
      false ->
         {next_state, handle, pub(State)}
   end;

handle({'DOWN', _, process, Pid, _Reason}, _Pipe, #state{idle = Idle0, busy = Busy0} = State) ->
   Idle1 = [Sub || Sub = {X, _} <- Idle0, X /= Pid],
   Busy1 = [Sub || Sub = {_, {X, _}} <- Busy0, X /= Pid],
   {next_state, handle, pub(State#state{idle = Idle1, busy = Busy1})}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

pub(#state{idle = []} = State) ->
   State;
pub(#state{idle = [{Pid, N} = H|Idle], busy = Busy, q = Queue0, id = Id} = State) ->
   case esq:deq(N, Queue0) of
      {[], Queue1} ->
         State#state{q = Queue1};
      {List, Queue1} -> 
         Tx = publish(Pid, Id, List),
         State#state{idle = Idle, busy = [{Tx, H}|Busy], q = Queue1}
   end.

publish(Pid, Id, [{_, X}]) ->
   pipe:cast(Pid, {kmq, Id, X});
publish(Pid, Id, [{_, X} | T]) ->
   pipe:send(Pid, {kmq, Id, X}),
   publish(Pid, Id, T).
