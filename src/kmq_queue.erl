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

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Name, Opts) ->
   pipe:start_link(?MODULE, [Name, Opts], []).

init([Name, Opts]) ->
   pns:register(kmq, Name, self()),
   {ok, handle, esq:new(Opts)}.

free(_Reason, State) ->
   esq:free(State).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({enq, Msg}, Pipe, State0) ->
   State1 = esq:enq(Msg, State0),
   pipe:a(Pipe, ok),
   {next_state, handle, State1};

handle({deq,  N}, Pipe, State0) ->
   {List, State1} = esq:deq(N, State0),
   pipe:a(Pipe, [X || {_, X} <- List]),
   {next_state, handle, State1}.

