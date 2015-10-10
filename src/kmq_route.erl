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
-module(kmq_route).
-behavior(pipe).

-include("kmq.hrl").

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

start_link(In, Eg) ->
   pipe:start_link(?MODULE, [In, Eg], []).

init([In, Eg]) ->
   erlang:send_after(?CONFIG_TIMEOUT_IDLE, self(), timeout),
   {ok, handle, 
      #{
         in => pns:whereis(kmq, In)
        ,eg => pns:whereis(kmq, Eg) 
      }
   }.

free(_Reason, _State) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle(timeout, _Pipe, #{in := In} = State) ->
   Tx = pipe:cast(In, {deq, ?CONFIG_ROUTE_JOB}),
   {next_state, handle, State#{tx => Tx}};

handle({Tx, []}, _Pipe, #{tx := Tx} = State) ->
   erlang:send_after(?CONFIG_TIMEOUT_IDLE, self(), timeout),
   {next_state, handle, State#{tx => undefined}};

handle({Tx, List}, _Pipe, #{tx := Tx, in := In, eg := Eg} = State) ->
   lists:foreach(
      fun(X) ->         
         pipe:call(Eg, {enq, X}, infinity)
      end,
      List
   ),
   Txx = pipe:cast(In, {deq, ?CONFIG_ROUTE_JOB}),
   {next_state, handle, State#{tx => Txx}};
   
handle(_, _Pipe, State) ->
   {next_state, handle, State}.

