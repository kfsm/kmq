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
-module(kmq_queue_sup).
-behaviour(supervisor).

-export([
   start_link/2, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
%%
start_link(Name, Opts) ->
   {ok, Sup} = supervisor:start_link(?MODULE, [Name, Opts]),
   lists:foreach(
      fun(X) ->
         {ok, _}  = supervisor:start_child(Sup, 
            ?CHILD(worker, X, kmq_route, [{in, Name}, {mq, Name}])
         )
      end,
      lists:seq(1, opts:val(n, 1, Opts))
   ),
   {ok, Sup}.

init([Name, Opts]) -> 
   {ok,
      {
         {one_for_all, 4, 1800},
         [
            ?CHILD(worker, in, kmq_queue, [{in, Name}, opts:val(in, [], Opts)])
           ,?CHILD(worker, mq, kmq_queue, [{mq, Name}, opts:val(mq, [], Opts)])
         ]
      }
   }.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   
