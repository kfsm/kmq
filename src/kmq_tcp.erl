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
%%   queue tcp protocol
-module(kmq_tcp).
-behaviour(pipe).

-export([
   start_link/2,
   init/1,
   free/2,
   ioctl/2,
   handle/3
]).


%%
%%
start_link(Uri, Opts) ->
   pipe:start_link(?MODULE, [Uri, Opts], []).

init([Uri, Opts]) ->
   {ok, handle, 
      #{sock => knet:bind(Uri, Opts)}
   }.

free(_, #{sock := Sock}) ->
   knet:close(Sock).

%%
ioctl(_, _) ->
   throw(not_implemented).

%%
%%
handle({tcp, _Pid, {established, _Peer}},  _Pipe, State) ->
   {next_state, handle, State};

handle({tcp, _Pid, {terminated, _Reason}}, _Pipe, State) ->
   {stop, normal, State};

handle({tcp, _Pid, passive}, Pipe, State) ->
   pipe:a(Pipe, active),
   {next_state, handle, State};

handle({tcp, _Peer, <<"queue:", Name/binary>>}, _Pipe, State) ->
   % bind socket with queue name allowing duplex communication
   pns:register(kmq, {in, Name}, self()),
   {next_state, handle, State};

handle({tcp, _Peer, Pckt}, _Pipe, State) ->
   % Note: enqueue uses synchronous call to 
   %       implement socket level flow control
   [Queue, E] = binary:split(Pckt, <<$:>>),
   kmq:enq(Queue, E, infinity),
   {next_state, handle, State};

handle({enq, Name, Pckt}, Pipe, #{sock := Sock} = State) ->
   knet:send(Sock, <<(scalar:s(Name))/binary, $:, Pckt/binary, $\n>>),
   pipe:a(Pipe, ok),
   {next_state, handle, State}.



