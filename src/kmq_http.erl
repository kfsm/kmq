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
-module(kmq_http).
-behaviour(pipe).

-export([
   start_link/2,
   init/1,
   free/2,
   ioctl/2,
   handle/3,
   message/3
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
handle({http, _Sock, {'POST', Url, Head, _Env}}, _Pipe, State) ->
   [Queue] = uri:segments(Url),
   Connect = case lists:keyfind('Connection', 1, Head) of
      false    -> <<"keep-alive">>;
      {_, Val} -> Val
   end,
   {next_state, message, State#{q => Queue, in => q:new(), c => Connect}}.

message({http, _Sock,  eof}, Pipe, #{q := Queue, in := In, c := Connect} = State) ->
   Pckt = erlang:iolist_to_binary(q:list(In)),
   kmq:enq(Queue, Pckt),
   pipe:a(Pipe, {ok, [
      {'Server',     <<"kmq">>},
      {'Transfer-Encoding', <<"chunked">>},
      {'Connection', Connect}
   ]}),
   pipe:a(Pipe, bits:btoh( crypto:hash(sha, Pckt) )),
   pipe:a(Pipe, eof),
   {next_state, handle, State#{q => undefined, in => undefined}};

message({http, _Sock, Pckt}, _Pipe, #{in := In} = State) ->
   {next_state, message, State#{in => q:enq(Pckt, In)}}.

