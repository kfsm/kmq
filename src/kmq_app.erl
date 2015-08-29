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
-module(kmq_app).
-behaviour(application).

-export([
   start/2, stop/1
]).

start(_Type, _Args) ->
   lists:foreach(
      fun(X) -> listen(uri:new(X)) end, 
      opts:val(port, [], kmq)
   ),
   kmq_sup:start_link().

stop(_State) ->
   ok.

%%
%% 
listen({uri, tcp, _} = Uri) ->
   knet:listen(Uri, [
      {acceptor,    kmq_tcp}
     ,{backlog,         256}
     ,{pack,           line}
     ,{sndbuf,   256 * 1024}
     ,{recbuf,   256 * 1024}
   ]);

listen({uri, udp, _} = Uri) ->
   knet:listen(Uri, [
      {acceptor,    kmq_udp}
     ,{backlog,        1024}
     ,{dispatch,        p2p}
     ,{pack,            raw}
     ,{sndbuf,   256 * 1024}
     ,{recbuf,   256 * 1024}
   ]);

listen({uri, http, _} = Uri) ->
   knet:listen(Uri, [
      {acceptor,   kmq_http}
     ,{backlog,         256}
     ,{sndbuf,   256 * 1024}
     ,{recbuf,   256 * 1024}
   ]).
