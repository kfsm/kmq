%% @doc
%%   udp publisher
-module(kmqq_udp).
-behaviour(pipe).

-export([
   start_link/0
  ,init/1
  ,free/2
  ,handle/3
]).

start_link() ->
   pipe:start_link(?MODULE, [], []).

init(_) ->
   erlang:send_after(10, self(), run),
   {ok, handle,
      knet:connect("udp://*:0", [{sndbuf, 256 * 1024}])
   }.

free(_, Sock) ->
   knet:close(Sock).

handle(run, _, Sock) ->
   pipe:send(Sock, {peer(), pack()}),
   erlang:send_after(1, self(), run),
   clue:inc({kmqq, udp}),
   {next_state, handle, Sock};

handle(_, _, Sock) ->
   {next_state, handle, Sock}.


pack() ->
   <<"kmqq:0123456789\n">>.

peer() ->
   {{127,0,0,1}, port()}.

port() ->
   10020 + random:uniform(10) - 1. 


