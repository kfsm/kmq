%% @doc
%%   tcp publisher
-module(kmqq_tcp).
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
      knet:connect("tcp://127.0.0.1:10030", [{sndbuf, 256 * 1024}])
   }.

free(_, Sock) ->
   knet:close(Sock).

handle(run, _, Sock) ->
   pipe:send(Sock, pack()),
   erlang:send_after(1, self(), run),
   clue:inc({kmqq, tcp}),
   {next_state, handle, Sock};

handle(_, _, Sock) ->
   {next_state, handle, Sock}.


pack() ->
   <<"kmqq:0123456789\n">>.

