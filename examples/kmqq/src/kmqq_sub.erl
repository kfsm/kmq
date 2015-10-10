%% @doc
%%   subscriber process
-module(kmqq_sub).
-beaviour(pipe).

-export([
   start_link/0
  ,init/1
  ,free/2
  ,handle/3
]).

start_link() ->
   pipe:start_link(?MODULE, [], []).

init(_) ->
   erlang:send(self(), run),
   {ok, handle, #{}}.

free(_, _State) ->
   ok.


handle(run, _, State) ->
   case kmq:deq(kmqq, 10, infinity) of
      [] ->
         erlang:send_after(1000, self(), run),
         {next_state, handle, State};
      List ->
         clue:inc({kmqq, sub}, length(List)),
         erlang:send(self(), run),
         {next_state, handle, State}
   end;

handle(_, _, State) ->
   {next_state, handle, State}.
