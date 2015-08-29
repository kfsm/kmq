-module(kmq_benchmark).

-export([new/1, run/4]).

-define(HOST, {127,0,0,1}).
-define(TCP,        10022).
-define(UDP,        10023).

%%
%%
new(1) ->
   kmq:start([
      {kmq, [
         {port, [
            "tcp://*:10022",
            "udp://*:10023"
         ]}
      ]}
   ]),
   kmq:queue(testq, basho_bench_config:get(queue, [])),
   {ok, #{tcp => undefined, udp => undefined}};
new(_) ->
   {ok, #{tcp => undefined, udp => undefined}}.

%%
%%
run(tcp, KeyGen, ValGen, #{tcp := undefined} = State) ->
   {ok, Sock}  = gen_tcp:connect(?HOST, ?TCP, [binary]),
   run(tcp, KeyGen, ValGen, State#{tcp => Sock});
   
run(tcp,_KeyGen, ValGen, #{tcp := Sock} = State) ->
   Pckt = <<"testq:", (base64:encode(ValGen()))/binary, $\n>>,
   case gen_tcp:send(Sock, Pckt) of
      ok    -> {ok, State};
      Error -> {error, Error, State#{tcp => undefined}}
   end; 

run(udp, KeyGen, ValGen, #{udp := undefined} = State) ->
   {ok, Sock}  = gen_udp:open(0),
   run(udp, KeyGen, ValGen, State#{udp => Sock});
   
run(udp,_KeyGen, ValGen, #{udp := Sock} = State) ->
   Pckt = <<"testq:", (base64:encode(ValGen()))/binary, $\n>>,
   case gen_udp:send(Sock, ?HOST, ?UDP, Pckt) of
      ok    -> {ok, State};
      Error -> {error, Error, State#{udp => undefined}}
   end; 


run(enq, _KeyGen, ValGen, State) ->
   case kmq:enq(testq, ValGen()) of
      ok  -> {ok, State};
      Err -> {error, Err, State}
   end;

run(deq, _KeyGen, _ValGen, State) ->
   case kmq:deq(testq) of
      []  -> {error, not_found, State};
      X when is_list(X) -> {ok, State};
      Err -> {error, Err, State}
   end.


