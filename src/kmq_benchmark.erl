-module(kmq_benchmark).

-export([new/1, run/4]).

%%
%%
new(1) ->
   kmq:start([
      {kmq, [
         {port, [
            "tcp://*:10022"
         ]}
      ]}
   ]),
   kmq:queue(testq, basho_bench_config:get(queue, [])),
   {ok, #{sock => undefined}};
new(_) ->
   {ok, #{sock => undefined}}.

%%
%%
run(tcp, KeyGen, ValGen, #{sock := undefined} = State) ->
   {ok, Sock}  = gen_tcp:connect({127,0,0,1}, 10022, [binary]),
   run(tcp, KeyGen, ValGen, State#{sock => Sock});
   
run(tcp,_KeyGen, ValGen, #{sock := Sock} = State) ->
   Pckt = <<"testq:", (base64:encode(ValGen()))/binary, $\n>>,
   case gen_tcp:send(Sock, Pckt) of
      ok    -> {ok, State};
      Error -> {error, Error, State#{sock => undefined}}
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


