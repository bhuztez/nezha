-module(nezha_proxy).

-behaviour(gen_server).

-export([start_link/3, start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Name, Server, Timeout) ->
    start_link(Name, Server, Timeout, []).

start_link(Name, Server, Timeout, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [Server, Timeout], Options).

init([Server, Timeout]) ->
    link(whereis(Server)),
    {ok,
     #{queue => queue:new(),
       server => Server,
       timeout => Timeout,
       timer => erlang:start_timer(Timeout, self(), [])
      }
    }.


handle_call(Request, From, State = #{queue := Queue}) ->
    {noreply, State#{queue := queue:in({Request, From}, Queue)}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, Timer, []}, State = #{timer := Timer, queue := Queue, timeout := Timeout, server := Server}) ->
    [ Server ! {'$gen_call', From, Request}
      || {Request, From} <- queue:to_list(Queue)
    ],

    {noreply,
     State#{
       timer := erlang:start_timer(Timeout, self(), []),
       queue := queue:new()
      }
    }.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
