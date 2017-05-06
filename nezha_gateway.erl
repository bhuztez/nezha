-module(nezha_gateway).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Name, Proposer, Server) ->
    start_link(Name, Proposer, Server, 5000).

start_link(Name, Proposer, Server, Timeout) ->
    start_link(Name, Proposer, Server, Timeout, []).

start_link(Name, Proposer, Server, Timeout, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [Proposer, Server, Timeout], Options).

init([Proposer, Server, Timeout]) ->
    link(whereis(Proposer)),
    link(whereis(Server)),
    {ok, {Proposer, Timeout}}.

handle_call(Request, From, State = {Proposer, Timeout}) ->
    ok = gen_fsm:sync_send_all_state_event(Proposer, {request, {Request, From}}, Timeout),
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
