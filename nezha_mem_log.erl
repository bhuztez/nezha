-module(nezha_mem_log).

-behaviour(gen_server).

-export([start_link/1, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_base/1, set_base/3, get_entry/2, set_entry/3]).


get_base(Pid) ->
    gen_server:call(Pid, get_base, infinity).

set_base(Pid, Base, Data) ->
    gen_server:call(Pid, {set_base, Base, Data}, infinity).

get_entry(Pid, I) ->
    gen_server:call(Pid, {get_entry, I}, infinity).

set_entry(Pid, I, Value) ->
    gen_server:call(Pid, {set_entry, I, Value}, infinity).


start_link(Name) ->
    start_link(Name, []).

start_link(Name, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [], Options).

init([]) ->
    {ok, {0, none, dict:new()}}.

handle_call(get_base, _From, State = {0, _, _}) ->
    {reply, none, State};
handle_call(get_base, _From, State = {Base, Data, _}) ->
    {reply, {ok, Base, Data}, State};
handle_call({set_base, I, _}, _From, State = {Base, Data, _}) when I < Base ->
    {reply, {error, Base, Data}, State};
handle_call({set_base, Base, Data}, _From, State = {Base, Data, _}) ->
    {reply, ok, State};
handle_call({set_base, Base, Data}, _From, {_, _, Entries}) ->
    {reply, ok, {Base, Data, dict:filter(fun(X, _) -> X >= Base end, Entries)}};
handle_call({get_entry, I}, _From, State = {Base, _, _}) when I < Base ->
    {reply, error, State};
handle_call({set_entry, I}, _From, State = {Base, _, _}) when I < Base ->
    {reply, error, State};
handle_call({get_entry, I}, _From, State = {_, _, Entries}) ->
    Reply =
        case dict:find(I, Entries) of
            {ok, Value} ->
                Value;
            error ->
                #{}
        end,
    {reply, {ok, Reply}, State};
handle_call({set_entry, I, Value}, _From, {Base, Data, Entries}) ->
    {reply, ok, {Base, Data, dict:store(I, Value, Entries)}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
