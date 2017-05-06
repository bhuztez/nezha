-module(nezha_counter_snapshot).

-behaviour(gen_server).

-export([start_link/1, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get/1, set/2, load/2]).


get(Pid) ->
    gen_server:call(Pid, get).

set(Pid, Snapshot) ->
    gen_server:call(Pid, {set, Snapshot}).

load(Pid, Snapshots) ->
    gen_server:call(Pid, {load, Snapshots}, infinity).

start_link(Name) ->
    start_link(Name, []).

start_link(Name, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [], Options).

init([]) ->
    {ok, {1, 0}}.


handle_call(get, _From, State) ->
    {reply, State, State};
handle_call({set, Snapshot}, _From, State) ->
    handle_set_snapshot(Snapshot, State);
handle_call({load, []}, _From, State) ->
    {reply, error, State};
handle_call({load, Snapshots}, _From, State) ->
    handle_set_snapshot(lists:max(Snapshots), State).

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_set_snapshot(Snapshot = {I,_}, {N,_}) when I > N ->
    {reply, ok, Snapshot};
handle_set_snapshot(State, State) ->
    {reply, ok, State};
handle_set_snapshot(_, State) ->
    {reply, {error, State}, State}.
