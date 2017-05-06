-module(nezha_counter).

-behaviour(gen_server).

-compile({no_auto_import, [get/1]}).

-export([start_link/2, start_link/3, start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([commit/2, acquire/2, release/2, handle_request/3, call/2]).

-export([get/0, dirty_get/0, incr/0, watch/0, leave/1]).
-export([get/1, dirty_get/1, incr/1, watch/1]).

-define(SERVER, counter).

get() -> get(?SERVER).
dirty_get() -> dirty_get(?SERVER).
incr() -> incr(?SERVER).
watch() -> watch(?SERVER).
leave(Node) ->
    nezha_proposer:leave(nezha_proposer, {nezha_acceptor, Node}).


get(Pid) ->
    gen_server:call(Pid, get, infinity).

dirty_get(Pid) ->
    gen_server:call(Pid, dirty_get, infinity).

incr(Pid) ->
    gen_server:call(Pid, incr, infinity).

watch(Pid) ->
    Ref = make_ref(),
    gen_server:call(Pid, {watch, Ref}, infinity),
    watch_loop(Ref).

watch_loop(Ref) ->
    receive
        {counter, Ref, V} ->
            io:format("~p~n", [V]),
            watch_loop(Ref)
    end.


commit(Pid, I) ->
    gen_server:call(Pid, {commit, I}).

acquire(Pid, I) ->
    gen_server:call(Pid, {acquire, I}).

release(Pid, Ref) ->
    gen_server:call(Pid, {release, Ref}).

handle_request(Server, From, {watch, _}=Req) ->
    Server ! {'$gen_call', From, Req},
    noreply;
handle_request(Server, From, dirty_get) ->
    Server ! {'$gen_call', From, get},
    noreply;
handle_request(_Server, _From, Req) ->
    {ok, Req}.

call(Server, Req) ->
    gen_server:call(Server, Req, infinity).

start_link(Name, Snapshot) ->
    start_link(Name, Snapshot, 15000).

start_link(Name, Snapshot, Interval) ->
    start_link(Name, Snapshot, Interval, []).

start_link(Name, Snapshot, Interval, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [Snapshot, Interval], Options).

init([Snapshot, Interval]) ->
    process_flag(trap_exit, true),

    {I, V} = Committed = nezha_counter_snapshot:get(Snapshot),

    {ok,
     #{ snapshot => Snapshot,
        watchers => dict:new(),
        committers => gb_trees:empty(),
        committed => Committed,
        latest => Committed,
        i => I,
        value => V,
        timer => erlang:start_timer(Interval, self(), []),
        interval => Interval
      }
    }.

handle_call({acquire, _}, _From, State = #{lock := _}) ->
    {stop, conflict, State};
handle_call({acquire, I}, _From, State = #{i := I1}) when I < I1->
    {reply, {error, I1}, State};
handle_call({acquire, I}, {Pid, _}, State = #{i := I}) ->
    link(Pid),
    Ref = make_ref(),
    {reply, {ok, Ref}, State#{lock => {Pid, Ref}}};
handle_call({acquire, I}, {Pid, _}, State = #{snapshot := Snapshot}) ->
    link(Pid),
    case nezha_counter_snapshot:get(Snapshot) of
        {I, V} ->
            Ref = make_ref(),
            {reply, {ok, Ref}, State#{committed => {I, V}, i => I, value => V, lock => {Pid, Ref}}};
        {I1, V} when I1 > I ->
            {reply, {error, I1}, State#{committed => {I1, V}, i => I1, value => V}};
        _ ->
            {reply, {error, I}, State}
    end;
handle_call({release, Ref}, _From, State = #{i := I, value := V, lock := {_, Ref}}) ->
    State1 = maps:remove(lock, State#{i := I + 1, latest := {I+1, V}}),
    {reply, ok, State1};
handle_call(incr, _From, State = #{value := V, watchers := Watchers, lock := _}) ->
    V1 = V + 1,
    [W ! {counter, Ref, V1} || {W, Ref} <- dict:to_list(Watchers)],
    {reply, V1, State#{value := V1}};
handle_call(get, _From, State = #{value := V}) ->
    {reply, V, State#{value := V}};
handle_call({watch, Ref}, {Pid,_}, State = #{watchers := Watchers}) ->
    case State of
        #{value := V} ->
            Pid ! {counter, Ref, V};
        _ ->
            ok
    end,

    {reply, ok, State#{watchers := dict:store(Pid, Ref, Watchers)}};
handle_call({commit, I1}, _, State = #{committed := {I2, _} = Committed}) when I1 =< I2 ->
    {reply, {ok, Committed}, State};
handle_call({commit, I}, From, State = #{committers := Committers}) ->
    Value =
        case gb_trees:lookup(I, Committers) of
            none ->
                [From];
            {value, V} ->
                [From|V]
        end,

    {noreply, State#{committers := gb_trees:enter(I, Value, Committers)}}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #{lock := {Pid, _}}) ->
    {stop, Reason, State};
handle_info({'EXIT', Pid, _}, State = #{watchers := Watchers}) ->
    {noreply, State#{watchers := dict:erase(Pid, Watchers)}};
handle_info({timeout, Timer, []}, State = #{timer := Timer, interval := Interval, snapshot := Snapshot}) ->
    State1 =
        case State of
            #{latest := {I1, _} = Latest, committed := {I2, _}, committers := Committers} when I1 > I2->
                case nezha_counter_snapshot:set(Snapshot, Latest) of
                    ok ->
                        Committed = Latest;
                    {error, Committed}  ->
                        Committed
                end,

                State#{committed := Committed, committers := reply_committers(Committed, Committers)};
            _ ->
                State
        end,

    {noreply, State1#{timer := erlang:start_timer(Interval, self(), [])}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


reply_committers(Committed = {I, _}, Committers) ->
    case gb_trees:is_empty(Committers) of
        true ->
            Committers;
        false ->
            case gb_trees:smallest(Committers) of
                {I1, _} when I < I1 ->
                    Committers;
                {_, Value} ->
                    [ gen_server:reply(From, {ok, Committed}) || From <- Value ],
                    {_,_,Committers1} = gb_trees:take_smallest(Committers),
                    Committers1
            end
    end.
