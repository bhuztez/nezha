-module(nezha_proposer).

-behaviour(gen_fsm).

-export([start_link/4, start_link/6, start_link/7]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-export([leave/2, leave/3]).

-export([replay/3, sync/3, load/3, commit/3, join/3, joined/3]).

leave(Server, Name) ->
    ok = gen_fsm:sync_send_all_state_event(Server, {leave, Name}, infinity).

leave(Server, Name, Timeout) ->
    ok = gen_fsm:sync_send_all_state_event(Server, {leave, Name}, Timeout).

start_link(RegName, Name, Server, Snapshot) ->
    start_link(RegName, Name, Server, Snapshot, 1000, 5000).

start_link(RegName, Name, Server, Snapshot, Timeout, QuorumTimeout) ->
    start_link(RegName, Name, Server, Snapshot, Timeout, QuorumTimeout, []).

start_link(RegName, Name, Server, Snapshot, Timeout, QuorumTimeout, Options) ->
    gen_fsm:start_link({local, RegName}, ?MODULE, [Name, Server, Snapshot, Timeout, QuorumTimeout], Options).


init([Name, Server, Snapshot, Timeout, QuorumTimeout]) ->
    process_flag(trap_exit, true),

    {ok, Base, Data} = nezha_acceptor:get_base(Name),
    State = nezha_log:base_data_to_state(Data),

    {N, _} = Committed = nezha_snapshot:get(Snapshot),

    State1 =
        State#{
          name => Name,
          server => Server,
          snapshot => Snapshot,
          timeout => Timeout,
          quorum_timeout => QuorumTimeout,
          i => Base,
          base => Base,
          committed => Committed,
          play => N,
          workers => #{},
          requests => queue:new(),
          leave_requests => queue:new(),
          accepted => gb_trees:empty(),
          notified => none,
          propose => none
         },

    {ok, replay, enter(replay, State1)}.


replay({choose, I, none}, _From, State = #{i := I, name := Name, peers := Peers, bases := Bases, committed := {Y, _}}) ->
    StateName =
        case dict:is_key(Name, Peers) of
            true ->
                joined;
            false ->
                case gb_trees:smallest(Bases) of
                    {X, _} when I < X ->
                        sync;
                    {X, _} when Y < X ->
                        commit;
                    _ ->
                        join
                end
        end,
    {reply, ok, StateName, enter(StateName, State)};
replay({choose, I, Chosen}, _From, State = #{i := I}) ->
    State1 = nezha_log:apply_entry(Chosen, State),
    {reply, ok, replay, enter(replay, State1#{i := I+1})};
replay(_Event, _From, State) ->
    {reply, error, replay, State}.


sync({set_base, Base, Data}, _From, State) ->
    State1 = nezha_log:base_data_to_state(Data),
    State2 = maps:merge(State1#{i := Base, base := Base}, State),
    {reply, ok, load, enter(load, State2)};
sync(_Event, _From, State) ->
    {reply, error, sync, State}.


load({error, bad_instance}, _From, State) ->
    {reply, ok, sync, enter(sync, State)};
load({choose, I, Chosen}, _From, State = #{i := I}) ->
    State1 = nezha_log:apply_entry(Chosen, State),
    {reply, ok, load, enter(load, State1#{i := I+1})};
load({new_snapshot, {I, _} = Committed}, _From, State = #{bases := Bases}) ->
    case gb_trees:smallest(Bases) of
        {X, _} when I < X ->
            {reply, ok, load, State};
        _ ->
            {reply, ok, join, enter(join, State#{committed := Committed, play := I})}
    end;
load({set_base, Base, _}, _From, State = #{i := I}) when I >= Base ->
    {reply, ok, load, enter(load, State#{base := Base})};
load(_Event, _From, State) ->
    {reply, error, load, State}.


commit({error, bad_instance}, _From, State) ->
    {reply, ok, sync, enter(sync, State)};
commit({choose, I, Chosen}, _From, State = #{i := I}) ->
    State1 = nezha_log:apply_entry(Chosen, State),
    {reply, ok, commit, enter(commit, State1#{i := I+1})};
commit({new_snapshot, {I, _} = Committed}, _From, State = #{bases := Bases}) ->
    case gb_trees:smallest(Bases) of
        {X, _} when I < X ->
            {reply, ok, commit, State};
        _ ->
            {reply, ok, join, enter(join, State#{committed := Committed})}
    end;
commit({set_base, Base, _}, _From, State = #{i := I}) when I >= Base ->
    {reply, ok, commit, enter(commit, State#{base := Base})};
commit({set_play, I}, _From, State = #{play := X}) when X + 1 >= I ->
    {reply, ok, commit, enter(commit, State#{play := I})};
commit(_Event, _From, State) ->
    {reply, error, commit, State}.


join({error, bad_instance}, _From, State) ->
    {reply, ok, sync, enter(sync, State)};
join({choose, I, Chosen}, _From, State = #{i := I, name := Name, bases := Bases, propose := Ref}) ->
    State1 = #{peers := Peers} = nezha_log:apply_entry(Chosen, State),

    StateName =
        case Chosen of
            {_, _, {Ref, _, _}} ->
                case dict:is_key(Name, Peers) of
                    true ->
                        joined;
                    false ->
                        case gb_trees:smallest(Bases) of
                            {X, _} when I + 1 < X ->
                                load;
                            _  ->
                                commit
                        end
                end;
            _ ->
                join
        end,

    {reply, ok, StateName, enter(StateName, State1#{i := I+1})};
join({new_snapshot, {I, _} = Committed}, _From, State = #{committed := {N, _}}) when I > N ->
    {reply, ok, join, enter(join, State#{committed := Committed})};
join({new_snapshot, _}, _From, State) ->
    {reply, ok, join, State};
join({set_base, Base, _}, _From, State = #{i := I}) when I >= Base->
    {reply, ok, join, enter(join, State#{base := Base})};
join({set_play, I}, _From, State = #{play := X}) when X + 1 >= I ->
    {reply, ok, join, enter(join, State#{play := I})};
join(_Event, _From, State) ->
    {reply, error, join, State}.


joined({error, bad_instance}, _From, State) ->
    {reply, ok, sync, enter(sync, State)};
joined({choose, I, Chosen = {_, _, {Ref, _, _}}}, _From, State = #{name := Name, i := I, accepted := Accepted}) ->
    State1 = #{peers := Peers} = nezha_log:apply_entry(Chosen, State),
    State2 =
        case State1 of
            #{propose := {Ref, Leave, Value}} ->
                [ gen_fsm:reply(From, ok) || {_,From} <- queue:to_list(Leave) ],
                State1#{accepted := gb_trees:insert(I, Value, Accepted), propose := none};
            _ ->
                State1
        end,

    StateName =
        case dict:is_key(Name, Peers) of
            true ->
                joined;
            false ->
                join
        end,

    State3 =
        case StateName of
            join ->
                #{requests := Requests, leave_requests := LeaveRequests} = State2,

                [ gen_fsm:reply(From, {error, not_available})
                  || {_,From} <- queue:to_list(LeaveRequests) ],

                [ gen_server:reply(From, {error, not_available})
                  || {_,From} <- queue:to_list(Requests) ],

                State2#{
                  requests := queue:new(),
                  leave_requests := queue:new()
                 };
            joined ->
                State2
        end,

    {reply, ok, StateName, enter(StateName, State3#{i := I+1})};
joined({new_snapshot, {I, _} = Committed}, _From, State = #{committed := {N, _}}) when I > N ->
    {reply, ok, joined, enter(joined, State#{committed := Committed})};
joined({new_snapshot, _}, _From, State) ->
    {reply, ok, joined, State};
joined({set_base, Base, _}, _From, State = #{i := I}) when I >= Base ->
    {reply, ok, joined, enter(joined, State#{base := Base})};
joined({set_play, I}, _From, State = #{play := X}) when X + 1 >= I ->
    {reply, ok, joined, enter(joined, State#{play := I})};
joined(_Event, _From, State) ->
    {reply, error, joined, State}.



handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({request, {Request, From}}, _From, joined, State = #{server := Server, requests := Requests}) ->
    State1 =
        case nezha_server:handle_request(Server, From, Request) of
            noreply ->
                State;
            {ok, Req} ->
               enter(joined, State#{requests := queue:in({Req, From}, Requests)})
        end,
    {reply, ok, joined, State1};
handle_sync_event({request, {_, From}}, _From, StateName, State) ->
    gen_server:reply(From, {error, not_available}),
    {reply, ok, StateName, State};
handle_sync_event({leave, _}=Req, From, joined, State = #{leave_requests := Leaves}) ->
    {next_state, joined, enter(joined, State#{leave_requests := queue:in({Req, From}, Leaves)})};
handle_sync_event({leave, _}, _From, StateName, State) ->
    {reply, {error, not_available}, StateName, State};
handle_sync_event(get_value, _From, joined, State = #{requests := Requests, leave_requests := LeaveRequests}) ->
    Ref = make_ref(),
    case State of
        #{propose := {_, Leave, Value}} ->
            Requests1 = queue:join(Value, Requests),
            LeaveRequests1 = queue:join(Leave, LeaveRequests);
        _ ->
            Requests1 = Requests,
            LeaveRequests1 = LeaveRequests
    end,
    Leave1 = [L || {L, _} <- queue:to_list(LeaveRequests1)],
    Value1 = [R || {R, _} <- queue:to_list(Requests1)],

    State1 =
        State#{propose := {Ref, LeaveRequests1, Requests1},
               requests := queue:new(),
               leave_requests := queue:new()},

    {reply, {ok, {Ref, Leave1, Value1}}, joined, State1};
handle_sync_event(get_value, _From, StateName, State) ->
    Ref = make_ref(),
    {reply, {ok, {Ref, [], []}},  StateName, State#{propose := Ref}};
handle_sync_event({get_requests, I}, _From, StateName, State=#{accepted := Accepted}) ->
    {I, Value, Accepted1} = gb_trees:take_smallest(Accepted),
    {reply, queue:to_list(Value), StateName, State#{accepted := Accepted1}}.



handle_info({'EXIT', Pid, _}, StateName, State = #{workers := Workers}) ->
    case lists:keyfind(Pid, 2, maps:to_list(Workers)) of
        false ->
            {next_state, StateName, State};
        {Key, Pid} ->
            {next_state, StateName, start_worker(Key, State)}
    end.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



kill(Pid) ->
    true = exit(Pid, kill),
    true = unlink(Pid),
    receive
        {'EXIT', Pid, _} ->
            ok
    after 0 ->
            ok
    end.


kills(Keys, Map) ->
    [ kill(Pid)
      || Pid <- maps:values(maps:without(Keys, Map)) ],
    maps:with(Keys, Map).


%% also leave note on callback message of these workers
%% hay propose worker will also tell you about new snapshot
workers(replay) ->
    [replay];
workers(sync) ->
    [sync];
workers(load) ->
    [load, propose, truncate];
workers(commit) ->
    [commit, play, propose, truncate];
workers(join) ->
    [play, propose, truncate];
workers(joined) ->
    [play, propose, truncate].


enter(StateName, State = #{workers := Workers}) ->
    Keys = workers(StateName),
    Workers1 = kills(Keys, Workers),

    State1 =
        lists:foldl(
          fun (K, S = #{workers := W}) ->
                  case maps:is_key(K, W) of
                      true ->
                          S;
                      false ->
                          start_worker(K, S)
                  end
          end,
          State#{workers := Workers1},
          Keys),

    case State1 of
        #{notified := Pid, workers := #{propose := Pid}} ->
            State1;
        #{workers := #{propose := Pid}, requests := Requests, leave_requests := Leaves} ->
            case (queue:len(Requests) =:= 0) and (queue:len(Leaves) =:= 0) of
                true ->
                    State1;
                false ->
                    Pid ! {self(), new_value},
                    State1#{notified := Pid}
            end;
        _ ->
            State1
    end.


start_worker(Key, State = #{workers := Workers}) ->
    case worker(Key, State) of
        none ->
            State#{workers := maps:remove(Key, Workers)};
        Pid ->
            State#{workers := Workers#{Key => Pid}}
    end.

worker(replay, #{i := I, name := Name, timeout := Timeout}) ->
    Self = self(),
    spawn_link(fun() -> replay_worker(Self, Name, I, Timeout) end);
worker(sync, #{peers := Peers, name := Name, quorum_timeout := Timeout}) ->
    Peers1 = dict:fetch_keys(Peers),
    {ok, Pid} = nezha_sync:start_link(self(), Name, Peers1, Timeout),
    Pid;
worker(load, #{snapshot := Snapshot, peers := Peers}) ->
    Self = self(),
    Snapshots = [ V || {_,V} <- dict:to_list(Peers) ],
    spawn_link(fun () -> load_worker(Self, Snapshot, Snapshots) end);
worker(commit, #{server := Server, bases := Bases}) ->
    Self = self(),
    {I, _} = gb_trees:smallest(Bases),
    spawn_link(fun () -> commit_worker(Self, Server, I) end);
worker(play, #{i := I, name := Name, base := Base, play := N, server := Server, accepted := Accepted, timeout := Timeout}) when N >= Base ->
    Self = self(),
    case I > N of
        false ->
            none;
        true ->
            case gb_trees:is_empty(Accepted) of
                true ->
                    Known = none;
                false ->
                    {Known, _} = gb_trees:smallest(Accepted)
            end,
            spawn_link(fun () -> play_worker(Self, Name, Server, N, Known, Timeout) end)
    end;
worker(propose, #{name := Name, i := I, committed := Committed, peers := Peers, bases := Bases, server := Server, timeout := Timeout, quorum_timeout := QuorumTimeout}) ->
    N =
        case dict:find(Name, Peers) of
            error ->
                {X, _} = gb_trees:smallest(Bases),
                X - 1;
            {ok, {X, _}} ->
                X
        end,

    {ok, Pid} = nezha_propose:start_link(self(), Server, Name, dict:fetch_keys(Peers), I, Committed, N, Timeout, QuorumTimeout),
    Pid;
worker(truncate, #{name := Name, base := Base, committed := {N, _}, bases := Bases, timeout := Timeout}) ->
    case gb_trees:smallest(Bases) of
        {X, _} when Base < X, Base < N ->
            Self = self(),
            spawn_link(fun() -> truncate_worker(Self, Name, min(X,N), Timeout) end);
        _ ->
            none
    end.

get_log_entry(Name, I, Timeout) ->
    Name ! {request, self(), I, peek},
    receive
        {reply, Name, I, Reply} ->
            Reply
    after Timeout ->
            get_log_entry(Name, I, Timeout)
    end.

replay_worker(Parent, Name, I, Timeout) ->
    case get_log_entry(Name, I, Timeout) of
        {ok, _, Chosen, chosen} ->
            gen_fsm:sync_send_event(Parent, {choose, I, Chosen}),
            replay_worker(Parent, Name, I+1, Timeout);
        _ ->
            gen_fsm:sync_send_event(Parent, {choose, I, none})
    end.


load_worker(Parent, Snapshot, Snapshots) ->
    {ok, Result} = nezha_snapshot:load(Snapshot, Snapshots),
    gen_fsm:sync_send_event(Parent, {new_snapshot, Result}).

commit_worker(Parent, Server, I) ->
    {ok, Result} = nezha_server:commit(Server, I),
    gen_fsm:sync_send_event(Parent, {new_snapshot, Result}).



play_worker(Parent, Name, Server, I, Known, Timeout) ->
    case nezha_server:acquire(Server, I) of
        {error, I1} ->
            gen_fsm:sync_send_event(Parent, {set_play, I1});
        {ok, Ref} ->
            case Known of
                I ->
                    Requests = gen_fsm:sync_send_all_state_event(Parent, {get_requests, I}),
                    [ gen_fsm:reply(From, nezha_server:call(Server, Req)) || {Req, From} <- Requests ];
                _ when I < Known; Known =:= none ->
                    {ok, _, {_,_,{_,_,Requests}}, chosen} = get_log_entry(Name, I, Timeout),
                    [ nezha_server:call(Server, Req) || Req <- Requests ]
            end,
            nezha_server:release(Server, Ref),
            gen_fsm:sync_send_event(Parent, {set_play, I+1})
    end.


replay_until(_, I, I, State, _) ->
    State;
replay_until(Name, I, Base, State, Timeout) ->
    {ok, _, Chosen, chosen} = get_log_entry(Name, I, Timeout),
    State1 = nezha_log:apply_entry(Chosen, State),
    replay_until(Name, I+1, Base, State1, Timeout).


truncate_worker(Parent, Name, NewBase, Timeout) ->
    case nezha_acceptor:get_base(Name) of
        {Base, Data} when Base < NewBase ->
            State = nezha_log:base_data_to_state(Data),
            State1 = replay_until(Name, Base, NewBase, State, Timeout),
            NewData = nezha_log:state_to_base_data(State1),
            case nezha_acceptor:set_base(Name, NewBase, NewData) of
                ok ->
                    Base1 = NewBase,
                    Data1 = NewData;
                {error, Base1, Data1} ->
                    ok
            end,
            gen_fsm:sync_send_event(Parent, {set_base, Base1, Data1});
        {Base, Data} ->
            gen_fsm:sync_send_event(Parent, {set_base, Base, Data})
    end.
