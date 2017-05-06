-module(nezha_propose).

-behaviour(gen_server).

-export([start_link/9]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Parent, Snapshot, Name, Peers, I, Committed, N, Timeout, QuorumTimeout) ->
    gen_server:start_link(?MODULE, [Parent, Snapshot, Name, Peers, I, Committed, N, Timeout, QuorumTimeout], []).

init([Parent, Snapshot, Name, Peers, I, Committed, N, Timeout, QuorumTimeout]) ->
    random:seed(erlang:phash2(Name), erlang:phash2(self()), I),

    process_flag(trap_exit, true),
    Self = self(),

    State = #{
      parent => Parent,
      name => Name,
      peers => sets:from_list(Peers),
      i => I,
      have => N,
      new_value => false,
      committed => Committed,
      loader => spawn_link(fun () -> get_snapshot(Self, Parent, Snapshot, Committed) end),
      phase => load,
      timeout => Timeout,
      quorum_timeout => QuorumTimeout
     },

    {ok, State}.


get_snapshot(Parent, Proposer, Snapshot, {N, _} = Committed) ->
    case nezha_snapshot:get(Snapshot) of
        {I, _} = Committed1 when I > N ->
            gen_fsm:sync_send_event(Proposer, {new_snapshot, Committed1});
        _ ->
            Committed1 = Committed
    end,
    gen_server:call(Parent, {set_committed, Committed1}).


loaded(State = #{committed := {N, _}, have := I, new_value := false}) when I >= N ->
    enter_peek(State);
loaded(State) ->
    {noreply, State1} = enter_prepare(State#{n => 0}),
    State1.


handle_call({set_committed, Committed}, _From, State = #{loader := Loader}) ->
    unlink(Loader),
    {reply, ok, loaded(State#{committed := Committed})}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _}, State = #{loader := Pid, phase := load}) ->
    {noreply, loaded(State)};
handle_info({'EXIT', Pid, _}, State = #{loader := Pid}) ->
    {noreply, State};
handle_info({timeout, Timer, []}, State = #{timer := Timer, phase := {prepare, _} = Req, peers := Peers, ready := Ready}) ->
    N = sets:size(Peers),
    R = sets:size(Ready),
    if 2 * R > N ->
            enter_prepare(State);
       true ->
            {noreply, send_request(Req, State)}
    end;
handle_info({timeout, Timer, []}, State = #{timer := Timer, phase := Req}) ->
    {noreply, send_request(Req, State)};
handle_info({Parent, new_value}, State = #{parent := Parent, phase := load}) ->
    {noreply, State#{new_value := true}};
handle_info({Parent, new_value}, State = #{parent := Parent, phase := peek}) ->
    enter_prepare(State);
handle_info({Parent, new_value}, State = #{parent := Parent}) ->
    {noreply, State};
handle_info({reply, From, I, Reply}, State = #{phase := {choose, _}, i := I, name := From}) ->
    handle_reply(Reply, From, State);
handle_info({reply, _, I, _}, State = #{i := I, phase := {choose, _}}) ->
    {noreply, State};
handle_info({reply, From, I, Reply}, State = #{i := I, peers := Peers}) ->
    case sets:is_element(From, Peers) of
        false ->
            {noreply, State};
        true ->
            handle_reply(Reply, From, State)
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_reply({ok, _, Chosen, chosen}, From, State = #{phase := {choose, _}, name := From, i := I}) ->
    stop({choose, I, Chosen}, State);
handle_reply({ok, _, Chosen, chosen}, _, State) ->
    enter_choose(Chosen, State);
handle_reply(_, _, State = #{phase := {choose, _}}) ->
    {noreply, State};
handle_reply({error, bad_instance}, From, State = #{phase := peek, errors := Errors}) ->
    check_error(State#{errors := sets:del_element(From, Errors)});
handle_reply({error, bad_instance}, From, State = #{waitings := Waitings, ready := Ready, errors := Errors}) ->
    check_error(
      State#{
        waitings := sets:del_element(From, Waitings),
        ready := sets:del_element(From, Ready),
        errors := sets:del_element(From, Errors)
       });
handle_reply(ok, _, State) ->
    {noreply, State};
handle_reply(Reply, From, State) ->
    case Reply1 = fix_promised(Reply) of
        {ok, {P, _} = Promised} ->
            ok;
        {ok, {P, _} = Promised, _} ->
            ok
    end,

    case State1 = update_accepted(Reply1, State) of
        #{promised := {N, _}, n := N1} when N > N1 ->
            ok;
        #{n := N} ->
            ok
    end,

    State2 =
        case P > N of
            true ->
                State1#{promised => Promised};
            false ->
                State1
        end,

    process_reply(Reply1, From, State2).


process_reply({ok, _}, _, State = #{phase := peek}) ->
    {noreply, State};
process_reply({ok, _, Accepted}, _, State = #{phase := peek}) ->
    enter_propose(Accepted, State);
process_reply({ok, {N, _}}, _, State = #{n := N1}) when N < N1->
    {noreply, State};
process_reply({ok, {N, _}, _}, _, State = #{n := N1}) when N < N1->
    {noreply, State};
process_reply({ok, Promised}, From, State = #{phase := {prepare, Promised}}) ->
    set_ready(From, State);
process_reply({ok, Promised, _}, From, State = #{phase := {prepare, Promised}}) ->
    set_ready(From, State);
process_reply({ok, _, {N,_,_}=A1}, From, State = #{phase := {propose, {N,_,_}=A2}}) ->
    A1 = A2,
    set_ready(From, State);
process_reply(_, From, State = #{waitings := Waitings, ready := Ready}) ->
    check_state(
      State#{
        waitings := sets:del_element(From, Waitings),
        ready := sets:del_element(From, Ready)
       }
     ).


fix_promised({ok, {N1, _}, {N2, Pid, _}=Accepted}) when N1 < N2 ->
    {ok, {N2, Pid}, Accepted};
fix_promised(Reply) ->
    Reply.


update_accepted({ok, _}, State) ->
    State;
update_accepted({ok, _, {N1,_,_}}, State=#{accepted := {N2,_,_}}) when N1 < N2 ->
    State;
update_accepted({ok, _, {N,_,_} = A1}, State=#{accepted := {N,_,_} = A2}) ->
    A1 = A2,
    State;
update_accepted({ok, _, Accepted}, State) ->
    State#{accepted => Accepted}.


set_ready(From, State = #{waitings := Waitings, ready := Ready}) ->
    case sets:is_element(From, Waitings) of
        false ->
            {noreply, State};
        true ->
            check_state(
              State#{
                waitings := sets:del_element(From, Waitings),
                ready := sets:add_element(From, Ready)
               }
             )
    end.


check_state(State = #{peers := Peers, ready := Ready, waitings := Waitings}) ->
    N = sets:size(Peers),
    R = sets:size(Ready),
    W = sets:size(Waitings),

    if 2 * R > N ->
            next_state(true, State);
       2 * (R + W) > N ->
            {noreply, State};
       true ->
            next_state(false, State)
    end.


check_error(State = #{peers := Peers, errors := Errors}) ->
    N = sets:size(Peers),
    E = sets:size(Errors),
    if 2 * E > N ->
            stop({error, bad_instance}, State);
       true ->
            case State of
                #{phase := peek} ->
                    {noreply, State};
                _ ->
                    check_state(State)
            end
    end.


next_state(true, State = #{phase := {propose, Value}}) ->
    enter_choose(Value, State);
next_state(true, State = #{phase := {prepare, {_, Self} = Promised}}) when Self =:= self() ->
    enter_propose(Promised, State);
next_state(true, State = #{phase := {prepare, _}, quorum_timeout := Timeout}) ->
    {noreply, reset_timer(Timeout, State)};
next_state(false, State = #{phase := {propose, {N1, _, _}}, accepted := {N2, _, _} = Accepted}) when N2 > N1 ->
    enter_propose(Accepted, State);
next_state(false, State = #{n := N1, promised := {N2, _} = Promised}) when N2 > N1 ->
    enter_prepare(Promised, State);
next_state(false, State) ->
    enter_prepare(State).


enter_peek(State) ->
    State1 = send_request(peek, State),
    State1#{
      phase => peek,
      n => 0,
      errors => sets:new()
     }.



enter_prepare(State = #{promised := {N, _}, n := N1}) when N > N1->
    enter_prepare({new_number(N, State), self()}, State);
enter_prepare(State = #{n := N}) ->
    enter_prepare({new_number(N, State), self()}, State).

new_number(N, #{peers := Peers}) ->
    P = sets:size(Peers),
    N + random:uniform(2 * P * P).

enter_prepare(Promise = {N,_}, State = #{peers := Peers}) ->
    Req = {prepare, Promise},
    State1 = send_request(Req, State),

    {noreply,
     State1#{
       n => N,
       phase => Req,
       waitings => Peers,
       ready => sets:new(),
       errors => sets:new()
      }
    }.

enter_propose({N,Pid}, State = #{accepted := {_,_,Value}}) ->
    enter_propose({N,Pid,Value}, State);
enter_propose({N,Pid}, State = #{parent := Parent, name := Name, committed := Committed}) ->
    {ok, {Ref, Leave, Value}} = gen_fsm:sync_send_all_state_event(Parent, get_value),
    enter_propose(
      {N, Pid, {Ref, [{committed, Name, Committed}] ++ Leave, Value}},
      State);

enter_propose(Accepted = {N,_,_}, State = #{peers := Peers}) ->
    Req = {propose, Accepted},
    State1 = send_request(Req, State),

    {noreply,
     State1#{
       n => N,
       phase => Req,
       waitings => Peers,
       ready => sets:new(),
       errors => sets:new()
      }
    }.


enter_choose(Chosen, State) ->
    Req = {choose, Chosen},
    State1 = send_request(Req, State),
    {noreply,
     State1#{
       phase => Req
      }
    }.


shuffle([], _, Acc) ->
    [V || {_, V} <- lists:keysort(1, maps:to_list(Acc)) ];
shuffle([H|T], N, Acc) ->
    Acc1 =
        case random:uniform(N) of
            N ->
                Acc#{N => H};
            I ->
                O = maps:get(I, Acc),
                Acc#{I => H, N => O}
        end,
    shuffle(T, N+1, Acc1).

shuffle(List) ->
    shuffle(List, 1, #{}).

stop_timer(State = #{timer := Timer}) ->
    erlang:cancel_timer(Timer),
    receive
        {timeout, Timer, []} ->
            ok
    after 0 ->
            ok
    end,
    maps:remove(timer, State);
stop_timer(State) ->
    State.

reset_timer(Timeout, State) ->
    State1 = stop_timer(State),
    State1#{timer => erlang:start_timer(Timeout, self(), []) }.

send_request({choose, _} = Req, State = #{i := I, name := Name, quorum_timeout := Timeout}) ->
    State1 = reset_timer(Timeout, State),
    Name ! {request, self(), I, Req},
    State1;
send_request(Req, State = #{i := I, peers := Peers, timeout := Timeout}) ->
    State1 = reset_timer(Timeout, State),
    [P ! {request, self(), I, Req}
     || P <- shuffle(sets:to_list(Peers))],
    State1.

stop(Request, State = #{parent := Parent}) ->
    State1 = stop_timer(State),
    ok = gen_fsm:sync_send_event(Parent, Request, infinity),
    {stop, normal, State1}.
