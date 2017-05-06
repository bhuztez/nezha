-module(nezha_sync).

-behaviour(gen_server).

-export([start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Parent, Name, Peers, Timeout) ->
    gen_server:start_link(?MODULE, [Parent, Name, Peers, Timeout], []).


init([Parent, Name, Peers, Timeout]) ->
    process_flag(trap_exit, true),

    {ok,
     #{workers => dict:from_list([{start_worker(P), P} || P <- Peers]),
       finished => sets:new(),
       parent => Parent,
       name => Name,
       timer => erlang:start_timer(Timeout, self(), []),
       timeout => Timeout
      }
    }.


handle_call({set_result, Peer, Result = {N, _}}, From = {Pid, _}, State = #{workers := Workers, finished := Finished}) ->
    case dict:find(Pid, Workers) of
        error ->
            {reply, error, State};
        {ok, Peer} ->
            State1 =
                case State of
                    #{result := {N, _} = Result1} ->
                        Result1 = Result,
                        State;
                    #{result := {N1, _}} when N1 > N ->
                        State;
                    _ ->
                        State#{result => Result}
                end,
            Workers1 = dict:erase(Pid, Workers),
            State2 = State1#{workers := Workers1, finished := sets:add_element(Peer, Finished)},

            case dict:size(Workers1) of
                0 ->
                    gen_server:reply(From, ok),
                    finish(State2);
                _ ->
                    {reply, ok, State2}
            end
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, Ref, []}, State = #{timeout := Timeout, timer := Ref, workers := Workers, finished := Finished}) ->
    case dict:size(Workers) < sets:size(Finished) of
        true ->
            finish(State);
        false ->
            {noreply, State#{timer := erlang:start_timer(Timeout, self(), [])}}
    end;
handle_info({'EXIT', Pid, _}, State = #{workers := Workers, finished := Finished}) ->
    case dict:find(Pid, Workers) of
        error ->
            {noreply, State};
        {ok, Peer} ->
            case sets:is_element(Peer, Finished) of
                true ->
                    {noreply, State};
                false ->
                    {noreply, State#{workers := dict:store(start_worker(Peer), Peer, Workers)}}
            end
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_worker(Name) ->
    Self = self(),
    spawn_link(fun () -> worker(Self, Name) end).


worker(Parent, Name) ->
    {ok, Base, Data} = nezha_acceptor:get_base(Name, get_base),
    ok = gen_server:call(Parent, {set_result, Name, {Base, Data}}).


finish(State = #{parent := Parent, result := {Base, Data}, name := Name}) ->
    case nezha_acceptor:set_base(Name, Base, Data) of
        ok ->
            Base1 = Base,
            Data1 = Data;
        {error, Base1, Data1} ->
            ok
    end,
    ok = gen_fsm:send_event(Parent, {set_base, Base1, Data1}),
    {stop, normal, State}.
