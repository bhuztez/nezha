-module(nezha_acceptor).

-behaviour(gen_server).

-export([start_link/3, start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_base/1, set_base/3]).


get_base(Pid) ->
    gen_server:call(Pid, get_base, infinity).

set_base(Pid, Base, Data) ->
    gen_server:call(Pid, {set_base, Base, Data}, infinity).


start_link(Name, Peers, Log) ->
    start_link(Name, Peers, Log, []).

start_link(Name, Peers, Log, Options) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, [Name, Peers, Log], Options).

name({Name, Node}) when is_atom(Name), Node =:= node() ->
    Name;
name(Name) when is_atom(Name) ->
    Name.

init([Name, Peers, Log]) ->
    process_flag(trap_exit, true),

    case nezha_log:get_base(Log) of
        none ->
            {ok, Base, Data} = nezha_log:new_base(Peers),
            ok = nezha_log:set_base(Log, Base, Data);
        {ok, Base, _} ->
            ok
    end,

    {ok,
     #{name => Name,
       log => Log,
       base => Base,
       instances => dict:new(),
       workers => dict:new()
      }
    }.


handle_call(get_base, From = {Pid, _}, State = #{log := Log}) ->
    spawn_link(
      fun() ->
              link(Pid),
              gen_server:reply(From, nezha_log:get_base(Log))
      end),
    {noreply, State};
handle_call({set_base, Base, Data}, From = {Pid, _}, State = #{log := Log}) ->
    spawn_link(
      fun() ->
              link(Pid),
              gen_server:reply(From, nezha_log:set_base(Log, Base, Data))
      end),
    {noreply, State};
handle_call({set_entry, I, error}, _, State = #{name := Name, instances := Instances, base := Base}) ->
    #{queue := Queue} = Instance = dict:fetch(I, Instances),

    [ From ! {reply, Name, I, {error, bad_instance}}
      || {_, From} <- queue:to_list(Queue)
    ],

    State1 = set_instance(State, I, Instance#{queue := queue:new()}),
    {reply, ok, State1#{base := max(I+1, Base)}};
handle_call({set_entry, I, Entry}, From, State = #{name := Name, instances := Instances}) ->
    #{queue := Queue} = Instance = dict:fetch(I, Instances),

    Queue1 =
        lists:foldl(
          fun({Req, F}, Q) ->
                  case reply(Req, Entry) of
                      {ok, Reply} ->
                          F ! format(Reply, I, Name),
                          Q;
                      noreply ->
                          queue:in({Req, F}, Q)
                  end
          end,
          queue:new(),
          queue:to_list(Queue)
         ),

    case queue:is_empty(Queue1) of
        false ->
            Entry1 =
                lists:foldl(
                  fun ({Req, _}, E) ->
                          update(Req, E)
                  end,
                  Entry,
                  queue:to_list(Queue1)),

            {reply, {ok, Entry1}, set_instance(State, I, Instance#{last => Entry, queue := Queue1})};
        true ->
            {noreply, set_instance(State, I, Instance#{last => Entry, queue := Queue1, wait => From})}
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', Pid, _}, State = #{workers := Workers, instances := Instances, log := Log}) ->
    case dict:find(Pid, Workers) of
        error ->
            {noreply, State};
        {ok, I} ->
            Workers1 = dict:erase(Pid, Workers),

            Instance = #{queue := Queue} = dict:fetch(I, Instances),

            case queue:is_empty(Queue) of
                true ->
                    {noreply, State#{instances := dict:erase(I, Instances), workers := Workers1}};
                false ->
                    State1 = set_instance(State, I, maps:remove(wait, Instance)),
                    Pid = start_worker(Log, I),
                    {noreply, State1#{workers := dict:store(Pid, I, Workers1)}}
            end
    end;
handle_info({request, From, I, _Req}, State = #{base := Base, name := Name}) when I < Base ->
    From ! {reply, Name, I, {error, bad_instance}},
    {noreply, State};
handle_info({request, From, I, Req}, State = #{name := Name, instances := Instances, workers := Workers, log := Log}) ->
    State1 = #{instances := Instances1} =
        case dict:is_key(I, Instances) of
            true ->
                State;
            false ->
                Pid = start_worker(Log, I),
                State#{
                  workers := dict:store(Pid, I, Workers),
                  instances := dict:store(I, #{ queue => queue:new() }, Instances)
                 }
        end,

    case dict:fetch(I, Instances1) of
        #{last := Entry} = Instance ->
            case reply(Req, Entry) of
                {ok, Reply} ->
                    From ! format(Reply, I, Name),
                    {noreply, State1};
                noreply ->
                    {noreply, queue(State1, I, Instance, From, Req)}
            end;
        Instance ->
            {noreply, queue(State1, I, Instance, From, Req)}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_worker(Log, I) ->
    Self = self(),
    spawn_link(
      fun() ->
              case nezha_log:get_entry(Log, I) of
                  error ->
                      ok = gen_server:call(Self, {set_entry, I, error});
                  {ok, Entry} ->
                      worker(Self, Log, I, Entry)
              end
      end
     ).

worker(Parent, Log, I, Entry) ->
    {ok, Entry1} = gen_server:call(Parent, {set_entry, I, Entry}),
    ok = nezha_log:set_entry(Log, I, Entry1),
    worker(Parent, Log, I, Entry1).

queue(State, I, Instance=#{queue := Queue}, From, Req) ->
    Instance1 =
        case Instance of
            #{last := Entry, wait := Wait} ->
                gen_server:reply(Wait, {ok, update(Req, Entry)}),
                maps:remove(wait, Instance);
            _ ->
                Instance
        end,
    set_instance(State, I, Instance1#{queue := queue:in({Req, From}, Queue)}).


set_instance(State = #{instances := Instances}, I, Instance) ->
    State#{instances := dict:store(I, Instance, Instances)}.


%% instance() :: pos_integer().
%% proposal_number() :: pos_integer().
%% acceptor() :: atom() | {atom(), node()}
%% proposer() :: pid()
%% promised() :: {proposal_number(), proposer()}
%% accepted() :: {proposal_number(), proposer(), Value}
%% message

%% {request, proposer(), instance(), peek}
%% {request, proposer(), instance(), {prepare, promised()}}
%% {request, proposer(), instance(), {propose, accepted()}}
%% {request, proposer(), instance(), {choose, accepted()}}

%% {reply, acceptor(), instance(), ok}
%% {reply, acceptor(), instance(), {ok, promised()}}
%% {reply, acceptor(), instance(), {ok, promised(), accepted()}}
%% {reply, acceptor(), instance(), {ok, promised(), accepted(), chosen}}
%% {reply, acceptor(), instance(), {error, Reason}}


reply(peek, Entry) ->
    {ok, Entry};
reply({prepare, {N, _} = Req}, Entry) ->
    case promised(Entry) of
        {N1, _} when N1 >= N ->
            {ok, Entry};
        _ ->
            case Entry of
                #{chosen := _} ->
                    {ok, Entry#{promised => Req}};
                _ ->
                    noreply
            end
    end;
reply({propose, {N, Pid, _} = Req}, Entry) ->
    case accepted(Entry) of
        {N, _, _} = Req1 ->
            Req = Req1,
            {ok, Entry};
        _ ->
            case promised(Entry) of
                {N1, _} when N1 > N ->
                    {ok, Entry};
                Promised ->
                    Entry1 =
                        case Promised of
                            {N, _} ->
                                Entry#{accepted => Req};
                            _ ->
                                Entry#{promised => {N, Pid}, accepted => Req}
                        end,
                    case Entry of
                        #{chosen := {_, _, Value}} ->
                            Req = {N, Pid, Value},
                            {ok, Entry1};
                        _ ->
                            noreply
                    end
            end
    end;
reply({choose, {N, Pid, _} = Chosen}, Entry) ->
    case Entry of
        #{chosen := {_, _, Value}} ->
            {N, Pid, Value} = Chosen,
            {ok, Entry};
        _ ->
            noreply
    end.


update({prepare, {N, _} = Req}, Entry) ->
    case promised(Entry) of
        {N1, _} when N1 >= N ->
            Entry;
        _ ->
            case Entry of
                #{chosen := _} ->
                    Entry;
                _ ->
                    Entry#{promised => Req}
            end
    end;
update({propose, {N, Pid, _} = Req}, Entry) ->
    case accepted(Entry) of
        {N, _, _} = Req1 ->
            Req = Req1,
            Entry;
        _ ->
            case promised(Entry) of
                {N1, _} when N1 > N ->
                    Entry;
                Promised ->
                    case Entry of
                        #{chosen := {_,_,Value}} ->
                            Req = {N, Pid, Value},
                            Entry;
                        _ ->
                            case Promised of
                                {N, _} ->
                                    Entry#{accepted => Req};
                                _ ->
                                    Entry#{promised => {N, Pid}, accepted => Req}
                            end
                    end
            end
    end;
update({choose, {N, Pid, _} = Chosen}, Entry) ->
    case Entry of
        #{chosen := {_, _, Value}} ->
            {N, Pid, Value} = Chosen,
            Entry;
        #{accepted := {N1, _, Value}} when N1 > N ->
            {N, Pid, Value} = Chosen,
            #{chosen => Chosen};
        _ ->
            #{chosen => Chosen}
    end.


promised(#{promised := P}) ->           P;
promised(#{chosen := {N, Pid, _}}) ->   {N, Pid};
promised(#{accepted := {N, Pid, _}}) -> {N, Pid};
promised(#{}) ->                        none.

accepted(#{accepted := Accepted}) -> Accepted;
accepted(#{chosen := Chosen}) ->     Chosen;
accepted(#{}) ->                     none.


format(Entry = #{chosen := _}, I, Name) ->
    {reply, Name, I, {ok, promised(Entry), accepted(Entry), chosen}};
format(Entry, I, Name) ->
    Reply =
        case promised(Entry) of
            none ->
                ok;
            Promised ->
                case accepted(Entry) of
                    none ->
                        {ok, Promised};
                    Accepted ->
                        {ok, Promised, Accepted}
                end
        end,
    {reply, Name, I, Reply}.
