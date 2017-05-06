-module(nezha_log).

-export([new_base/1, base_data_to_state/1, state_to_base_data/1,
         get_base/1, set_base/3,
         apply_entry/2, get_entry/2, set_entry/3]).

new_base(Peers) ->
    {ok, 1, [{P, {1, none}} || P <- ordsets:from_list(Peers)]}.

base_data_to_state(Peers) ->
    #{peers => dict:from_list(Peers),
      bases =>
          lists:foldl(
            fun({_, {X, _}}, T) -> increase(X, T) end,
            gb_trees:empty(),
            Peers)
     }.

state_to_base_data(#{peers := Peers}) ->
    orddict:from_list(dict:to_list(Peers)).

apply_entry({_, _, {_, Commands, _}}, State) ->
    lists:foldl(fun apply_command/2, State, Commands).


apply_command({leave, Name}, State = #{peers := Peers, bases := Bases}) ->
    case dict:find(Name, Peers) of
        error ->
            State;
        {ok, {X, _}} ->
            State#{
              peers := dict:erase(Name, Peers),
              bases := decrease(X, Bases)
             }
    end;
apply_command({committed, Name, {I, Snapshot}}, State = #{peers := Peers, bases := Bases}) ->
    case dict:find(Name, Peers) of
        {ok, {X, _}} when I < X ->
            State;
        _ ->
            case gb_trees:smallest(Bases) of
                {X, _} when I < X ->
                    State;
                _ ->
                    State#{
                      peers := dict:store(Name, {I, Snapshot}, Peers),
                      bases := increase(I, Bases)
                     }
            end
    end.

increase(Key, Tree) ->
    case gb_trees:lookup(Key, Tree) of
        none ->
            gb_trees:insert(Key, 1, Tree);
        {value, N} ->
            gb_trees:update(Key, N+1, Tree)
    end.

decrease(Key, Tree) ->
    case gb_trees:get(Key, Tree) of
        1 ->
            gb_trees:delete(Key, Tree);
        N ->
            gb_trees:update(Key, N-1, Tree)
    end.


get_base({Mod, Pid}) ->
    Mod:get_base(Pid).

set_base({Mod, Pid}, Base, Data) ->
    Mod:set_base(Pid, Base, Data).

get_entry({Mod, Pid}, I) ->
    Mod:get_entry(Pid, I).

set_entry({Mod, Pid}, I, Value) ->
    Mod:set_entry(Pid, I, Value).
