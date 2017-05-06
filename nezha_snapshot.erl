-module(nezha_snapshot).

-export([get/1, load/2]).


get({Mod, Pid}) ->
    Mod:get(Pid).

load({Mod, Pid}, Snapshots) ->
    Mod:load(Pid, Snapshots).
