-module(nezha_server).

-export([commit/2, acquire/2, release/2, handle_request/3, call/2]).

commit({Mod, Pid}, I) ->
    Mod:commit(Pid, I).

acquire({Mod, Pid}, I) ->
    Mod:acquire(Pid, I).

release({Mod, Pid}, Ref) ->
    Mod:release(Pid, Ref).

handle_request({Mod, Pid}, From, Req) ->
    Mod:handle_request(Pid, From, Req).

call({Mod, Pid}, Req) ->
    Mod:call(Pid, Req).

