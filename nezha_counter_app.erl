-module(nezha_counter_app).

-behaviour(application).

-export([start/0, start/2, stop/1]).


start() ->
    application:ensure_all_started(nezha_counter).

start(_StartType, _StartArgs) ->
    nezha_counter_sup:start_link().

stop(_State) ->
    ok.
