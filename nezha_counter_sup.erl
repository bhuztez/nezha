-module(nezha_counter_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Name = nezha_acceptor,
    {ok, Nodes} = application:get_env(nezha_counter, nodes),
    Log = nezha_mem_log,
    Proxy = nezha_proxy,
    Acc = nezha_acceptor,
    Snapshot = nezha_counter_snapshot,
    Srv = nezha_counter,
    Prop = nezha_proposer,
    Gw = nezha_gateway,

    {ok,
     {#{ strategy  => one_for_one,
         intensity => 10,
         period    => 1},
      [#{id => Log,
         start => { Log, start_link, [Log] }
        },

       #{id => Proxy,
         start => { Proxy, start_link, [Proxy, Log, 500] }
        },

       #{id => Acc,
         start =>
             { Acc, start_link,
               [ {Name, node()},
                 [ {Name, Node} || Node <- Nodes ],
                 {Log, Proxy}
               ]
             }
        },

       #{id => Snapshot,
         start => { Snapshot, start_link, [Snapshot] }
        },

       #{id => Srv,
         start => { Srv, start_link, [Srv, Snapshot] }
        },

       #{id => Prop,
         start =>
             { Prop, start_link,
               [ Prop,
                 {Name, node()},
                 {Srv, Srv},
                 {Snapshot, Snapshot}
               ]
             }
        },

       #{id => Gw,
         start =>
             { Gw, start_link,
               [counter, Prop, Srv]
             }
        }

      ]
     }
    }.
