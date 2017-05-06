=====
Nezha
=====

Nezha__ is a paxos library written in Erlang, named after a notable
character in the popular Chinese novel `FengShen YanYi`__, along with
`Erlang Shen`__. Nezha has 3 heads and 8 arms, so he must have
invented his own Paxos to decide what to do next. Nezha is like
ConCoord__, a library to help you create your own replicated state
machine, rather than a full-blown distributed key-value store like
Zookeeper__ and etcd__.

.. __: https://en.wikipedia.org/wiki/Nezha
.. __: https://en.wikipedia.org/wiki/Investiture_of_the_Gods
.. __: https://en.wikipedia.org/wiki/Erlang_Shen
.. __: https://github.com/denizalti/concoord
.. __: https://zookeeper.apache.org/
.. __: https://github.com/coreos/etcd

Nezha is not intended to be used in production, though pull requests
to make it ready for production are welcome. Instead, Nezha is mostly
for fast prototyping. Nezha takes a modular approach to help you focus
on your state machine. It has two modules, the server module and the
snapshot module. And they are independent of the log module, which
provides storage for the paxos log. And the crash of proposer process,
acceptor process or server process would not affect each others if it
is not caused by a bug.

.. code::

                                     +----------+   +-----+
                                 +-->| acceptor |-->| log |
                                 |   +----------+   +-----+
    +---------+    +----------+  |   +--------+   +----------+
    | gateway |--->| proposer |--+-->| server |-->|          |
    +---------+    +----------+  |   +--------+   | snapshot |
                                 +--------------->|          |
                                                  +----------+


Hopefully, in a long run, a proper interface between distributed
consensus library and state machine would be figured out, so that the
same implementation of state machine could be used with different
distributed consensus algorithms. Right now, only the most simple
variant of Paxos as described in `Paxos Made Simple`__ is implemented
in Nezha.

.. __: https://www.microsoft.com/en-us/research/publication/paxos-made-simple/


Log
===

log module should implement following functions

.. code::

    get_base(Pid)
    set_base(Pid, Base, Data)
    get_entry(Pid, I)
    set_entry(Pid, I, Value)


Snapshot
========

snapshot module should implement following functions

.. code::

    get(Pid)
    load(Pid, Snapshots)


Server
======

server module should implement following functions

.. code::

    commit(Pid, I)
    acquire(Pid, I)
    release(Pid, Ref)
    handle_request(Pid, From, Req)
    call(Pid, Req)


Counter
=======

:code:`nezha_counter` is a simple replicated counter.

To bootstrap a cluster with 3 nodes

.. code::

    ./start-node.sh a a b c
    ./start-node.sh b a b c
    ./start-node.sh c a b c

Later, join a new node

.. code::

    ./start-node.sh d a b c

In Erlang shell, run :code:`nezha_counter:get()` to get counter's value,
run :code:`nezha_counter:incr()` to increase counter by 1, run
:code:`nezha_counter:watch()` to print the new value when the value of
counter changes.

You may also run :code:`exit(whereis(nezha_proposer), kill)`,
:code:`exit(whereis(nezha_acceptor), kill)`,
:code:`exit(whereis(nezha_counter), kill)`, to kill proposer process,
acceptor process, server process. It should continue to work without
problem.
