Clients
-------

.. autoclass:: coredis.Redis
   :class-doc-from: both
.. autoclass:: coredis.RedisCluster
   :class-doc-from: both

:mod:`coredis.sentinel`

.. autoclass:: coredis.sentinel.Sentinel
   :class-doc-from: both

Redis Command related types
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following classes and types are used in the internals of coredis
to wire arguments to Python command functions representing Redis commands
to the expected :term:`RESP` syntax and eventually send it to a connection  and back
to the client with a pythonic response mapped from the :term:`RESP` response

.. autoclass:: coredis.commands.CommandRequest
   :show-inheritance:
   :class-doc-from: both
.. autodata:: coredis.commands.CommandResponseT
.. autoclass:: coredis.typing.RedisCommandP
   :class-doc-from: both
.. autoclass:: coredis.typing.ExecutionParameters
   :class-doc-from: both
   :show-inheritance:
   :no-inherited-members:
