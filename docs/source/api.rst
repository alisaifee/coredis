API Documentation
=================
.. currentmodule:: coredis

Clients
^^^^^^^
Redis
-----
.. autoclass:: Redis
   :class-doc-from: both
   :inherited-members:

Cluster
-------
.. autoclass:: RedisCluster
   :class-doc-from: both
   :inherited-members:


Sentinel
--------
.. currentmodule:: coredis.sentinel
.. autoclass:: Sentinel
   :class-doc-from: both
   :inherited-members:

Command Wrappers
^^^^^^^^^^^^^^^^

Certain commands and/or concepts in redis cannot be simply
accomplished by calling the pass through APIs and require some state
management. The following wrappers provide an abstraction layer to
simplify operations.


BitField Operations
-------------------

.. autoclass:: coredis.commands.bitfield.BitFieldOperation
   :no-inherited-members:
   :class-doc-from: both

PubSub
------
.. autoclass:: coredis.commands.pubsub.PubSub
   :no-inherited-members:
   :class-doc-from: both

Scripting
---------
.. autoclass:: coredis.commands.script.Script
   :no-inherited-members:
   :class-doc-from: both

Functions
---------
.. autoclass:: coredis.commands.function.Library
   :class-doc-from: both

.. autoclass:: coredis.commands.function.Function
   :class-doc-from: both

Pipelines
---------

.. autoclass:: coredis.commands.pipeline.Pipeline
   :class-doc-from: both

.. autoclass:: coredis.commands.pipeline.ClusterPipeline
   :class-doc-from: both


Monitor
-------
.. autoclass:: coredis.commands.monitor.Monitor

Connection Classes
^^^^^^^^^^^^^^^^^^
.. currentmodule:: coredis

All connection classes derive from the same base-class:

.. autoclass:: BaseConnection
   :class-doc-from: both

TCP Connection
--------------

.. autoclass:: Connection
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

Unix Domain Socket Connection
-----------------------------
.. autoclass:: UnixDomainSocketConnection
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

Cluster TCP Connection
----------------------
.. autoclass:: ClusterConnection
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

Sentinel Connection
-------------------
.. currentmodule:: coredis.sentinel

.. autoclass:: SentinelManagedConnection
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

.. currentmodule:: coredis

Connection Pools
^^^^^^^^^^^^^^^^
Connection Pool
---------------
.. autoclass:: ConnectionPool
   :class-doc-from: both

Blocking Connection Pool
------------------------
.. autoclass:: BlockingConnectionPool
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

Cluster Connection Pool
-----------------------
.. autoclass:: ClusterConnectionPool
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:

Sentinel Connection Pool
------------------------
.. currentmodule:: coredis.sentinel

.. autoclass:: SentinelConnectionPool
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:


Parsers
^^^^^^^

.. currentmodule:: coredis.parsers
.. autoclass:: PythonParser
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:
.. autoclass:: HiredisParser
   :class-doc-from: both
   :show-inheritance:
   :inherited-members:



Response Types
^^^^^^^^^^^^^^
In most cases the command API returns native python types as they are described
in the redis specification. In certain cases these are "lightly" typed using
:class:`~typing.NamedTuple` or :class:`~typing.TypedDict` for ease of documentation.

.. automodule:: coredis.response.types
   :no-inherited-members:
   :show-inheritance:


Utility Classes
^^^^^^^^^^^^^^^

.. currentmodule:: coredis

Enums
-----
.. autoclass:: PureToken
   :no-inherited-members:
   :show-inheritance:

Threaded Workers
----------------
.. autoclass:: coredis.commands.pubsub.PubSubWorkerThread
   :no-inherited-members:
   :show-inheritance:
.. autoclass:: coredis.commands.monitor.MonitorThread
   :no-inherited-members:
   :show-inheritance:

Locks
-----
.. autoclass:: coredis.lock.Lock
   :show-inheritance:
.. autoclass:: coredis.lock.LuaLock
   :show-inheritance:
.. autoclass:: coredis.lock.ClusterLock
   :show-inheritance:

Exceptions
^^^^^^^^^^

.. currentmodule:: coredis

Authentication & Authorization
------------------------------

.. autoexception:: AuthenticationFailureError
   :no-inherited-members:
.. autoexception:: AuthenticationRequiredError
   :no-inherited-members:
.. autoexception:: AuthorizationError
   :no-inherited-members:

Cluster
-------
.. autoexception:: AskError
   :no-inherited-members:
.. autoexception:: ClusterCrossSlotError
   :no-inherited-members:
.. autoexception:: ClusterDownError
   :no-inherited-members:
.. autoexception:: ClusterError
   :no-inherited-members:
.. autoexception:: ClusterTransactionError
   :no-inherited-members:
.. autoexception:: MovedError
   :no-inherited-members:
.. autoexception:: RedisClusterException
   :no-inherited-members:

Sentinel
--------
.. autoexception:: PrimaryNotFoundError
   :no-inherited-members:
.. autoexception:: ReplicaNotFoundError
   :no-inherited-members:

Scripting Errors
----------------
.. autoexception:: NoScriptError
   :no-inherited-members:
.. autoexception:: FunctionError
   :no-inherited-members:

General Exceptions
-------------------
.. autoexception:: BusyLoadingError
   :no-inherited-members:
.. autoexception:: CommandSyntaxError
   :no-inherited-members:
.. autoexception:: CommandNotSupportedError
   :no-inherited-members:
.. autoexception:: ConnectionError
   :no-inherited-members:
.. autoexception:: DataError
   :no-inherited-members:
.. autoexception:: ExecAbortError
   :no-inherited-members:
.. autoexception:: InvalidResponse
   :no-inherited-members:
.. autoexception:: LockError
   :no-inherited-members:
.. autoexception:: NoKeyError
   :no-inherited-members:
.. autoexception:: PubSubError
   :no-inherited-members:
.. autoexception:: ReadOnlyError
   :no-inherited-members:
.. autoexception:: RedisError
   :no-inherited-members:
.. autoexception:: ResponseError
   :no-inherited-members:
.. autoexception:: TimeoutError
   :no-inherited-members:
.. autoexception:: TryAgainError
   :no-inherited-members:
.. autoexception:: WatchError
   :no-inherited-members:
