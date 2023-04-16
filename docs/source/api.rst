:tocdepth: 3

API Documentation
=================

Clients
^^^^^^^

Redis
-----
.. autoclass:: coredis.Redis
   :class-doc-from: both

Cluster
-------
.. autoclass:: coredis.RedisCluster
   :class-doc-from: both


Sentinel
--------
:mod:`coredis.sentinel`

.. autoclass:: coredis.sentinel.Sentinel
   :class-doc-from: both

KeyDB
-----
.. autoclass:: coredis.KeyDB
   :no-inherited-members:

   .. automethod:: bitop
   .. automethod:: cron
   .. automethod:: expiremember
   .. automethod:: expirememberat
   .. automethod:: pexpirememberat
   .. automethod:: hrename
   .. automethod:: mexists
   .. automethod:: ttl
   .. automethod:: pttl
   .. automethod:: object_lastmodified

.. autoclass:: coredis.KeyDBCluster
   :no-inherited-members:

   .. automethod:: expiremember
   .. automethod:: expirememberat
   .. automethod:: pexpirememberat
   .. automethod:: hrename
   .. automethod:: mexists
   .. automethod:: ttl
   .. automethod:: pttl
   .. automethod:: object_lastmodified

Pipeline Support
^^^^^^^^^^^^^^^^
:mod:`coredis.pipeline`

:term:`Pipelining` and :term:`Transactions` are exposed by the following classes
that are returned by :meth:`coredis.Redis.pipeline` and :meth:`coredis.RedisCluster.pipeline`.
For examples refer to :ref:`handbook/pipelines:pipelines`.

.. autoclass:: coredis.pipeline.Pipeline
   :class-doc-from: both

.. autoclass:: coredis.pipeline.ClusterPipeline
   :class-doc-from: both

Stream Consumers
^^^^^^^^^^^^^^^^
:mod:`coredis.streams`

.. autoclass:: coredis.stream.Consumer
   :class-doc-from: both
   :show-inheritance:
   :special-members: __aiter__, __anext__

.. autoclass:: coredis.stream.GroupConsumer
   :class-doc-from: both
   :show-inheritance:
   :special-members: __aiter__, __anext__

.. autoclass:: coredis.stream.StreamParameters
   :show-inheritance:
   :no-inherited-members:

Command Wrappers
^^^^^^^^^^^^^^^^

:mod:`coredis.commands`

Certain commands and/or concepts in redis cannot be simply
accomplished by calling the pass through APIs and require some state
management. The following wrappers provide an abstraction layer to
simplify operations.

.. autosummary::

   ~coredis.commands.BitFieldOperation
   ~coredis.commands.PubSub
   ~coredis.commands.ClusterPubSub
   ~coredis.commands.ShardedPubSub
   ~coredis.commands.Script
   ~coredis.commands.Library
   ~coredis.commands.Function
   ~coredis.commands.Monitor

BitField Operations
-------------------

.. autoclass:: coredis.commands.BitFieldOperation
   :no-inherited-members:
   :class-doc-from: both

PubSub
------
.. autosummary::

   ~coredis.commands.PubSub
   ~coredis.commands.ClusterPubSub
   ~coredis.commands.ShardedPubSub
   ~coredis.commands.pubsub.PubSubWorkerThread
   ~coredis.commands.pubsub.SubscriptionCallback

.. autoclass:: coredis.commands.PubSub
   :class-doc-from: both

.. autoclass:: coredis.commands.ClusterPubSub
   :class-doc-from: both

.. autoclass:: coredis.commands.ShardedPubSub
   :class-doc-from: both

.. autoclass:: coredis.commands.pubsub.PubSubWorkerThread
   :no-inherited-members:
   :show-inheritance:

.. autodata:: coredis.commands.pubsub.SubscriptionCallback


Scripting
---------
.. autoclass:: coredis.commands.Script
   :no-inherited-members:
   :class-doc-from: both
   :special-members: __call__


Functions
---------
.. autoclass:: coredis.commands.Library
   :class-doc-from: both

.. autoclass:: coredis.commands.Function
   :class-doc-from: both
   :special-members: __call__

Monitor
-------
.. autoclass:: coredis.commands.Monitor
.. autoclass:: coredis.commands.monitor.MonitorThread
   :no-inherited-members:
   :show-inheritance:

Modules
^^^^^^^
:mod:`coredis.modules`

Redis module commands in :mod:`coredis` are exposed under properties
of the :class:`~coredis.Redis` or :class:`~coredis.RedisCluster` clients
such as :attr:`~coredis.Redis.json`, :attr:`~coredis.Redis.bf`. These properties
in turn return instances of the module command group containers which are bound
by a weak reference to the client itself.

The module commands can also be accessed by instantiating instances
of the module command group classes (listed in the sections below) directly.

To access the :class:`~coredis.modules.Json` command group from the :class:`coredis.modules.RedisJSON` module for example::

    import coredis

    client = coredis.Redis()

    # through the client
    await client.json.get("key", "$")
    # or directly
    json = coredis.modules.Json(client)
    await json.get("key", "$")



RedisJSON
------
.. autoclass:: coredis.modules.Json

RedisBloom
----------

===========
BloomFilter
===========
.. autoclass:: coredis.modules.BloomFilter

============
CuckooFilter
============
.. autoclass:: coredis.modules.CuckooFilter

================
Count Min Sketch
================
.. autoclass:: coredis.modules.CountMinSketch

=======
TDigest
=======
.. autoclass:: coredis.modules.TDigest

====
TopK
====
.. autoclass:: coredis.modules.TopK

RedisSearch
-----------
====================
Search & Aggregation
====================

.. autoclass:: coredis.modules.Search

.. autoclass:: coredis.modules.search.Field
.. autoclass:: coredis.modules.search.Filter
.. autoclass:: coredis.modules.search.Group
.. autoclass:: coredis.modules.search.Reduce
.. autoclass:: coredis.modules.search.Apply

============
Autocomplete
============
.. autoclass:: coredis.modules.Autocomplete


TimeSeries
----------
.. autoclass:: coredis.modules.TimeSeries


Connection Pools
^^^^^^^^^^^^^^^^
.. currentmodule:: coredis

:mod:`coredis`


Connection Pool
---------------
.. autoclass:: coredis.pool.ConnectionPool
   :class-doc-from: both

Blocking Connection Pool
------------------------
.. autoclass:: coredis.pool.BlockingConnectionPool
   :class-doc-from: both
   :show-inheritance:

Cluster Connection Pool
-----------------------
.. autoclass:: coredis.pool.ClusterConnectionPool
   :class-doc-from: both
   :show-inheritance:

Blocking Cluster Connection Pool
--------------------------------
.. autoclass:: coredis.pool.BlockingClusterConnectionPool
   :class-doc-from: both
   :show-inheritance:

Sentinel Connection Pool
------------------------

.. autoclass:: coredis.sentinel.SentinelConnectionPool
   :class-doc-from: both
   :show-inheritance:



Connection Classes
^^^^^^^^^^^^^^^^^^
:mod:`coredis`

All connection classes derive from the same base-class:

.. autoclass:: coredis.connection.BaseConnection
   :show-inheritance:
   :class-doc-from: both

TCP Connection
--------------

.. autoclass:: coredis.connection.Connection
   :show-inheritance:
   :class-doc-from: both

Unix Domain Socket Connection
-----------------------------
.. autoclass:: coredis.connection.UnixDomainSocketConnection
   :show-inheritance:
   :class-doc-from: both

Cluster TCP Connection
----------------------
.. autoclass:: coredis.connection.ClusterConnection
   :show-inheritance:
   :class-doc-from: both

Sentinel Connection
-------------------

.. autoclass:: coredis.sentinel.SentinelManagedConnection
   :show-inheritance:
   :class-doc-from: both


Caching
^^^^^^^
:mod:`coredis.cache`

Built in caches
---------------

.. autoclass:: coredis.cache.TrackingCache
   :class-doc-from: both

.. autoclass:: coredis.cache.NodeTrackingCache
   :class-doc-from: both

.. autoclass:: coredis.cache.ClusterTrackingCache
   :class-doc-from: both

Implementing a custom cache
---------------------------
All caches accepted by :class:`~coredis.Redis` or :class:`~coredis.RedisCluster`
must implement :class:`~coredis.cache.AbstractCache`

.. autoclass:: coredis.cache.AbstractCache

Additionally, caches can opt in to additional features by implementing any of the
following protocols:

.. autoclass:: coredis.cache.SupportsClientTracking
.. autoclass:: coredis.cache.SupportsStats
.. autoclass:: coredis.cache.SupportsSampling

.. autoclass:: coredis.cache.CacheStats

Type Aliases
^^^^^^^^^^^^

:mod:`coredis.typing`

Input types
-----------
The API uses the following type aliases to describe the unions of acceptable types
for parameters to redis command wrappers.

.. autodata:: coredis.typing.KeyT
.. autodata:: coredis.typing.ValueT
.. autodata:: coredis.typing.StringT

For methods that accept non optional variable number of keys or values, coredis does **NOT**
use **positional** or **keyword varargs** and expects a "container" to be passed in for the argument.
Common examples of such APIs are :meth:`~coredis.Redis.delete` and :meth:`~coredis.Redis.exists`.

Instead of accepting :class:`~collections.abc.Iterable`, a union of select containers from the standard
library are accepted via :data:`~coredis.typing.Parameters`.

.. autodata:: coredis.typing.Parameters


Redis Response (RESP) descriptions
----------------------------------

The follow two types describe the total representation of parsed responses from the redis
serialization protocol(s) (RESP & RESP3) (See :ref:`handbook/response:redis response` for more details).

In most cases these are not exposed through the client API and are only meant
for internal pre-validation before the parsed response is transformed or narrowed
to the returns documented in the client API at :ref:`api:clients`.

.. autodata:: coredis.typing.ResponsePrimitive
.. autodata:: coredis.typing.ResponseType


Response Types
^^^^^^^^^^^^^^
In most cases the API returns native python types mapped as closely as possible
to the response from redis. The responses are normalized across RESP versions ``2`` and ``3``
to maintain a consistent signature (Most notable example of this is dictionary

In certain cases these are "lightly" typed using :class:`~typing.NamedTuple`
or :class:`~typing.TypedDict` for ease of documentation and in the case of "tuples"
returned by redis - to avoid errors in indexing.

:mod:`coredis.response.types`

.. automodule:: coredis.response.types
   :no-inherited-members:
   :show-inheritance:

:mod:`coredis.modules.response.types`

.. automodule:: coredis.modules.response.types
   :no-inherited-members:
   :show-inheritance:

Utility Classes
^^^^^^^^^^^^^^^

Enums
-----
:mod:`coredis.tokens`

.. autoclass:: coredis.tokens.PureToken
   :no-inherited-members:
   :show-inheritance:

Retries
-------
:mod:`coredis.retry`

Utilities for managing errors that can be recovered from by providing retry policies.

.. autoclass:: coredis.retry.ConstantRetryPolicy
.. autoclass:: coredis.retry.ExponentialBackoffRetryPolicy
.. autoclass:: coredis.retry.CompositeRetryPolicy
.. autofunction:: coredis.retry.retryable

All retry policies need to derive from :class:`coredis.retry.RetryPolicy`

.. autoclass:: coredis.retry.RetryPolicy

Exceptions
^^^^^^^^^^

:mod:`coredis.exceptions`

Authentication & Authorization
------------------------------

.. autoexception:: coredis.exceptions.AuthenticationFailureError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.AuthenticationRequiredError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.AuthorizationError
   :no-inherited-members:

Cluster Errors
--------------
.. autoexception:: coredis.exceptions.AskError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterCrossSlotError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterDownError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterResponseError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterRoutingError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ClusterTransactionError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.MovedError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.RedisClusterException
   :no-inherited-members:

Sentinel Errors
---------------
.. autoexception:: coredis.exceptions.PrimaryNotFoundError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ReplicaNotFoundError
   :no-inherited-members:

Scripting Errors
----------------
.. autoexception:: coredis.exceptions.NoScriptError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.FunctionError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.NotBusyError
   :no-inherited-members:


Stream Consumer Errors
----------------------
.. autoexception:: coredis.exceptions.StreamConsumerInitializationError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.StreamDuplicateConsumerGroupError
   :no-inherited-members:

General Exceptions
-------------------
.. autoexception:: coredis.exceptions.BusyLoadingError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.CommandSyntaxError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.CommandNotSupportedError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ModuleCommandNotSupportedError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ConnectionError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.DataError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ExecAbortError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.InvalidResponse
   :no-inherited-members:
.. autoexception:: coredis.exceptions.LockError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.NoKeyError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.PersistenceError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.PubSubError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ReadOnlyError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.RedisError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ReplicationError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.ResponseError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.TimeoutError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.TryAgainError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.UnblockedError
   :no-inherited-members:
.. autoexception:: coredis.exceptions.WatchError
   :no-inherited-members:
