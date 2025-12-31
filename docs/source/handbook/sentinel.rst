Sentinel support
----------------

coredis can be used together with `Redis Sentinel <http://redis.io/topics/sentinel>`_
to discover Redis nodes. You need to have at least one Sentinel daemon running
in order to use coredis's Sentinel support.

Connecting coredis to the Sentinel instance(s) is easy. You can use a
Sentinel connection to discover the primary and replicas network addresses:

.. code-block:: python

    from coredis.sentinel import Sentinel
    sentinel = Sentinel([('localhost', 26379)], stream_timeout=0.1)
    async with sentinel:
        await sentinel.discover_primary('myredis')
        # ('127.0.0.1', 6379)
        await sentinel.discover_replicas('myredis')
        # [('127.0.0.1', 6380)]

You can also create Redis client connections from a Sentinel instance. You can
connect to either the primary (for write operations) or a replica (for read-only
operations).

.. code-block:: python

    primary = sentinel.primary_for('myredis', stream_timeout=0.1)
    replica = sentinel.replica_for('myredis', stream_timeout=0.1)
    async with primary, replica:
        await primary.set('foo', 'bar')
        await replica.get('foo')
        # 'bar'

The primary and replica objects are normal :class:`~coredis.Redis` instances with
their connection pool bound to the Sentinel instance via :class:`~coredis.sentinel.SentinelConnectionPool`.
When a Sentinel backed client attempts to establish a connection, it first queries the Sentinel servers to
determine an appropriate host to connect to. If no server is found,
a :exc:`~coredis.PrimaryNotFoundError` or :exc:`~coredis.ReplicaNotFoundError` is raised.
Both exceptions are subclasses of :exc:`~coredis.ConnectionError`.

When trying to connect to a replica client, the Sentinel connection pool will
iterate over the list of replicas until it finds one that can be connected to.
If no replicas can be connected to, a connection will be established with the
primary.

Further Reading
^^^^^^^^^^^^^^^
See `Guidelines for Redis clients with support for Redis Sentinel
<http://redis.io/topics/sentinel-clients>`_ to learn more about Redis Sentinel.
