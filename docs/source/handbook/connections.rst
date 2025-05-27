Connections
^^^^^^^^^^^

Connection Pools
----------------

Both :class:`~coredis.Redis` and :class:`~coredis.RedisCluster` are backed by a connection
pool that manages the underlying connections to the redis server(s). **coredis** supports
both blocking and non-blocking connection pools. The default pool that is allocated is a
non-blocking connection pool.

To explicitly select the type of connection pool used pass in the appropriate class as
:paramref:`coredis.Redis.connection_pool_cls` or :paramref:`coredis.RedisCluster.connection_pool_cls`.

Connection pools can also be shared between multiple clients through the :paramref:`coredis.Redis.connection_pool`
or :paramref:`coredis.RedisCluster.connection_pool` parameter.

============================
Non-Blocking Connection Pool
============================

Standalone
    :class:`~coredis.pool.ConnectionPool`

Cluster
    :class:`~coredis.pool.ClusterConnectionPool`

The default non-blocking connection pools that are allocated to clients will only allow
upto ``max_connections`` connections to be acquired concurrently, and if more are requested
they will raise an exception.

In the following example, a client is created with ``max_connections`` set to ``2``, however ``10``
blocking requests are concurrently started. This means ~ ``8`` requests will fail::

    import coredis
    import asyncio

    async def test():
        client = coredis.Redis(max_connections=2)
        # or with cluster
        # client = coredis.RedisCluster(
        #   "localhost", 7000,
        #   max_connections=2, max_connections_per_node=True
        # )

        await client.set("fubar", 1)
        results = await asyncio.gather(
            *[client.get("fubar") for _ in range(10)],
            return_exceptions=True
        )
        print(len([r for r in results if isinstance(r, Exception)]))
        assert len([r for r in results if isinstance(r, Exception)]) == 8

    asyncio.run(test())


Changing ``max_connections`` to ``10`` will result in all requests succeeding::

    import coredis
    import asyncio

    async def test():
        client = coredis.Redis(max_connections=10)
        # or with cluster
        # client = coredis.RedisCluster(
        #   "localhost", 7000,
        #   max_connections=2, max_connections_per_node=True
        # )

        await client.set("fubar", 1)
        results = await asyncio.gather(
            *[client.get("fubar") for _ in range(10)],
            return_exceptions=True
        )
        assert len([r for r in results if isinstance(r, Exception)]) == 0

    asyncio.run(test())

========================
Blocking Connection Pool
========================

Standalone
    :class:`~coredis.pool.BlockingConnectionPool`

Cluster
    :class:`~coredis.pool.BlockingClusterConnectionPool`

Re-using the example from the :ref:`handbook/connections:non-blocking connection pool` section above,
but using the blocking variants of the connection pools for parameters :paramref:`coredis.Redis.connection_pool_cls` or :paramref:`coredis.RedisCluster.connection_pool_cls`
and setting ``max_connections`` to ``2`` will not result in any requests failing but instead blocking to re-use
the ``2`` connections in the pool::


    import coredis
    import asyncio

    async def test():
        client = coredis.Redis(
            connection_pool_cls=coredis.BlockingConnectionPool,
            max_connections=2
        )
        # or with cluster
        # client = coredis.RedisCluster(
        #    "localhost", 7000,
        #    connection_pool_cls=coredis.BlockingClusterConnectionPool,
        #    max_connections=2,
        #    max_connections_per_node=True
        # )

        await client.set("fubar", 1)
        results = await asyncio.gather(
            *[client.get("fubar") for _ in range(10)],
            return_exceptions=True
        )
        assert len([r for r in results if isinstance(r, Exception)]) == 0

    asyncio.run(test())

.. note:: For :class:`~coredis.pool.BlockingClusterConnectionPool` the
   :paramref:`~coredis.pool.BlockingClusterConnectionPool.max_connections_per_node`
   controls whether the value of :paramref:`~coredis.pool.BlockingClusterConnectionPool.max_connections`
   is used cluster wide or per node.

Connection types
----------------
coredis ships with three types of connections.

- The default, :class:`coredis.connection.Connection`, is a normal TCP socket based connection.

- :class:`~coredis.connection.UnixDomainSocketConnection` allows
  for clients running on the same device as the server to connect via a unix domain socket.
  To use a :class:`~coredis.connection.UnixDomainSocketConnection` connection,
  simply pass the :paramref:`~coredis.Redis.unix_socket_path` argument,
  which is a string to the unix domain socket file.

  Additionally, make sure the parameter is defined in your redis.conf file. It's
  commented out by default.

  .. code-block:: python

      r = coredis.Redis(unix_socket_path='/tmp/redis.sock')

- :class:`~coredis.connection.ClusterConnection` connection which is essentially
  just :class:`~coredis.connection.Connection` with the exception of ensuring appropriate
  ``READONLY`` handling is set if configured (:paramref:`coredis.RedisCluster.readonly`)


=========================
Custom connection classes
=========================
You can create your own connection subclasses by deriving from
:class:`coredis.connection.BaseConnection` as well. This may be useful if
you want to control the socket behavior within an async framework. To
instantiate a client class using your own connection, you need to create
a connection pool, passing your class to the connection_class argument.
Other keyword parameters you pass to the pool will be passed to the class
specified during initialization.

.. code-block:: python

    pool = coredis.ConnectionPool(connection_class=YourConnectionClass,
                                    your_arg='...', ...)


