Connections
^^^^^^^^^^^

Connection Pools
----------------

Both :class:`~coredis.Redis` and :class:`~coredis.RedisCluster` are backed by a connection
pool that manages the underlying connections to the Redis server(s). **coredis** connection
pools are blocking and multiplex most kinds of commands over a few connections, while
allocating dedicated connections to blocking commands, pubsub instances, and pipelines.

To explicitly select the type of connection pool used pass in the appropriate class as
:paramref:`coredis.Redis.connection_pool_cls` or :paramref:`coredis.RedisCluster.connection_pool_cls`.

Connection pools can also be shared between multiple clients through the :paramref:`coredis.Redis.connection_pool`
or :paramref:`coredis.RedisCluster.connection_pool` parameter::

    import coredis

    pool = coredis.ConnectionPool(max_connections=8)
    client1 = coredis.Redis(connection_pool=pool)
    client2 = coredis.Redis(connection_pool=pool)

    async with pool:
        async with client1, client2:
            assert await client1.client_id() == await client2.client_id()

===============
Connection Pool
===============

Standalone
    :class:`~coredis.pool.ConnectionPool`

Cluster
    :class:`~coredis.pool.ClusterConnectionPool`

Connection pools will only allow up to ``max_connections`` connections to be running
concurrently, and if more are requested the command will block until one becomes
available. Since most commands can be multiplexed over a few connections this is rare
in practice unless you're using many pipelines/blocking commands/pubsubs simultaneously.

In the following example, a client is created with ``max_connections`` set to ``8``,
however ``10`` blocking requests are concurrently started. This means ``2`` requests will
block::

    import coredis
    import asyncio
    from anyio import fail_after

    async def test():
        client = coredis.Redis(max_connections=8)
        # or with cluster
        # client = coredis.RedisCluster(
        #   "localhost", 7000,
        #   max_connections=8, max_connections_per_node=True
        # )

        async with client:
            with fail_after(4):
                results = await asyncio.gather(
                    *[client.blpop(["fubar"], 3) for _ in range(10)],
                    return_exceptions=True
                )

    asyncio.run(test())


Changing ``max_connections`` to ``10`` will result in all requests succeeding::

    import coredis
    import asyncio
    from anyio import fail_after

    async def test():
        client = coredis.Redis(max_connections=10)
        # or with cluster
        # client = coredis.RedisCluster(
        #   "localhost", 7000,
        #   max_connections=10, max_connections_per_node=True
        # )

        async with client:
            with fail_after(4):
                results = await asyncio.gather(
                    *[client.blpop(["fubar"], 3) for _ in range(10)],
                    return_exceptions=True
                )

    asyncio.run(test())

Connection types
----------------
coredis ships with three types of connections.

- The default, :class:`coredis.connection.Connection`, is a normal TCP socket-based connection.

- :class:`~coredis.connection.UnixDomainSocketConnection` allows
  for clients running on the same device as the server to connect via a Unix domain socket.
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

    pool = coredis.ConnectionPool(connection_class=YourConnectionClass, ...)
