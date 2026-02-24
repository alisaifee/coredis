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

================
Pool parameters
================

Standalone
    :class:`~coredis.pool.ConnectionPool`

Cluster
    :class:`~coredis.pool.ClusterConnectionPool`

Connection limits
==================
Connection pools will only allow up to :paramref:`~coredis.pool.ConnectionPool.max_connections``
connections to be running concurrently, and if more are requested the command will block until one becomes
available. Since most commands can be multiplexed over a few connections this is rare
in practice unless you're using many pipelines/blocking commands/pubsubs simultaneously.

In the following example, a client is created with ``max_connections`` set to ``8``,
however ``10`` blocking requests are concurrently started. This means ``2`` requests will
block and only start after ``2`` other requests complete. In terms of wall clock time this means
all requests will complete in ``6`` seconds::

    import asyncio
    import coredis

    async def test():
        client = coredis.Redis(max_connections=8)
        # or with cluster
        # client = coredis.RedisCluster(
        #   "localhost", 7000,
        #   max_connections=8, max_connections_per_node=True
        # )

        async with client:
            results = await asyncio.gather(
                *[client.blpop(["fubar"], 3) for _ in range(10)],
            )

    asyncio.run(test())


Changing ``max_connections`` to ``10`` will result in all requests starting immediately

Timeouts
========

Connection pools can be created with an optional blocking :paramref:`~coredis.pool.ConnectionPool.timeout`
parameter that will control how long to wait for an available connection before raising an :exc:`TimeoutError`.

.. important:: To configure the timeout when using an implicit connection pool owned by the :class:`~coredis.Redis` or
   :class:`~coredis.RedisCluster` clients, use the :paramref:`~coredis.Redis.pool_timeout` argument.

Reusing the previous example with a low timeout will result in the ``2`` blocked requests actually failing
before they can even get a chance to send the request to redis::


    import asyncio
    import coredis

    async def test():
        client = coredis.Redis(max_connections=8, pool_timeout=1)
        async with client:
            result = await asyncio.gather(
              *[client.blpop(["fubar"], 3) for _ in range(10)],
            )


    asyncio.run(test())

==========================
Sharing a connection pool
==========================
Connection pools can also be shared between multiple clients through the :paramref:`coredis.Redis.connection_pool`
or :paramref:`coredis.RedisCluster.connection_pool` parameter::

    import asyncio
    from typing import Any

    import coredis
    from coredis.connection import TCPLocation

    async def test() -> None:
        pool = coredis.pool.ConnectionPool(location=TCPLocation("localhost", port=6379), max_connections=8)
        client1 = coredis.Redis(connection_pool=pool)
        client2 = coredis.Redis(connection_pool=pool)

        async with pool:
            async with client1, client2:
              assert await client1.client_id() == await client2.client_id()

    asyncio.run(test())

.. important:: When sharing a connection pool that will be used by different concurrent tasks
  **ALWAYS** enter the connection pool's async context manager in the parent before sharing it.
  This will ensure that the connection pool's lifecycle is maintained correctly and not tied to
  any child task::

    from typing import Any
    import asyncio
    import coredis
    from coredis.connection import TCPLocation

    async def worker(pool: coredis.pool.ConnectionPool[Any]) -> None:
       async with coredis.Redis(connection_pool=pool) as client:
           while True:
               await client.ping()

    async def start_workers():
        pool = coredis.ConnectionPool(location=TCPLocation("localhost", 6379))
        # Entering the pool here means it's lifetime is now managed here
        async with pool:
            await asyncio.gather(*(worker(pool) for _ in range(1024)))

    asyncio.run(start_workers())


.. danger:: As an anti pattern (i.e. **DON'T DO THIS**), consider the following example where the parent starting
  the workers does not own the lifecycle of the pool. This means that there is no longer
  any reliable way to ensure that the pool is properly managed while all workers are running::


    import asyncio
    from typing import Any
    import random

    import coredis
    from coredis.connection import TCPLocation

    async def worker(pool: coredis.pool.ConnectionPool[Any]) -> None:
        # The pool might not have been entered and the client below
        # might be the first one initializing it.
        async with coredis.Redis(connection_pool=pool) as client:
            while True:
                await client.ping()
                if random.random() < 0.1:
                    break

    async def start_workers():
       pool = coredis.ConnectionPool(location=TCPLocation("localhost", 6379))
       await asyncio.gather(*(worker(pool) for _ in range(1024)))

    asyncio.run(start_workers())

Connection types
----------------
coredis ships with three types of connections.

- The default, :class:`coredis.connection.TCPConnection`, is a normal TCP socket-based connection.

- :class:`~coredis.connection.UnixDomainSocketConnection` allows
  for clients running on the same device as the server to connect via a Unix domain socket.
  To use a :class:`~coredis.connection.UnixDomainSocketConnection` connection,
  simply pass the :paramref:`~coredis.Redis.unix_socket_path` argument,
  which is a string to the unix domain socket file.

  Additionally, make sure the parameter is defined in your redis.conf file. It's
  commented out by default.

  ::

      r = coredis.Redis(unix_socket_path='/tmp/redis.sock')

- :class:`~coredis.connection.ClusterConnection` connection which is essentially
  just :class:`~coredis.connection.Connection` with the exception of ensuring appropriate
  ``READONLY`` handling is set if configured (:paramref:`coredis.RedisCluster.readonly`)


=========================
Custom connection classes
=========================
You can create your own connection subclasses by deriving from
:class:`~coredis.connection.BaseConnection` as well. This may be useful if
you want to control the socket behavior within an async framework. To
instantiate a client class using your own connection, you need to create
a connection pool, passing your class to the connection_class argument.
Other keyword parameters you pass to the pool will be passed to the class
specified during initialization.

::

    pool = coredis.pool.ConnectionPool(connection_class=YourConnectionClass, ...)
