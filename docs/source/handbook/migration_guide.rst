==========================
Migrating from 5.x to 6.0
==========================

Summary
=======

coredis ``6.0`` is a major architectural rewrite focused on structured concurrency,
predictable resource lifetimes, and safer defaults. While most APIs remain familiar,
several core behaviors have changed.

Key highlights
--------------

anyio integration
    All async operations now run on :mod:`anyio`. This enables structured concurrency,
    ensuring tasks, connections, and background resources are tied to explicit lifetimes.
    Many classes of bugs related to leaked connections or cancelled tasks are eliminated.
    Both :mod:`asyncio` and :mod:`trio` backends are supported.

Async context-managed classes
    All user-facing classes representing Redis concepts (clients, connection pools, Pub/Sub,
    pipelines, stream consumers, and Sentinel clients) now support the async context manager
    protocol and **must** be used as such. This is the only way to initialize and cleanup
    the underlying resources used by each class.

Pipeline auto-execution
    Pipelines execute automatically when their context exits. Explicit
    ``pipeline.execute()`` calls no longer exist.

:term:`RESP3` support only
    :term:`RESP` protocol version ``2`` support has been removed.

Simplified connection pools
    All connection pools now block when exhausted by default. Dedicated
    blocking pool variants have been removed.


Breaking Changes
================

Async Context Managers Required
-------------------------------

All user-facing classes that interact with Redis must be used as async context managers
to ensure proper initialization and cleanup.

.. tab:: 5.x

    ::

        import asyncio
        import coredis

        async def main():
            client = coredis.Redis(host="127.0.0.1", port=6379)
            await client.set("key", "1")

            # Explicit disconnection of connection pool
            client.connection_pool.disconnect()

        asyncio.run(main())

.. tab:: 6.0

    ::

        import anyio
        import coredis

        async def main():
            client = coredis.Redis(host="127.0.0.1", port=6379)
            async with client:
                await client.set("key", "1")
            # Client and connection pool are automatically cleaned up

        anyio.run(main, backend="asyncio")  # or "trio"


.. important::

   This applies to the following classes:

   * :class:`~coredis.Redis` and :class:`~coredis.RedisCluster`
   * :class:`~coredis.Sentinel`
   * :class:`~coredis.pool.ConnectionPool` and :class:`~coredis.pool.ClusterConnectionPool`
   * :class:`~coredis.patterns.pubsub.PubSub`
   * :class:`~coredis.patterns.pipeline.Pipeline`
   * :class:`~coredis.patterns.streams.Consumer` & :class:`~coredis.patterns.streams.GroupConsumer`


Pipeline
--------

Pipelines instances must be used as async context managers.
Execution happens automatically when the context exits.

.. important::

   * Pipeline instances can no longer be awaited and must be used as async context managers
   * Pipelines can no longer be reused
   * The ``watches`` parameter has been removed from the pipeline constructor
   * :meth:`~coredis.patterns.pipeline.Pipeline.watch` is now a context manager that
     handles :rediscommand:`MULTI`/:rediscommand:`EXEC` automatically
   * ``pipeline.execute()`` has been removed — pipelines execute automatically on
     context exit. Use :attr:`~coredis.patterns.pipeline.Pipeline.results`
     to retrieve full results or await individual commands.


Auto-execution on Context Exit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. tab:: 5.x

   ::

        import asyncio
        import coredis

        async def main():
            client = coredis.Redis()
            pipe = await client.pipeline()
            pipe.set("foo", 1)
            pipe.incr("foo")
            results = await pipe.execute()
            assert results == (True, 2)

        asyncio.run(main())


.. tab:: 6.0

   ::

        import anyio
        import coredis

        async def main():
            client = coredis.Redis()
            async with client:
                async with client.pipeline() as pipe:
                    pipe.set("foo", 1)
                    result = pipe.incr("foo")

                # Pipeline auto-executes when exiting context manager
                value = await result
                assert value == 2
                assert (True, 2) == pipe.results

        anyio.run(main)

Transaction Watches
^^^^^^^^^^^^^^^^^^^

.. tab:: 5.x

    ::

        import asyncio
        import coredis

        async def main():
            client = coredis.Redis()
            async with await client.pipeline(transaction=True) as pipe:
                await pipe.watch("key")
                # Note that in 5.x the pipe would switch to immediate
                # excecution mode once a watch was issued and was no longer
                # the 'pipeline'.
                value = await pipe.get("key")
                # Only after the multi is issued does the pipeline go back
                # to stacking commands.
                pipe.multi()
                pipe.set("key", int(value or 0) + 1)
                pipe.get("key")
                results = await pipe.execute()
                print(results[-1])

        asyncio.run(main())

.. tab:: 6.0

    ::

        import anyio
        import coredis

        async def main():
            client = coredis.Redis()
            async with client:
                async with client.pipeline(transaction=False) as pipe:
                    async with pipe.watch("key"):
                        # Just use the client for any immediate executions
                        value = await client.get("key")
                        pipe.set("key", int(value or 0) + 1)
                        value = pipe.get("key")
                    # Executes atomically when exiting watch context
                print(await value)
        anyio.run(main)

Connection Pool
---------------

.. important::

   * Connection pool instances must be used as async context managers
   * :class:`~coredis.pool.BlockingConnectionPool` removed — :class:`~coredis.pool.ConnectionPool` is now blocking
   * :class:`~coredis.pool.BlockingClusterConnectionPool` removed — :class:`~coredis.pool.ClusterConnectionPool` is now blocking
   * :exc:`TimeoutError` is now raised when a connection cannot be acquired from the pool
     within the :paramref:`~coredis.pool.ConnectionPool.timeout` instead of
     :exc:`~coredis.exceptions.ConnectionError`. (This would normally only happen if all
     the connections in the pool are being used for blocking commands, as non blocking commands
     are multiplexed over shared connections)

.. tab:: 5.x

    ::

        import coredis
        import asyncio

        from coredis import ConnectionPool, BlockingConnectionPool

        async def main():
            # Non-blocking pool
            client = coredis.Redis(connection_pool=ConnectionPool(max_connections=2))
            try:
                await asyncio.gather(*(client.blpop(["fubar"], timeout=1) for _ in range(3)))
            except coredis.exceptions.ConnectionError as err:
                print("Failed with too many connections attempted")

            # Blocking pool
            client = coredis.Redis(connection_pool=BlockingConnectionPool(max_connections=2))
            await asyncio.gather(*(client.blpop(["fubar"], timeout=1) for _ in range(3)))

            # Blocking pool with timeout
            client = coredis.Redis(connection_pool=BlockingConnectionPool(max_connections=2, timeout=1))
            try:
                await asyncio.gather(*(client.blpop(["fubar"], timeout=5) for _ in range(3)))
            except coredis.exceptions.ConnectionError as err:
                print("Failed with timeout")

        asyncio.run(main())

.. tab:: 6.0

    ::

        import coredis
        import asyncio

        async def main():
            async with coredis.Redis(
                connection_pool=coredis.pool.ConnectionPool(max_connections=2)
            ) as client:
                print(await asyncio.gather(*(client.blpop(["fubar"], timeout=1) for _ in range(3))))

            async with coredis.Redis(
                connection_pool=coredis.pool.ConnectionPool(max_connections=2, timeout=1)
            ) as client:
                try:
                    print(await asyncio.gather(*(client.blpop(["fubar"], timeout=5) for _ in range(3))))
                except TimeoutError:  # Note that a `TimeoutError` is raised instead of `ConnectionError`
                    print("Failed with timeout")

        asyncio.run(main())


Client-Side Caching
-------------------

.. important::

   * :class:`coredis.cache.TrackingCache` replaced by :class:`~coredis.patterns.cache.LRUCache`
   * Cache classes moved from :mod:`coredis.cache` to :mod:`coredis.patterns.cache`
   * ``max_size_bytes`` removed — only :paramref:`~coredis.patterns.cache.LRUCache.max_keys`
     is supported

.. tab:: 5.x

    ::

        import asyncio
        import coredis
        from coredis.cache import TrackingCache

        async def main():
            cache=TrackingCache(max_size_bytes=128 * 1024 * 1024)
            client = coredis.Redis(cache=cache)
            await client.set("fubar", 1)
            await client.get("fubar")
            await client.get("fubar")
            print(cache.stats)

        asyncio.run(main())

.. tab:: 6.0

    ::

        import anyio
        import coredis
        from coredis.patterns.cache import LRUCache

        async def main():
            cache=LRUCache(max_keys=10000)
            client = coredis.Redis(cache=cache)
            async with client:
              await client.set("fubar", 1)
              await client.get("fubar")
              await client.get("fubar")
              print(cache.stats)

        anyio.run(main)

PubSub
------

.. important::

   * PubSub classes must be used as async context managers
   * :meth:`~coredis.Redis.pubsub` now only accepts keyword arguments
   * Subscriptions are validated and may raise :exc:`TimeoutError` if an
     acknowledgement isn't received within the configured
     :paramref:`~coredis.patterns.pubsub.PubSub.subscription_timeout` (defaults to 1 second)

.. tab:: 5.x

    ::

        import asyncio
        import coredis

        async def main():
            client = coredis.Redis()
            pubsub = client.pubsub()
            await pubsub.subscribe("channel")
            async for message in pubsub:
                print(message)

        asyncio.run(main())

.. tab:: 6.0

    ::

        import anyio
        import coredis

        async def main():
            client = coredis.Redis()
            async with client:
                async with client.pubsub() as pubsub:
                    await pubsub.subscribe("channel")
                    async for message in pubsub:
                        print(message)

        anyio.run(main)

Stream Consumer
---------------

.. important::

   * Stream consumer instances must be async context managers
   * Stream consumers have moved from the :mod:`coredis.stream` module to
     :mod:`coredis.patterns.streams`. Update your imports accordingly

.. tab:: 5.x

    ::

        import asyncio
        import coredis
        from coredis.stream import Consumer

        async def main():
            client = coredis.Redis()
            consumer = await Consumer(client, streams=["one", "two"])
            stream, entry = await consumer.get_entry()

        asyncio.run(main())

.. tab:: 6.0

    ::

        import anyio
        import coredis

        async def main():
            async with coredis.Redis() as client:
                async with client.xconsumer(streams=["one", "two"]) as consumer:
                    stream, entry = await consumer.get_entry()
                    async for stream, entry in consumer:
                        print(stream, entry)

                async with client.xconsumer(
                    streams=["one", "two"],
                    group="group-a",
                    consumer="consumer-1",
                ) as group_consumer:
                    async for stream, entry in group_consumer:
                        print(stream, entry)

        anyio.run(main)

Sentinel
--------

.. important::

   :class:`~coredis.sentinel.Sentinel` instances must be used as async context managers

.. tab:: 6.0

    ::

        import anyio
        import coredis

        async def main():
            sentinel = coredis.Sentinel(sentinels=[("localhost", 26379)])
            async with sentinel:
                primary = sentinel.primary_for("svc")
                replica = sentinel.replica_for("svc")
                async with primary, replica:
                    await primary.set("fubar", 1)
                    await replica.get("fubar")

        anyio.run(main)

Scripting
---------

LUA Scripts
^^^^^^^^^^^

.. important::

   * Any script function stub wrapped with :meth:`coredis.commands.Script.wraps` must annotate key arguments with
     :class:`~coredis.typing.KeyT` to distinguish it from script args
   * Any function stub wrapped with :meth:`coredis.commands.Script.wraps` should be a synchronous function
     that returns an instance of the generic :class:`~coredis.commands.CommandRequest`

.. tab:: 5.x

   ::

      import asyncio
      import coredis

      async def main():
          client = coredis.Redis()

          @client.register_script("return {KEYS[1], ARGV[1]}").wraps(key_spec=["key"])
          async def echo_key_value(key: str, value: str) -> list[bytes]:
            ...

          k, v = await echo_key_value("co", "redis")
          print(f"{k!r}={v!r}")

      asyncio.run(main())

.. tab:: 6.0

   ::

      import asyncio
      import coredis
      from coredis.typing import KeyT
      from coredis.commands import CommandRequest

      async def main():
          client = coredis.Redis()

          @client.register_script("return {KEYS[1], ARGV[1]}").wraps()
          def echo_key_value(key: KeyT, value: str) -> CommandRequest[list[bytes]]:
            ...

          async with client:
              k, v = await echo_key_value("co", "redis")
              print(f"{k!r}={v!r}")

      asyncio.run(main())


Library Functions
^^^^^^^^^^^^^^^^^

.. important::

   * ``Library.wraps`` replaced by :func:`coredis.commands.function.wraps`
   * Any Library function type stub wrapped with ``wraps`` must annotate key arguments with
     :class:`~coredis.typing.KeyT` to distinguish it from script args
   * Any Library function stub wrapped with :meth:`coredis.commands.Script.wraps` should be a synchronous function
     that returns an instance of the generic :class:`~coredis.commands.CommandRequest`


.. tab:: 5.x

   ::

      import asyncio
      from typing import AnyStr

      import coredis
      from coredis.commands import CommandRequest, Library

      class EchoLib(Library[AnyStr]):
          NAME = "echolib"
          CODE = """
          #!lua name=echolib

          redis.register_function('echo_key_value', function(k, a)
            return {k[1], a[1]}
          end)
          """

          @Library.wraps("echo_key_value", key_spec=["key"])
          def echo_key_value(self, key: str, value: str): ...

      async def main():
          client = coredis.Redis()
          lib = await EchoLib(client, replace=True)
          print(await lib.echo_key_value("fubar", 1))


      asyncio.run(main())

.. tab:: 6.0

   ::

      import anyio
      from typing import AnyStr

      import coredis
      from coredis.commands import Library
      from coredis.commands.function import wraps
      from coredis.typing import KeyT, ValueT

      class EchoLib(Library[AnyStr]):
          NAME = "echolib"
          CODE = """
          #!lua name=echolib

          redis.register_function('echo_key_value', function(k, a)
            return {k[1], a[1]}
          end)
          """

          @wraps()
          def echo_key_value(self, key: KeyT, value: ValueT) -> CommandRequest[list[bytes]]: ...

      async def main():
          async with coredis.Redis() as client:
              lib = await EchoLib(client, replace=True)
              print(await lib.echo_key_value("fubar", 1))

      anyio.run(main)


Lock API
--------

.. important::

   * ``LuaLock`` moved from :mod:`coredis.recipes.locks` to :class:`~coredis.patterns.lock.Lock`
   * Use :meth:`~coredis.Redis.lock` convenience method for simpler access

.. tab:: 5.x

   ::

       import asyncio
       import coredis
       from coredis.recipes.locks import LuaLock

       async def main():
           client = coredis.Redis()
           async with LuaLock(client, "mylock", timeout=1.0):
               print("locked!")

       asyncio.run(main())

.. tab:: 6.0

   ::

       import anyio
       import coredis

       async def main():
           async with coredis.Redis() as client:
               async with client.lock("mylock", timeout=1.0):
                   print("locked!")

       anyio.run(main)


Removals
========

* RESP2 protocol support
* Monitor wrapper
* RedisGraph support


Migration Checklist
===================

* ☐ :class:`~coredis.Redis` and :class:`~coredis.RedisCluster` clients must be used as async context managers
* ☐ Replace all instances of ``coredis.BlockingConnectionPool`` and ``coredis.BlockingClusterConnectionPool``
  with :class:`~coredis.pool.ConnectionPool` or :class:`~coredis.pool.ClusterConnectionPool`.
* ☐ Pipelines must be used as async context managers; remove all explicit ``execute()`` calls.
* ☐ PubSub consumers must be used as async context managers
* ☐ Stream consumers must be used as async context managers
* ☐ Sentinel clients and derived primary/replica instances must be used as async context managers
* ☐ Replace ``coredis.cache.TrackingCache`` with :class:`~coredis.patterns.cache.LRUCache`
* ☐ Update cache imports from :mod:`coredis.cache` to :mod:`coredis.patterns.cache`
* ☐ Update stream consumer imports from :mod:`coredis.stream` to :mod:`coredis.patterns.streams`
* ☐ Update lock imports from :mod:`coredis.recipes.locks` to :mod:`coredis.patterns.lock`
* ☐ Replace all usage of ``BlockingConnectionPool`` / ``BlockingClusterConnectionPool`` with
  :class:`~coredis.pool.ConnectionPool` or :class:`~coredis.pool.ClusterConnectionPool`
* ☐ Add :class:`~coredis.typing.KeyT` type annotations to scripts and library function keys
* ☐ Replace ``Library.wraps`` with :func:`~coredis.commands.function.wraps`
* ☐ Replace ``LuaLock`` usage with :meth:`~coredis.Redis.lock` or :class:`~coredis.patterns.lock.Lock`
* ☐ Handle :exc:`TimeoutError` instead of :exc:`~coredis.exceptions.ConnectionError` for connection pool timeouts
