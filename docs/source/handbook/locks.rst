Locks
-----
:mod:`coredis.lock`

``coredis`` ships with an implementation (:class:`~coredis.lock.Lock`) of distributed locking that can be used
with a single redis instance or a cluster. The lock object can either be used as an async
context manager or acquired explicitly using the :meth:`~coredis.lock.Lock.acquire` method (
and subsequently released using :meth:`~coredis.lock.Lock.release`).

For convenience, factory methods :meth:`coredis.Redis.lock` & :meth:`coredis.RedisCluster.lock`
are available in the clients.

As an example, let's try to implement an atomic increment operation using a
distributed lock

.. code-block:: python

    import asyncio
    import coredis

    async def increment(client: coredis.Redis, key: str) -> int:
        async with client.lock(f"increment:{key}") as lock:
            value = int(await client.get(key) or 0)
            await client.set(key, int(value)+1)

    async def test():
        async with coredis.Redis() as client:
            await client.delete(["fubar"])
            await asyncio.gather(
                *(increment(client, "fubar") for _ in range(64))
            )
            assert int(await client.get("fubar")) ==  64

    asyncio.run(test())

Implementation
^^^^^^^^^^^^^^

The implementation is based on `the distributed locking pattern described in redis docs <https://redis.io/docs/latest/develop/use/patterns/distributed-locks/>`__

When used with a :class:`~coredis.RedisCluster` instance, acquiring the lock includes
ensuring that the token set by the :meth:`~coredis.lock.Lock.acquire` method
is replicated to atleast ``n/2`` replicas using the :meth:`~coredis.RedisCluster.ensure_replication`
context manager.

The implementation uses the following LUA scripts:

#. Release the lock

   .. literalinclude:: ../../../coredis/lock/lua/release.lua

#. Extend the lock

   .. literalinclude:: ../../../coredis/lock/lua/extend.lua

