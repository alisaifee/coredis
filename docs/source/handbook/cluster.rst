Redis Cluster
-------------
If your infrastructure contains a :term:`Redis Cluster`, **coredis** provides
a :class:`coredis.RedisCluster` client than can be used with the same API
as :class:`coredis.Redis` but with awareness of distributing the operations
to the appropriate shards.

For operations that operate on single keys the client simply routes the command
to the appropriate node.

There are, however a few other categories of operations that cannot simply be used with redis cluster
by just routing it to the appropriate node. The API docs will call out any exceptional
handling that **coredis** performs to allow application developers to use the APIs transparently
when possible.

Examples of such APIs are:

- .. automethod:: coredis.RedisCluster.keys
     :noindex:

- .. automethod:: coredis.RedisCluster.exists
     :noindex:

- .. automethod:: coredis.RedisCluster.delete
     :noindex:

- .. automethod:: coredis.RedisCluster.script_load
     :noindex:

Replication
^^^^^^^^^^^

**coredis** supports ensuring synchronous replication of writes using the :rediscommand:`WAIT`
command. This is abstracted away with the :meth:`~coredis.RedisCluster.ensure_replication`
context manager.

The following example will ensure that the :rediscommand:`SET` is replicated to atleast 2 replicas within 100 milliseconds (default
value of :paramref:`~coredis.RedisCluster.ensure_replication.timeout_ms`),
else raise a :exc:`~coredis.exceptions.ReplicationError`::

    import asyncio
    from coredis import RedisCluster


    async def test():
        async with RedisCluster("localhost", 7000, startup_nodes=[...]) as client:
            with client.ensure_replication(replicas=2):
                await client.set("fubar", 1)

    asyncio.run(test())
