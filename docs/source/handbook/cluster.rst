Redis Cluster
-------------
If your infrastructure contains a :term:`Redis Cluster`, **coredis** provides
a :class:`~coredis.RedisCluster` client than can be used with the same API
as :class:`~coredis.Redis` but with awareness of routing the operations
to the appropriate node in the cluster.

Request routing
^^^^^^^^^^^^^^^

For operations that operate on single keys the :class:`~coredis.RedisCluster` client
simply routes the command to the appropriate node that is serving the slot that contains the key.

There are other categories of operations that cannot **simply** be used with redis cluster
by just routing it to the appropriate node by a single key (such as commands that don't contain keys, or
contain multiple keys). **coredis** uses a few routing strategies detailed below, to handle some of these
operations so that the methods in :class:`~coredis.RedisCluster` can be used in a consistent manner.

.. danger:: For write operations that interact with multiple slots, this means that even if **coredis** is
   able to distribute the operation through sub requests - the actual atomicity of the request is lost
   as a failure on one node will leave the successful nodes in an inconsistent state. If the risk of this
   scenario is unacceptable the automatic routing for cross slot commands can be disabled by setting
   :paramref:`~coredis.RedisCluster.non_atomic_cross_slot` to ``False``.

.. include:: cluster_routing.rst

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
    from coredis.connection import TCPLocation

    async def test():
        async with RedisCluster(startup_nodes=[TCPLocation("127.0.0.1", 7000)]) as client:
            with client.ensure_replication(replicas=2):
                await client.set("fubar", 1)

    asyncio.run(test())
