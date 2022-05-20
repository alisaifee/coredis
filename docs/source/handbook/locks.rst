Locks
-----

Distributed Locking
^^^^^^^^^^^^^^^^^^^

There are two kinds of `Lock class` available:

- :class:`~coredis.lock.Lock` [Using :ref:`handbook/pipelines:pipelines`]
- :class:`~coredis.lock.LuaLock` [Using :ref:`handbook/scripting:lua scripts`]

A lock can be acquired using the :meth:`coredis.Redis.lock` or :meth:`coredis.RedisCluster.lock`
methods.

For example:

.. code-block:: python

   async def example():
       client = coredis.Redis()
       await client.flushall()
       lock = client.lock('lalala')
       print(await lock.icquire())
       # True
       print(await lock.acquire(blocking=False))
       # False
       print(await lock.release())
       # None
       try:
           await lock.release()
       except LockError as err:
           print(err)
           # coredis.exceptions.LockError: Cannot release an unlocked lock


Cluster Lock
^^^^^^^^^^^^

eclass:`~coredis.lock.ClusterLock` is supposed to solve distributed lock problem
in redis cluster. Since high availability is provided by redis cluster using primary-replica model,
the kind of lock aims to solve the fail-over problem referred in distributed lock
post given by redis official.

Quoting the documentation from the original author of :pypi:`aredis`:

    Why not use Redlock algorithm provided by official directly?

    It is impossible to make a key hashed to different nodes
    in a redis cluster and hard to generate keys
    in a specific rule and make sure they do not migrated in cluster.
    In the worst situation, all key slots may exists in one node.
    Then the availability will be the same as one key in one node.

    For more discussion please see:
    https://github.com/NoneGG/aredis/issues/55

    To gather more ideas i also raise a problem in stackoverflow:
    Not_a_Golfer's solution is awesome, but considering the migration problem, i think this solution may be better.
    https://stackoverflow.com/questions/46438857/how-to-create-a-distributed-lock-using-redis-cluster

    My solution is described below:

    1. random token + SETNX + expire time to acquire a lock in cluster master node

    2. if lock is acquired successfully then check the lock in replica nodes(may there be N replica nodes)
    using READONLY mode, if N/2+1 is synced successfully then break the check and return True,
    time used to check is also accounted into expire time

    3.Use lua script described in redlock algorithm to release lock
    with the client which has the randomly generated token,
    if the client crashes, then wait until the lock key expired.

    Actually you can regard the algorithm as a primary-replica version of redlock,
    which is designed for multi master nodes.

    Please read these article below before using this cluster lock in your app.

    - https://redis.io/topics/distlock
    - http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
    - http://antirez.com/news/101

.. code-block:: python

    async def example():
        client = coredis.RedisCluster("localhost", 7000)
        await client.flushall()
        lock = client.lock('lalala', lock_class=ClusterLock, timeout=1)
        print(await lock.acquire())
        # True
        print(await lock.acquire(blocking=False))
        # False
        print(await lock.release())
        # None
        try:
            await lock.release()
        except LockError as err:
            print(err)
            # coredis.exceptions.LockError: cannot release an unlocked lock


