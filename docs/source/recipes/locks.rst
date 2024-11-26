Locks
-----
:mod:`coredis.recipes.locks`

Distributed lock with LUA Scripts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The implementation is based on `the distributed locking pattern described in redis docs <https://redis.io/docs/latest/develop/use/patterns/distributed-locks/>`__

When used with a :class:`~coredis.RedisCluster` instance, acquiring the lock includes
ensuring that the token set by the :meth:`~coredis.recipes.locks.LuaLock.acquire` method
is replicated to atleast ``n/2`` replicas using the :meth:`~coredis.RedisCluster.ensure_replication`
context manager.

The implementation uses the following LUA scripts:

#. Release the lock

   .. literalinclude:: ../../../coredis/recipes/locks/release.lua
#. Extend the lock

   .. literalinclude:: ../../../coredis/recipes/locks/extend.lua

.. autoclass:: coredis.recipes.locks.LuaLock
   :class-doc-from: both

