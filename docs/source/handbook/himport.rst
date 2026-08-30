Hash import
-----------
:mod:`coredis.patterns.himport`

Redis 8.10 introduced :rediscommand:`HIMPORT`, which lets you write many hashes
that share the same field names in a single operation, declaring the field names
once and sending only the values for each key. This reduces network traffic and
per-command overhead compared to individual :rediscommand:`HSET` calls, and Redis
can store the resulting hashes compactly with a shared field-name set.

``coredis`` exposes this through :class:`~coredis.patterns.himport.HashImport`,
available via the :meth:`coredis.Redis.himport` and
:meth:`coredis.RedisCluster.himport` factory methods. It works as an async
context manager on both regular and cluster clients::

    async def example(client):
        async with client.himport("account", ["name", "email", "age"]) as himport:
            himport.add("user:1", ["alice", "alice@example.com", 30])
            himport.add("user:{a}", {"name": "carol", "email": "carol@example.com", "age": 28})
            himport.add("user:2", name="dave", email="dave@example.com", age=33)
        assert await client.hgetall("user:1") == {
            "name": "alice",
            "email": "alice@example.com",
            "age": "30",
        }

:meth:`~coredis.patterns.himport.HashImport.add` accepts values in several forms:
a list in field order, a mapping keyed by field name, or keyword arguments. Each
call replaces the entire hash at that key.

.. note:: Each write (:meth:`~coredis.patterns.himport.HashImport.flush` or
   context exit) borrows a connection, prepares the fieldset, sends the sets,
   and discards the fieldset. A cluster write does that once per primary that
   received a key. :meth:`~coredis.patterns.himport.HashImport.add` only queues.
   If your connection pool has a single slot, other commands on the same client
   wait until that write finishes.

Flush
^^^^^

By default, all queued hashes are written when the context exits. You can also
call :meth:`~coredis.patterns.himport.HashImport.flush` at any point to write
the current queue immediately, which is useful if you want to control exactly
when data lands in Redis rather than waiting until the end of the block::

    async def abort(client):
        try:
            async with client.himport("account", ["name"]) as himport:
                himport.add("user:1", ["alice"])
                await himport.flush()
                himport.add("user:2", ["bob"])
                raise RuntimeError("stop")
        except RuntimeError:
            pass
        assert await client.hget("user:1", "name") == "alice"
        assert await client.exists(["user:2"]) == 0

If the body raises an exception, only hashes written by an explicit
:meth:`~coredis.patterns.himport.HashImport.flush` call survive; any entries
queued after the last flush are discarded.
