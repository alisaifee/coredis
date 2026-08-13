Hash import
-----------
:mod:`coredis.patterns.himport`

Hash import writes many hashes that share one ordered field list. Redis 8.10
keeps that list as a **fieldset on one connection**: ``HIMPORT PREPARE`` names
the fields, ``HIMPORT SET`` writes a hash by positional values, and
``HIMPORT DISCARD`` / ``HIMPORT DISCARDALL`` drop the fieldset. The fieldset is
not a key in the database.

:meth:`coredis.Redis.himport` and :meth:`coredis.RedisCluster.himport` return
an async context manager that owns that session: queue rows in memory with
:meth:`~coredis.patterns.himport.HashImport.add`, lease connection(s) only when
writing, prepare once per held connection, write on
:meth:`~coredis.patterns.himport.HashImport.flush` or when the context exits,
then discard and release. Pure queueing does not hold a pool connection.

Standalone and cluster
^^^^^^^^^^^^^^^^^^^^^^

The same call works on a single Redis instance and on Redis Cluster. On a
cluster, rows are grouped by the primary that owns each key; each node
connection is prepared once. Multi-node flushes run concurrently under structured
concurrency. Slot redirects (``MOVED`` / ``ASK``) update the layout, prepare on
the destination connection, and retry the affected sets.

::

    async def example(client):
        async with client.himport("account", ["name", "email", "age"]) as himport:
            himport.add("user:1", ["alice", "alice@example.com", 30])
            himport.add("user:{a}", ["carol", "carol@example.com", 28])
            himport.add("user:{b}", ["dave", "dave@example.com", 33])
        assert await client.hgetall("user:1") == {
            "name": "alice",
            "email": "alice@example.com",
            "age": "30",
        }

Use :class:`~coredis.patterns.himport.HashImport` from a :class:`~coredis.Redis`
client and :class:`~coredis.patterns.himport.ClusterHashImport` from a
:class:`~coredis.RedisCluster` client. Both expose :meth:`add` and :meth:`flush`.

Flush and abort
^^^^^^^^^^^^^^^

:meth:`~coredis.patterns.himport.HashImport.flush` writes whatever is queued
and can be called more than once. Anything still queued is written when the
context exits. An empty session is a no-op (no connection leased).

If the body raises, queued rows that were not flushed are not written, and any
fieldset that was prepared is still discarded on the way out::

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

Raw subcommands
^^^^^^^^^^^^^^^

Prefer the session. The four raw subcommands remain on the command mixin for
**standalone** connection-affine use only: a
:class:`~coredis.patterns.pipeline.Pipeline` with ``transaction=False`` (one
socket for prepare, set, and discard).

A pooled :class:`~coredis.Redis` or :class:`~coredis.RedisCluster` client cannot
send them correctly and raises :exc:`NotImplementedError`. A
:class:`~coredis.patterns.pipeline.ClusterPipeline` also raises
:exc:`NotImplementedError`: prepare has no key and cannot be routed with set
across slots.

Do not wrap prepare or discard in ``MULTI``. The server rejects them inside a
transaction.

Standalone raw example::

    async def raw(redis: coredis.Redis):
        async with redis.pipeline(transaction=False) as pipe:
            prepared = pipe.himport_prepare("account", ["name", "email"])
            written = pipe.himport_set("user:1", "account", ["alice", "a@example.com"])
            discarded = pipe.himport_discard("account")
        assert await prepared
        assert await written
        assert await discarded
