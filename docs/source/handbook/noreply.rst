No Reply Mode
-------------

For scenarios where the response from the redis server is not relevant
to the client the :paramref:`~coredis.Redis.noreply` parameter can be passed to
either the :class:`~coredis.Redis` or :class:`~coredis.RedisCluster` constructors
which ensures the client does not wait for responses from the server and additionally
sets ``CLIENT REPLY OFF`` (using the :rediscommand:`CLIENT-REPLY` command) for any connection used by the client. This can provide some
performance benefits.

For example::

    import coredis

    client = coredis.Redis(noreply=True)
    async with client:
        assert await client.set("fubar", 1) is None
        assert await client.hset("hash_fubar", {"a": 1, "b": 2}) is None

    other_client = coredis.Redis()
    async with other_client:
        assert await other_client.get("fubar") == b"1"
        assert await other_client.hgetall("hash_fubar") == {b"a": b"1", b"b": b"2"}


The mode can also be enabled temporarily through the :meth:`~coredis.Redis.ignore_replies` context manager::

    import coredis

    client = coredis.Redis()
    async with client:
        with client.ignore_replies():
            assert await client.set("fubar", 1) is None
        assert await client.get("fubar") == b"1"


.. danger:: When the client is used with the the ``noreply`` option there are no guarantees
   if the command was successfully performed by the redis server. The only validation performed
   by **coredis** is on the parameters that are passed in and that the command was written
   to the socket.

.. warning:: Using the ``noreply`` option effectively ignores return annotations
   and will (**probably**) therefore fail any type checkers (static or runtime).
