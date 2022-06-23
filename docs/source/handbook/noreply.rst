No Reply Mode
-------------

For scenarios where the response from the redis server is not relevant
to the client the :paramref:`~coredis.Redis.noreply` parameter can be passed to
either the :class:`~coredis.Redis` or :class:`~coredis.RedisCluster` constructors
which ensures the client does not wait for responses from the server and additionally
sets ``CLIENT REPLY OFF`` for any connection used by the client. This can provide some
performance benefits.

For example::

    import coredis

    client = coredis.Redis(noreply=True)
    assert await client.set("fubar", 1) is None
    assert await client.hset("hash_fubar", {"a": 1, "b": 2}) is None

    other_client = coredis.Redis()
    assert await other_client.get("fubar") == b"1"
    assert await other_client.hgetall("hash_fubar") == {b"a": b"1", b"b": b"2"}


The ``noreply`` flag can also be set temporarily::

    import coredis

    client = coredis.Redis()
    client.noreply = True
    assert await client.set("fubar", 1) is None
    assert await client.hset("hash_fubar", {"a": 1, "b": 2}) is None
    assert await client.get("fubar") is None
    client.noreply = False
    assert await client.get("fubar") == b"1"
    assert await client.hgetall("hash_fubar") == {b"a": b"1", b"b": b"2"}


.. danger:: When the client is used with the the ``noreply`` option there are no guarantees
   if the command was successfully performed by the redis server. The only validation performed
   by **coredis** is on the parameters that are passed in and that the command was written
   to the socket.

.. warning:: Using the ``noreply`` option effectively ignores return annotations
   and will therefore fail any type checkers (static or runtime).