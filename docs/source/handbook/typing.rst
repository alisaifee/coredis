Typing
------

Type Annotations
^^^^^^^^^^^^^^^^
**coredis** provides type annotations for the public API. These are tested using
:pypi:`mypy`.

The :class:`~coredis.Redis` and :class:`~coredis.RedisCluster` clients are Generic types constrained
by :class:`AnyStr`. The constructors and :meth:`~coredis.Redis.from_url` factory methods infer
the appropriate specialization automatically.

Without decoding:

.. code-block::

    async with coredis.Redis(
        "localhost", 6379, db=0, decode_responses=False, encoding="utf-8"
    ) as client:
        await client.set("string", 1)
        await client.lpush("list", [1])
        await client.hset("hash", {"a": 1})
        await client.sadd("set", ["a"])
        await client.zadd("sset", {"a": 1.0, "b": 2.0})

        str_response = await client.get("string")
        list_response_ = await client.lrange("list", 0, 1)
        hash_response = await client.hgetall("hash")
        set_response = await client.smembers("set")
        sorted_set_members_only_response = await client.zrange("sset", -1, 1)

        reveal_locals()
        # note: Revealed local types are:
        # note:     client: coredis.client.Redis[builtins.bytes]
        # note:     hash_response: builtins.dict*[builtins.bytes*, builtins.bytes*]
        # note:     list_response_: builtins.list*[builtins.bytes*]
        # note:     set_response: builtins.set*[builtins.bytes*]
        # note:     sorted_set_members_only_response: builtins.tuple*[builtins.bytes*, ...]
        # note:     str_response: builtins.bytes*

With decoding:

.. code-block::

    async with coredis.Redis(
        "localhost", 6379, db=0, decode_responses=True, encoding="utf-8"
    ) as client:
        await client.set("string", 1)
        await client.lpush("list", [1])
        await client.hset("hash", {"a": 1})
        await client.sadd("set", ["a"])
        await client.zadd("sset", {"a": 1.0, "b": 2.0})

        str_response = await client.get("string")
        list_response_ = await client.lrange("list", 0, 1)
        hash_response = await client.hgetall("hash")
        set_response = await client.smembers("set")
        sorted_set_members_only_response = await client.zrange("sset", -1, 1)

        reveal_locals()
        # note: Revealed local types are:
        # note:     client: coredis.client.Redis[builtins.str]
        # note:     hash_response: builtins.dict*[builtins.str*, builtins.str*]
        # note:     list_response_: builtins.list*[builtins.str*]
        # note:     set_response: builtins.set*[builtins.str*]
        # note:     sorted_set_members_only_response: builtins.tuple*[builtins.str*, ...]
        # note:     str_response: builtins.str*


Runtime Type checking
^^^^^^^^^^^^^^^^^^^^^

**coredis** optionally wraps all command methods with :pypi:`beartype` decorators to help
detect errors during testing (or if you are b(ea)rave enough, always).

This can be enabled by installing :pypi:`beartype` and setting the :data:`COREDIS_RUNTIME_CHECKS`
environment variable.

As an example:

.. code-block:: bash

    $ COREDIS_RUNTIME_CHECKS=1 python -c "
    import coredis
    import asyncio
    async def test():
        async with coredis.Redis() as client:
            await client.set(1,1)
    asyncio.run(test())
    """
    Traceback (most recent call last):
      File "<@beartype(coredis.commands.core.CoreCommands.set) at 0x10c403130>", line 33, in set
    beartype.roar.BeartypeCallHintParamViolation: @beartyped coroutine CoreCommands.set() parameter key=1 violates type hint typing.Union[str, bytes], as 1 not str or bytes.
