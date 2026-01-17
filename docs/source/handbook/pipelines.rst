Pipelines
---------

Pipelines expose an identical API to :class:`~coredis.Redis`, however
the awaitable returned by calling a pipeline method can only be awaited
after the entire pipeline has successfully executed, that is, after
exiting the pipeline's async context manager:

For example:

.. code-block:: python

    async def example(client):
        async with client.pipeline(transaction=True) as pipe:
            # commands is a tuple of awaitables
            commands = (
                pipe.flushdb(),
                pipe.set("foo", "bar"),
                pipe.set("bar", "foo"),
                pipe.keys("*"),
            )
        # results can be retrieved from the returns of each command
        # notice this is OUTSIDE of the pipeline block
        assert await asyncio.gather(*commands) == (True, True, True, {b"bar", b"foo"})


Atomicity & Transactions
^^^^^^^^^^^^^^^^^^^^^^^^
In addition, pipelines can also ensure the buffered commands are executed
atomically as a group by using the :paramref:`~coredis.Redis.pipeline.transaction` argument.

.. code-block:: python

    pipe = r.pipeline(transaction=True)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For instance,
let"s assume that the :rediscommand:`INCR` command didn't exist and we need to
build an atomic version of :rediscommand:`INCR` in Python.

The completely naive implementation could :rediscommand:`GET` the value, increment it in
Python, and :rediscommand:`SET` the new value back. However, this is not atomic because
multiple clients could be doing this at the same time, each getting the same
value from :rediscommand:`GET`.

Enter the :rediscommand:`WATCH` command. :rediscommand:`WATCH` provides the ability to monitor one or more keys
prior to starting a transaction. If any of those keys change prior the
execution of that transaction, the entire transaction will be canceled and a
:exc:`~coredis.exceptions.WatchError` will be raised. To implement our own client-side :rediscommand:`INCR` command, we
could do something like this:

.. code-block:: python

    async def incr(client: coredis.Redis, key: str) -> int:
        while True:
            try:
                async with client.pipeline(transaction=False) as pipe:
                    # put a WATCH on the key that holds our sequence value
                    async with pipe.watch(key):
                        current_value = await client.get(key)
                        next_value = int(current_value) + 1
                        pipe.set(key, next_value)
            except WatchError:
                # another client must have changed the value between
                # the time we started watching it and the pipeline"s execution.
                # our best bet is to just retry.
                continue
            else:
                # if a WatchError wasn"t raised during execution, everything
                # we just did happened atomically.
                break


