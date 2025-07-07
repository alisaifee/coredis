Pipelines
---------

Pipelines expose an identical API to :class:`~coredis.Redis`, however
the awaitable returned by calling a pipeline method can only be awaited
after the entire pipeline has successfully executed by calling
:meth:`~coredis.pipeline.Pipeline.execute`

For example:

.. code-block:: python

    async def example(client):
        async with await client.pipeline(transaction=True) as pipe:
            # commands is a tuple of awaitables
            commands = (
                pipe.flushdb(),
                pipe.set("foo", "bar"),
                pipe.set("bar", "foo"),
                pipe.keys("*"),
            )
            results = await pipe.execute()
            # results are in order corresponding to your command
            assert results == (True, True, True, set([b"bar", b"foo"]))
            # results can also be retrieved from the returns of each command
            assert await asyncio.gather(*commands) == (True, True, True, set[b"bar", b"foo"])


Atomicity & Transactions
^^^^^^^^^^^^^^^^^^^^^^^^
In addition, pipelines can also ensure the buffered commands are executed
atomically as a group by using the :paramref:`~coredis.Redis.pipeline.transaction` argument.

.. code-block:: python

    pipe = r.pipeline(transaction=True)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For instance,
let"s assume that the :rediscommand:`INCR` command didn"t exist and we need to build an atomic
version of :rediscommand:`INCR` in Python.

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

    async def example():
        async with await r.pipeline() as pipe:
            while True:
                try:
                    # put a WATCH on the key that holds our sequence value
                    await pipe.watch("OUR-SEQUENCE-KEY")
                    # after WATCHing, the pipeline is put into immediate execution
                    # mode until we tell it to start buffering commands again.
                    # this allows us to get the current value of our sequence
                    current_value = await pipe.get("OUR-SEQUENCE-KEY")
                    next_value = int(current_value) + 1
                    # now we can put the pipeline back into buffered mode with MULTI
                    pipe.multi()
                    # This call doesn't need to be awaited as it is part of the pipeline
                    pipe.set("OUR-SEQUENCE-KEY", next_value)
                    # and finally, execute the pipeline (the set command)
                    await pipe.execute()
                    # if a WatchError wasn"t raised during execution, everything
                    # we just did happened atomically.
                    break
                except WatchError:
                    # another client must have changed "OUR-SEQUENCE-KEY" between
                    # the time we started WATCHing it and the pipeline"s execution.
                    # our best bet is to just retry.
                    continue

Note that, because the Pipeline must bind to a single connection for the
duration of a :rediscommand:`WATCH`, care must be taken to ensure that the connection is
returned to the connection pool by calling the :meth:`~coredis.pipeline.Pipeline.clear` method. If the
:class:`~coredis.pipeline.Pipeline` is used as a context manager (as in the example above) :meth:`~coredis.pipeline.Pipeline.clear`
will be called automatically. Of course you can do this the manual way by
explicitly calling :meth:`~coredis.pipeline.Pipeline.clear`:

.. code-block:: python

    async def example():
        async with await r.pipeline() as pipe:
            while 1:
                try:
                    await pipe.watch("OUR-SEQUENCE-KEY")
                    ...
                    await pipe.execute()
                    break
                except WatchError:
                    continue
                finally:
                    await pipe.clear()

A convenience method :meth:`~coredis.Redis.transaction` exists for handling all the
boilerplate of handling and retrying watch errors. It takes a callable that
should expect a single parameter, a pipeline object, and any number of keys to
be watched. Our client-side :rediscommand:`INCR` command above can be written like this,
which is much easier to read:

.. code-block:: python

    async def client_side_incr(pipe) -> int:
        current_value = await pipe.get("OUR-SEQUENCE-KEY") or 0
        next_value = int(current_value) + 1
        pipe.multi()
        await pipe.set("OUR-SEQUENCE-KEY", next_value)
        return next_value

    await r.transaction(client_side_incr, "OUR-SEQUENCE-KEY")
    # (True,)
    await r.transaction(client_side_incr, "OUR-SEQUENCE-KEY", value_from_callable=True)
    # 2


