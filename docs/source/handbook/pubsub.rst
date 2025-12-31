PubSub
------
PubSub is implemented through the following classes:

- Single Redis Server: :class:`coredis.commands.PubSub`
- Redis Cluster: :class:`coredis.commands.ClusterPubSub`
- Redis Cluster (with sharded pubsub): :class:`coredis.commands.ShardedPubSub`

Creating an instance can be done through the :meth:`coredis.Redis.pubsub`,
:meth:`coredis.RedisCluster.pubsub`, :meth:`coredis.RedisCluster.sharded_pubsub` methods
exposed by the client or directly via the specific constructors.

Subscription management
^^^^^^^^^^^^^^^^^^^^^^^

Channels or patterns can either be subscribed to on instantiation through
constructor parameters or explicitly through the :meth:`~coredis.commands.PubSub.subscribe`
or :meth:`~coredis.commands.PubSub.psubscribe` methods.

Upon instantiation::

    async with client.pubsub(
        channels=["my-first-channel", "my-second-channel"], patterns=["my-*"]
    ) as consumer:
        ...

or explicitly::

    async with client.pubsub() as consumer:
        await consumer.subscribe("my-first-channel", "my-second-channel", ...)
        await consumer.psubscribe("my-*")

The async context manager automatically manages unsubscribing and cleanup on exit::

    async with client.pubsub(
        channels=["my-first-channel", "my-second-channel"], patterns=["my-*"]
    ) as consumer:
        async for message in consumer:
            print(message)
    # remaining subscriptions are unsubscribed and connection is released
    # back to the connection pool when the context manager exits.

If desired unsubscription can also be done explicitly by calling
:meth:`~coredis.commands.PubSub.unsubscribe` for channels
and :meth:`~coredis.commands.PubSub.punsubscribe` for patterns.

In the following example if any of the channels that the client was initially subscribed
to contain a ``STOP`` message the consumer will unsubscribe from the channel and continue
processing until there are no more subscriptions left (the async iterator will automatically
exit when the consumer has no subscriptions)::

    async with client.pubsub(
        channels=[f"channel-{i}" for i in range(10)]
    ) as consumer:
        async for message in consumer:
            if message["data"] == "STOP":
                await consumer.unsubscribe(message["channel"])
            else:
                print(message["data"])

Consuming Messages
^^^^^^^^^^^^^^^^^^

Messages received on the subscribed topics or patterns can be read either by
using the pubsub instance itself as an async iterator or explicitly by calling
the :meth:`~coredis.commands.PubSub.get_message` method.

Every message read from a :class:`~coredis.commands.PubSub` instance
will be a typed dictionary defined as:

.. autoclass:: coredis.response.types.PubSubMessage
   :noindex:
   :no-inherited-members:
   :show-inheritance:

With the iterator::

    await consumer.subscribe("my-channel")
    async for message in consumer:
        # do something with the message

.. note:: Unsubscribing from all subscribed channels will result in the iterator
   ending (i.e. raising :exc:`StopAsyncIteration`)

Explicitly with :meth:`~coredis.commands.PubSub.get_message`::

    while True:
        message = await consumer.get_message()
        if message:
            # do something with the message


.. note:: When using :meth:`~coredis.commands.PubSub.get_message` the return
   could be ``None`` either if :paramref:`~coredis.commands.PubSub.get_message.timeout` is
   exceeded without receiving a message or:

   - **if** the message was a subscription / unsubscription response and the instance was created
     with :paramref:`coredis.commands.PubSub.ignore_subscribe_messages` set to ``True``
   - **if** the message was received on a channel or pattern that has a handler registered (See :ref:`handbook/pubsub:callbacks` below)

Callbacks
^^^^^^^^^
coredis also allows you to register callback functions to handle published
messages. Message handlers take a single argument, the message, which is a
dictionary just like the examples above. To subscribe to a channel or pattern
with a message handler, pass the channel or pattern name as a keyword argument
with its value being the callback function.

When a message is read on a channel or pattern with a message handler, the
message dictionary is created and passed to the message handler. In this case,
a ``None`` value is returned from :meth:`~coredis.commands.PubSub.get_message`
since the message was already handled.

.. code-block:: python

    def my_handler(message):
        print('MY HANDLER: ', message['data'])
    await consumer.subscribe(**{'my-channel': my_handler})
    # read the subscribe confirmation message
    await consumer.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-channel', 'data': 1L}
    await client.publish('my-channel', 'awesome data')
    # 1

    # for the message handler to work, we need tell the instance to read data.
    # this can be done in several ways (read more below). we'll just use
    # the familiar get_message() function for now
    await message = consumer.get_message()
    # 'MY HANDLER:  awesome data'

    # note here that the my_handler callback printed the string above.
    # `message` is None because the message was handled by our handler.
    print(message)
    # None

PubSub instances remember what channels and patterns they are subscribed to. In
the event of a disconnection such as a network error or timeout, the
PubSub instance will re-subscribe to all prior channels and patterns when
reconnecting. Messages that were published while the client was disconnected
cannot be delivered.

The Pub/Sub support commands :rediscommand:`PUBSUB-CHANNELS`, :rediscommand:`PUBSUB-NUMSUB` and :rediscommand:`PUBSUB-NUMPAT` are also
supported:

.. code-block:: python

    await client.pubsub_channels()
    # ['foo', 'bar']
    await client.pubsub_numsub('foo', 'bar')
    # [('foo', 9001), ('bar', 42)]
    await client.pubsub_numsub('baz')
    # [('baz', 0)]
    await client.pubsub_numpat()
    # 1204

Cluster Pub/Sub
^^^^^^^^^^^^^^^

The :class:`coredis.RedisCluster` client exposes two ways of building a :term:`Pub/Sub`
application.

:meth:`coredis.RedisCluster.pubsub` returns an instance of :class:`coredis.commands.ClusterPubSub`
which exposes identical functionality to the non clustered client. This is possible
without worrying about sharding as the :rediscommand:`PUBLISH` command in clustered redis results
in messages being broadcasted to every node in the cluster.

On the consumer side of the equation **coredis** simply picks a random node and consumes the messages from all
subscribed topics.

This approach, though functional does pose limited opportunity for horizontal scaling as all the nodes
in the cluster will have to process the published messages for all channels.

Sharded Pub/Sub
^^^^^^^^^^^^^^^

As of :redis-version:`7.0.0` support for :term:`Sharded Pub/Sub` has been added
through the :rediscommand:`SSUBSCRIBE`, :rediscommand:`SUNSUBSCRIBE` and :rediscommand:`SPUBLISH` commands
which restricts publishing of messages to individual shards based on the same algorithm used
to route keys to shards.

.. note:: There is no corresponding support for pattern based subscriptions
   (as you might have guessed, it wouldn't be possible to shard those).

Access to Sharded Pub/Sub is available through the :meth:`coredis.RedisCluster.sharded_pubsub`
method which exposes the same api and functionality (except for pattern support) as the other previously mentioned `PubSub` classes.

To publish a messages that is meant to be consumed by a Sharded Pub/Sub consumer use
:meth:`coredis.Redis.spublish` instead of :meth:`coredis.Redis.publish`

:term:`Sharded Pub/Sub` can provide much better performance as each node in the cluster
only routes messages for channels that reside on the node (which in turn means that **coredis**
can use a dedicated connection per node to drain messages).

Additionally, the :paramref:`~coredis.RedisCluster.sharded_pubsub.read_from_replicas`
parameter can be set to ``True`` when constructing a :class:`~coredis.commands.pubsub.ShardedPubSub` instance
to further increase throughput by letting the consumer use read replicas.
