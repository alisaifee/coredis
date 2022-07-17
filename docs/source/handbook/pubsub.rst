PubSub
------

coredis includes a :class:`~coredis.commands.PubSub` class
that subscribes to channels and listens for new messages.
Creating an instance can be done through the :meth:`coredis.Redis.pubsub` or :meth:`coredis.RedisCluster.pubsub` methods.

.. code-block:: python

    r = coredis.Redis(...)
    p = r.pubsub()

Once a :class:`~coredis.commands.PubSub` instance is created,
channels and patterns can be subscribed to.

.. code-block:: python

    await p.subscribe('my-first-channel', 'my-second-channel', ...)
    await p.psubscribe('my-*', ...)

The :class:`~coredis.commands.PubSub` instance is now subscribed to those channels/patterns. The
subscription confirmations can be seen by reading messages from the :class:`~coredis.commands.PubSub`
instance.

.. code-block:: python

    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-second-channel', 'data': 1L}
    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-first-channel', 'data': 2L}
    await p.get_message()
    # {'pattern': None, 'type': 'psubscribe', 'channel': 'my-*', 'data': 3L}

Every message read from a :class:`~coredis.commands.PubSub` instance
will be a typed dictionary defined as:

.. autoclass:: coredis.response.types.PubSubMessage
   :noindex:
   :no-inherited-members:
   :show-inheritance:


Let's send a message now.

.. code-block:: python

    # the publish method returns the number matching channel and pattern
    # subscriptions. 'my-first-channel' matches both the 'my-first-channel'
    # subscription and the 'my-*' pattern subscription, so this message will
    # be delivered to 2 channels/patterns
    await r.publish('my-first-channel', 'some data')
    # 2
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 'some data', 'pattern': None, 'type': 'message'}
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 'some data', 'pattern': 'my-*', 'type': 'pmessage'}

Unsubscribing works just like subscribing. If no arguments are passed to
[p]unsubscribe, all channels or patterns will be unsubscribed from.

.. code-block:: python

    await p.unsubscribe()
    await p.punsubscribe('my-*')
    await p.get_message()
    # {'channel': 'my-second-channel', 'data': 2L, 'pattern': None, 'type': 'unsubscribe'}
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 1L, 'pattern': None, 'type': 'unsubscribe'}
    await p.get_message()
    # {'channel': 'my-*', 'data': 0L, 'pattern': None, 'type': 'punsubscribe'}

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
    await p.subscribe(**{'my-channel': my_handler})
    # read the subscribe confirmation message
    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-channel', 'data': 1L}
    await r.publish('my-channel', 'awesome data')
    # 1

    # for the message handler to work, we need tell the instance to read data.
    # this can be done in several ways (read more below). we'll just use
    # the familiar get_message() function for now
    await message = p.get_message()
    # 'MY HANDLER:  awesome data'

    # note here that the my_handler callback printed the string above.
    # `message` is None because the message was handled by our handler.
    print(message)
    # None

If your application is not interested in the subscribe/unsubscribe confirmation messages,
you can ignore them by setting :paramref:`~coredis.Redis.pubsub.ignore_subscribe_messages`
to ``True``. This will cause all subscribe/unsubscribe messages to be read, but they won't
bubble up to your application.

.. code-block:: python

    p = r.pubsub(ignore_subscribe_messages=True)
    await p.subscribe('my-channel')
    await p.get_message()  # hides the subscribe message and returns None
    await r.publish('my-channel')
    # 1
    await p.get_message()
    # {'channel': 'my-channel', 'data': 'my data', 'pattern': None, 'type': 'message'}

There are two main strategies for reading messages.

The examples above have been using :meth:`~coredis.commands.PubSub.get_message`.
If there's data available to be read, the method will read it, format the message
and return it or pass it to a message handler. If there's no data to be read, it
will return ``None`` after the configured :paramref:`~coredis.commands.PubSub.get_message.timeout`

.. code-block:: python

    while True:
        message = await p.get_message()
        if message:
            # do something with the message
        await asyncio.sleep(0.001)  # be nice to the system :)

The second option runs an event loop in a separate thread.
:meth:`~coredis.commands.PubSub.run_in_thread` creates a new thread and uses
the event loop in the main thread. The thread instance of
:class:`~coredis.commands.pubsub.PubSubWorkerThread` is returned to the caller
of :meth:`~coredis.commands.PubSub.run_in_thread()`. The caller can use the
:meth:`~coredis.commands.pubsub.PubSubWorkerThread.stop` method on the thread
instance to shut down the event loop and thread. Behind the scenes, this is
simply a wrapper around :meth:`~coredis.commands.PubSub.get_message`
that runs in a separate thread, and use :func:`asyncio.run_coroutine_threadsafe`
to run coroutines.

Note: Since we're running in a separate thread, there's no way to handle
messages that aren't automatically handled with registered message handlers.
Therefore, coredis prevents you from calling :meth:`~coredis.commands.PubSub.run_in_thread`
if you're subscribed to patterns or channels that don't have message handlers attached.

.. code-block:: python

    await p.subscribe(**{'my-channel': my_handler})
    thread = p.run_in_thread(sleep_time=0.001)
    # the event loop is now running in the background processing messages
    # when it's time to shut it down...
    thread.stop()

PubSub instances remember what channels and patterns they are subscribed to. In
the event of a disconnection such as a network error or timeout, the
PubSub instance will re-subscribe to all prior channels and patterns when
reconnecting. Messages that were published while the client was disconnected
cannot be delivered. When you're finished with a PubSub object, call the
:meth:`~coredis.commands.PubSub.close` method to shutdown the connection.

.. code-block:: python

    p = r.pubsub()
    ...
    p.close()

The Pub/Sub support commands :command:`PUBSUB-CHANNELS`, :command:`PUBSUB-NUMSUB` and :command:`PUBSUB-NUMPAT` are also
supported:

.. code-block:: python

    await r.pubsub_channels()
    # ['foo', 'bar']
    await r.pubsub_numsub('foo', 'bar')
    # [('foo', 9001), ('bar', 42)]
    await r.pubsub_numsub('baz')
    # [('baz', 0)]
    await r.pubsub_numpat()
    # 1204

Cluster Pub/Sub
^^^^^^^^^^^^^^^

The :class:`coredis.RedisCluster` client exposes two ways of building a :term:`Pub/Sub`
application.

:meth:`~coredis.RedisCluster.pubsub` returns an instance of :class:`coredis.commands.ClusterPubSub`
which exposes identical functionality to the non clustered client. This is possible
without worrying about sharding as the :command:``PUBLISH`` command in clustered redis results
in messages being broadcasted to every node in the cluster. On the consumer side of the equation
**coredis** simply picks a node by hashing the first subscribed channel using the same algorithm
used to find slots for keys and consumes the messages from the node the channel hashes to.

This approach, though functional does pose limited opportunity for horizontal scaling as all the nodes
in the cluster will have to process the published messages for all channels.

===============
Sharded Pub/Sub
===============

As of :redis-version:`7.0.0` support for :term:`Sharded Pub/Sub` has been added
through the :command:`SSUBSCRIBE`, :command:`SUNSUBSCRIBE` and :command:`SPUBLISH` commands
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