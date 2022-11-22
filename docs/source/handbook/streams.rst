Streams
-------

Redis :term:`Streams` are a flexible datastructure that can be applied to various
use cases including but not limited to timeseries and :term:`Pub/Sub` for event driven architectures.
The **coredis** clients support all stream related commands and provides two high level
abstractions for building stream consumers.

Simple Consumer
^^^^^^^^^^^^^^^

:class:`~coredis.stream.Consumer` can be used as an independent consumer
that can read from one or many streams. The consumer has limited scope
and can be configured with a collection of :paramref:`~coredis.stream.Consumer.streams`
to read from which by default starts from the latest entry observed upon initialization.

Given either a regular client or a cluster client::

    import coredis
    client = coredis.Redis()
    # or cluster
    # client = coredis.RedisCluster("localhost", 7000)

#. Create the consumer

   * Create the consumer in a synchronous context::

      from coredis.stream import Consumer
      consumer = Consumer(client, streams=["one", "two", "three"])

   * Or, create and initialize the consumer in an async context::

      from coredis.stream import Consumer
      consumer = await Consumer(client, streams=["one", "two", "three"]

#. Entries can be fetched explicitly by calling :meth:`~coredis.stream.Consumer.get_entry()`::

    stream, entry = await consumer.get_entry()

#. or, by using the consumer as an asynchronous iterator::

    async for stream, entry in consumer:
        # do something with the entry


   .. note:: The iterator will end the moment there are no entries
      returned or pending in the buffer.

   The above example can be converted into an infinite iterator over the configured streams::

      while True:
        async for stream, entry in consumer:
            # do something with the entry

=============
Configuration
=============

The consumer could have been configured with a few parameters to align better with
the performance characteristics of your application such as:

#. With a blocking timeout::

    consumer = Consumer(
        client,
        streams=["one", "two", "three"],
        timeout=30*1000  # 30 seconds
    )


#. With an internal buffer::

    consumer = Consumer(
        client,
        streams=["one", "two", "three"],
        # Will fetch upto 10 extra entries per stream
        # on every request to redis
        buffer_size=10,
        timeout=30*1000  # 30 seconds
    )


Group Consumer
^^^^^^^^^^^^^^

:class:`~coredis.stream.GroupConsumer` has an identical interface as that provided
by the standalone consumer. It differs significantly however, in the use cases for
which it is applicable. Group consumers work cooperatively by only fetching
new entries that have not been seen by other consumers within the same group.
As a consequence they have a concept of checkpointing by acknowledging
received stream entries and revisiting backlogs of entries that have not been acknowledged
either by themselves, or by other consumers in the same group.


The consumer is initialized in a similar manner as the standalone consumer, however
it requires at minimum the :paramref:`~coredis.stream.GroupConsumer.group` and
:paramref:`~coredis.stream.GroupConsumer.consumer` attributes.

The following two consumers will cooperatively consume from streams ``{one, two, three}``
without ever seeing an entry that the other has fetched::

    from coredis.stream import GroupConsumer

    consumer1 = GroupConsumer(
        client,
        streams = ["one", "two", "three"],
        group = "group-a",
        consumer = "consumer-1",
        auto_acknowledge = True,
    )
    consumer2 = GroupConsumer(
        client,
        streams = ["one", "two", "three"],
        group = "group-a",
        consumer = "consumer-2",
        auto_acknowledge = True,
    )

.. note:: Setting the :paramref:`~coredis.stream.GroupConsumer.auto_acknowledge`
   parameter ensures that the consumers don't need to explicitly acknowledge
   the entries that they fetch thus resulting in the received entries not
   populating the :term:`PEL`.

#. Add some entries to the three streams::

    [await client.xadd("one", {"id": i}) for i in range(10)]
    [await client.xadd("two", {"id": i}) for i in range(10)]
    [await client.xadd("three", {"id": i}) for i in range(10)]

#. Concurrently initiate a full drain with both consumers::

    async def processor(consumer):
        return [(stream, entry) async for (stream, entry) in consumer]

    consumer1_results, consumer2_results = await asyncio.gather(
        processor(consumer1),
        processor(consumer2)
    )

    assert len(consumer1_results) + len(consumer2_results) == 30

==================
Backlog management
==================

The above examples use the most common configuration for a consumer
that is a member of a consumer group. :class:`~coredis.stream.GroupConsumer`
respects the configuration parameters expected by :class:`~coredis.stream.Consumer`
for example with respect to blocking and buffer sizes. It has a few additional
optional constructor parameters that can be used modify the behavior with
respect to backlogs and checkpointing.

Setting the :paramref:`~coredis.stream.GroupConsumer.start_from_backlog`
parameter to ``True`` creates a consumer that considers any old entries that were not acknowledged
before picking up any new entries from the stream::

    from coredis.stream import GroupConsumer
    import random

    consumer = await GroupConsumer(
        client,
        streams=["one"],
        group = "group-a",
        consumer = "consumer-1",
        start_from_backlog = True
    )

    [await client.xadd("one", {"id": i}) for i in range(10)]


    # fetch all ten entries and simulate a bug occurring 50% of the time
    # when processing the entry
    async for stream, entry in consumer:
        if random.random() > 0.5:
            print("success", await client.xack(stream, consumer.group, [entry.identifier]))
        else:
            print("oh nos!")

    pending = await client.xpending("one", "group-a")
    assert pending.consumers[b"consumer-1"] > 0

    # Let's pretend the consumer crashed and started again
    # and now doesn't have a bug that fails 50% of the time
    consumer = GroupConsumer(
        client,
        streams=["one"],
        group = "group-a",
        consumer = "consumer-1",
        start_from_backlog = True
    )

    async for stream, entry in consumer:
         await client.xack(stream, consumer.group, [entry.identifier])

    pending = await client.xpending("one", "group-a")
    assert pending.consumers.get(b"consumer-1") is None

If no checkpointing is desired the group consumer can be initialized with the
:paramref:`~coredis.stream.GroupConsumer.auto_acknowledge` parameter set to
``True`` which effectively results in redis not maintaining a :term:`PEL`
for the entries received by the consumers in this group.


