Streams
-------

Redis :term:`Streams` are a flexible datastructure that can be applied to various
use cases including but not limited to timeseries and :term:`Pub/Sub` for event driven architectures.
The **coredis** clients support all stream related commands and provides two high level
abstractions for building stream consumers.

Simple Consumer
^^^^^^^^^^^^^^^

The :class:`~coredis.stream.Consumer` returned by :meth:`coredis.Redis.xconsumer` can be used as an
independent consumer that can read from one or many streams. The consumer has limited scope
and can be configured with a collection of :paramref:`~coredis.Redis.xconsumer.streams`
to read from which by default starts from the latest entry observed upon initialization.

Given either a regular client or a cluster client::

    import coredis
    client = coredis.Redis()
    # or cluster
    # client = coredis.RedisCluster("localhost", 7000)

#. Create the consumer::


     consumer = client.xconsumer(streams=["one", "two", "three"])
     # or directly
     # import coredis.stream
     # consumer = coredis.stream.Consumer(client, streams=["one", "two", "three"])

#. Entries can be fetched explicitly by calling :meth:`~coredis.stream.Consumer.get_entry()`::

    async with client:
        async with consumer:
            stream, entry = await consumer.get_entry()

#. or, by using the consumer as an asynchronous iterator::

    async with client:
        async for stream, entry in consumer:
            # do something with the entry


   .. note:: The iterator will end the moment there are no entries
      returned or pending in the buffer.

   The above example can be converted into an infinite iterator over the configured streams::

      async with client:
        while True:
            async for stream, entry in consumer:
                # do something with the entry

=============
Configuration
=============

The consumer could have been configured with a few parameters to align better with
the performance characteristics of your application such as:

#. With a blocking timeout::

    consumer = client.xconsumer(
        client,
        streams=["one", "two", "three"],
        timeout=30*1000  # 30 seconds
    )


#. With an internal buffer::

    consumer = client.xconsumer(
        client,
        streams=["one", "two", "three"],
        # Will fetch upto 10 extra entries per stream
        # on every request to redis
        buffer_size=10,
        timeout=30*1000  # 30 seconds
    )


Group Consumer
^^^^^^^^^^^^^^

:class:`~coredis.stream.GroupConsumer` returned by :meth:`coredis.Redis.xconsumer` when
:paramref:`~coredis.Redis.xconsumer.group` and :paramref:`~coredis.Redis.xconsumer.consumer`
are both provided, has an identical interface as that provided by the standalone consumer.
It differs significantly however, in the use cases for which it is applicable.
Group consumers work cooperatively by only fetching new entries that have not been seen by other
consumers within the same group. As a consequence they have a concept of checkpointing by acknowledging
received stream entries and revisiting backlogs of entries that have not been acknowledged
either by themselves, or by other consumers in the same group.


The following two consumers will cooperatively consume from streams ``{one, two, three}``
without ever seeing an entry that the other has fetched::


    consumer1 = client.xconsumer(
        streams = ["one", "two", "three"],
        group = "group-a",
        consumer = "consumer-1",
        auto_acknowledge = True,
    )
    consumer2 = client.xconsumer(
        streams = ["one", "two", "three"],
        group = "group-a",
        consumer = "consumer-2",
        auto_acknowledge = True,
    )

    # Or directly
    # import coredis.stream
    # consumer1 = coredis.stream.GroupConsumer(
    #   client,
    #   streams= ["one", "two", "three"],
    #   group = "group-a",
    #   consumer = "consumer-1",
    #   auto_acknowledge = True,
    # )
.. note:: Setting the :paramref:`~coredis.Redis.xconsumer.auto_acknowledge`
   parameter ensures that the consumers don't need to explicitly acknowledge
   the entries that they fetch thus resulting in the received entries not
   populating the :term:`PEL`.

#. Add some entries to the three streams::

    async with client:
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
that is a member of a consumer group. The group consumer
respects the configuration parameters expected by the standalone consumer (:class:`~coredis.stream.Consumer`)
for example with respect to blocking and buffer sizes. It has a few additional
optional parameters that can be used modify the behavior with
respect to backlogs and checkpointing.

Setting the :paramref:`~coredis.stream.xconsumer.start_from_backlog`
parameter to ``True`` creates a consumer that considers any old entries that were not acknowledged
before picking up any new entries from the stream::

    import random

    async with client:
        async with client.xconsumer(
            streams=["one"],
            group = "group-a",
            consumer = "consumer-1",
            start_from_backlog = True
        ) as consumer:
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

        print("round two")
        # Let's pretend the consumer crashed and started again
        # and now doesn't have a bug that fails 50% of the time
        async with client.xconsumer(
            streams=["one"],
            group = "group-a",
            consumer = "consumer-1",
            start_from_backlog = True
        ) as consumer:
            async for stream, entry in consumer:
                 await client.xack(stream, consumer.group, [entry.identifier])

            pending = await client.xpending("one", "group-a")
            assert pending.consumers.get(b"consumer-1") is None

If no checkpointing is desired the group consumer can be initialized with the
:paramref:`~coredis.Redis.xconsumer.auto_acknowledge` parameter set to
``True`` which effectively results in redis not maintaining a :term:`PEL`
for the entries received by the consumers in this group.


