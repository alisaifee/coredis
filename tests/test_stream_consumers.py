from __future__ import annotations

import asyncio
import threading
from collections import OrderedDict

import pytest

from coredis.exceptions import StreamConsumerInitializationError
from coredis.stream import Consumer, GroupConsumer
from tests.conftest import targets


async def consume_entries(consumer, count, consumed=None):
    consumed = OrderedDict() if consumed is None else consumed
    for i in range(count):
        entry = await consumer.get_entry()
        if entry:
            consumed.setdefault(entry[0], []).append(entry[1])
    return consumed


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
)
class TestStreamConsumers:
    async def test_single_consumer(self, client, _s):
        consumer = await Consumer(client, ["a", "b"])
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10, 20)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]
        assert list(range(10, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_add_stream_to_single_consumer(self, client, _s):
        consumer = await Consumer(client, ["a", "b"])
        [await client.xadd("a", {"id": i}) for i in range(10)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]

        assert await consumer.add_stream("c")
        await client.xadd("c", {"id": 21})
        consumed = await consume_entries(consumer, 1)
        assert int(consumed[_s("c")][0].field_values[_s("id")]) == 21

        await client.xadd("a", {"id": 22})
        consumed = await consume_entries(consumer, 1)
        assert int(consumed[_s("a")][0].field_values[_s("id")]) == 22

        d_last_identifier = await client.xadd("d", {"id": 23})
        assert await consumer.add_stream("d", d_last_identifier)

        await client.xadd("d", {"id": 24})
        consumed = await consume_entries(consumer, 1)
        assert int(consumed[_s("d")][0].field_values[_s("id")]) == 24

    async def test_single_consumer_start_from_latest(self, client, _s):
        [await client.xadd("a", {"id": i}) for i in range(5)]
        [await client.xadd("b", {"id": i}) for i in range(10, 15)]
        consumer = await Consumer(client, ["a", "b"])
        [await client.xadd("a", {"id": i}) for i in range(5, 10)]
        [await client.xadd("b", {"id": i}) for i in range(15, 20)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(5, 10)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]
        ]
        assert list(range(15, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_single_consumer_start_from_beginning(self, client, _s):
        [await client.xadd("a", {"id": i}) for i in range(5)]
        [await client.xadd("b", {"id": i}) for i in range(10, 15)]
        consumer = await Consumer(client, ["a", "b"], a={"identifier": "0-0"})
        [await client.xadd("a", {"id": i}) for i in range(5, 10)]
        [await client.xadd("b", {"id": i}) for i in range(15, 20)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(0, 10)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]
        ]
        assert list(range(15, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_single_group_consumer(self, client, _s):
        with pytest.raises(StreamConsumerInitializationError):
            await GroupConsumer(client, ["a", "b"], "group-a", "consumer-a", auto_create=False)
        await client.xgroup_create("a", "group-a", "$", mkstream=True)
        await client.xgroup_create("b", "group-a", "$", mkstream=True)

        consumer = await GroupConsumer(
            client, ["a", "b"], "group-a", "consumer-a", auto_create=False
        )
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10, 20)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]
        assert list(range(10, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_add_stream_to_single_group_consumer(self, client, _s):
        await client.xgroup_create("a", "group-a", "$", mkstream=True)
        await client.xgroup_create("b", "group-a", "$", mkstream=True)

        consumer = await GroupConsumer(
            client, ["a", "b"], "group-a", "consumer-a", auto_create=False
        )
        consumer_auto_create = await GroupConsumer(
            client, ["a", "b"], "group-b", "consumer-autocreate", auto_create=True
        )

        [await client.xadd("a", {"id": i}) for i in range(10)]
        consumed = await consume_entries(consumer, 10)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]

        with pytest.raises(StreamConsumerInitializationError):
            await consumer.add_stream("c")

        await client.xgroup_create("c", "group-a", "$", mkstream=True)
        assert await consumer.add_stream("c")
        assert await consumer_auto_create.add_stream("c")

        await client.xadd("c", {"id": 11})
        consumed = await consume_entries(consumer, 1)
        assert int(consumed[_s("c")][0].field_values[_s("id")]) == 11
        consumed = await consume_entries(consumer_auto_create, 11)
        assert int(consumed[_s("c")][-1].field_values[_s("id")]) == 11

    async def test_single_group_consumer_auto_create_group_stream(self, client, _s):
        consumer = await GroupConsumer(
            client, ["a", "b"], "group-a", "consumer-a", auto_create=True
        )
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10, 20)]
        consumed = await consume_entries(consumer, 20)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]
        assert list(range(10, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_multiple_group_consumer_auto_create_group_stream(self, client, cloner, _s):
        client_2 = await cloner(client)
        consumer_1 = await GroupConsumer(
            client, ["a", "b"], "group-a", "consumer-1", auto_create=True
        )
        consumer_2 = await GroupConsumer(
            client_2, ["a", "b"], "group-a", "consumer-2", auto_create=True
        )
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10, 20)]
        consumed = await consume_entries(consumer_1, 20)
        consumed = await consume_entries(consumer_2, 20, consumed)
        assert list(range(10)) == [int(entry.field_values[_s("id")]) for entry in consumed[_s("a")]]
        assert list(range(10, 20)) == [
            int(entry.field_values[_s("id")]) for entry in consumed[_s("b")]
        ]

    async def test_group_consumer_start_from_pending_list(self, client, _s):
        consumer = await GroupConsumer(
            client, ["a", "b"], "group-a", "consumer-1", auto_create=True
        )
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10)]
        [await consumer.get_entry() for _ in range(10)]

        consumer = await GroupConsumer(
            client,
            ["a", "b"],
            "group-a",
            "consumer-1",
            start_from_backlog=True,
            auto_create=True,
        )
        [await client.xadd("a", {"id": i}) for i in range(10, 15)]
        [await client.xadd("b", {"id": i}) for i in range(10, 15)]

        consumed = {}
        for i in range(30):
            stream, entry = await consumer.get_entry()
            await client.xack(stream, "group-a", [entry.identifier])
            consumed.setdefault(stream, []).append(int(entry.field_values[_s("id")]))
        assert list(range(15)) == consumed[_s("a")]
        assert list(range(15)) == consumed[_s("b")]

        assert not consumer.state[_s("a")].get("pending")
        assert not consumer.state[_s("b")].get("pending")

        assert (None, None) == await consumer.get_entry()
        assert (None, None) == await consumer.get_entry()

        await client.xadd("a", {"id": "a1"})
        await client.xadd("b", {"id": "b1"})
        consumed = set()
        for _ in range(2):
            k = await consumer.get_entry()
            if k:
                consumed.add(k[1].field_values[_s("id")])
        assert {_s("a1"), _s("b1")} == consumed

    async def test_single_consumer_buffered(self, client, _s):
        consumer = await Consumer(client, ["a"], buffer_size=10)
        expected = []
        for i in range(10):
            await client.xadd("a", {"id": i})
            expected.append(i)
        consumed = set()
        for _ in range(10):
            entry = await consumer.get_entry()
            if entry:
                consumed.add(int(entry[1].field_values[_s("id")]))
        assert set(expected) == consumed

    async def test_group_consumer_buffered(self, client, _s):
        consumer = await GroupConsumer(
            client, ["a"], "group-a", "consumer-a", buffer_size=10, auto_create=True
        )
        expected = []
        for i in range(10):
            await client.xadd("a", {"id": i})
            expected.append(i)
        consumed = set()
        for _ in range(10):
            entry = await consumer.get_entry()
            if entry:
                consumed.add(int(entry[1].field_values[_s("id")]))
        assert set(expected) == consumed

    async def test_single_blocking_consumer(self, client, cloner, _s):
        consumer = await Consumer(client, ["a"], timeout=1000)
        clone = await cloner(client)

        async def _inner():
            await asyncio.sleep(0.2)
            await clone.xadd("a", {"id": 1})

        th = threading.Thread(
            target=asyncio.run_coroutine_threadsafe,
            args=(_inner(), asyncio.get_running_loop()),
        )
        th.start()
        _, entry = await consumer.get_entry()
        th.join()
        assert entry.field_values[_s("id")] == _s(1)

    async def test_group_blocking_consumer(self, client, cloner, _s):
        consumer = await GroupConsumer(
            client, ["a"], "group-a", "consumer-a", auto_create=True, timeout=1000
        )
        clone = await cloner(client)

        async def _inner():
            await asyncio.sleep(0.2)
            await clone.xadd("a", {"id": 1})

        th = threading.Thread(
            target=asyncio.run_coroutine_threadsafe,
            args=(_inner(), asyncio.get_running_loop()),
        )
        th.start()
        _, entry = await consumer.get_entry()
        th.join()
        assert entry.field_values[_s("id")] == _s(1)

    async def test_single_non_blocking_iterator(self, client, _s):
        consumer = await Consumer(client, ["a", "b"])
        consumed = {}
        [await client.xadd("a", {"id": i}) for i in range(10)]
        [await client.xadd("b", {"id": i}) for i in range(10)]
        async for stream, entry in consumer:
            consumed.setdefault(stream, []).append(int(entry.field_values[_s("id")]))
        assert consumed[_s("a")] == list(range(10))
        assert consumed[_s("b")] == list(range(10))

    async def test_single_blocking_iterator(self, client, cloner, _s):
        consumer = await Consumer(client, ["a"], timeout=1000)
        clone = await cloner(client)

        async def _inner():
            await asyncio.sleep(0.2)
            await clone.xadd("a", {"id": 1})

        th = threading.Thread(
            target=asyncio.run_coroutine_threadsafe,
            args=(_inner(), asyncio.get_running_loop()),
        )
        th.start()
        consumed = {}

        async for stream, entry in consumer:
            consumed.setdefault(stream, []).append(entry)
        th.join()
        assert len(consumed[_s("a")]) == 1
        assert _s(1) == consumed[_s("a")][0].field_values[_s("id")]

    async def test_group_blocking_iterator(self, client, cloner, _s):
        consumer = await GroupConsumer(
            client, ["a"], "group-a", "consumer-a", auto_create=True, timeout=1000
        )
        clone = await cloner(client)

        async def _inner():
            await asyncio.sleep(0.2)
            await clone.xadd("a", {"id": 1})

        th = threading.Thread(
            target=asyncio.run_coroutine_threadsafe,
            args=(_inner(), asyncio.get_running_loop()),
        )
        th.start()
        consumed = {}

        async for stream, entry in consumer:
            consumed.setdefault(stream, []).append(entry)
        th.join()
        assert len(consumed[_s("a")]) == 1
        assert _s(1) == consumed[_s("a")][0].field_values[_s("id")]
