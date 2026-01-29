# python std lib
from __future__ import annotations

import time
from collections import Counter

import anyio
import pytest

from coredis._utils import b, hash_slot
from tests.conftest import targets


async def wait_for_message(pubsub, timeout=1, ignore_subscribe_messages=False):
    now = time.time()
    timeout = now + timeout

    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages, timeout=0.01
        )

        if message is not None:
            return message
        await anyio.sleep(0.01)
        now = time.time()

    return None


def make_message(type, channel, data, pattern=None):
    if type in ["subscribe", "psubscribe", "unsubscribe", "punsubscribe"]:
        return {
            "type": type,
            "pattern": channel if type[0] == "p" else pattern,
            "channel": channel,
            "data": data,
        }
    else:
        return {
            "type": type,
            "pattern": pattern,
            "channel": channel,
            "data": data,
        }


def make_subscribe_test_data(pubsub, type, sharded=False):
    if type == "channel":
        return {
            "p": pubsub,
            "sub_type": "subscribe" if not sharded else "ssubscribe",
            "unsub_type": "unsubscribe" if not sharded else "sunsubscribe",
            "sub_func": pubsub.subscribe,
            "unsub_func": pubsub.unsubscribe,
            "keys": ["foo", "bar", "uni" + chr(4456) + "code"],
        }
    elif type == "pattern" and not sharded:
        return {
            "p": pubsub,
            "sub_type": "psubscribe",
            "unsub_type": "punsubscribe",
            "sub_func": pubsub.psubscribe,
            "unsub_func": pubsub.punsubscribe,
            "keys": ["f*", "b*", "uni" + chr(4456) + "*"],
        }
    assert False, f"invalid subscribe type: {type}"


class TestPubSubSubscribeUnsubscribe:
    async def _test_subscribe_unsubscribe(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys, sharded=False
    ):
        async with p:
            counter = Counter()
            for key in keys:
                assert await sub_func(key) is None

            # should be a message for each channel/pattern we just subscribed to
            expected = set()
            received = set()

            for i, key in enumerate(keys):
                if sharded:
                    node_key = p.connection_pool.nodes.node_from_slot(hash_slot(b(key))).node_id
                else:
                    node_key = "legacy"
                counter[node_key] += 1
                received.add(tuple((await wait_for_message(p)).items()))
                expected.add(tuple(make_message(sub_type, key, counter[node_key]).items()))

            assert expected == received
            expected.clear()
            received.clear()
            for key in keys:
                assert await unsub_func(key) is None
            for i, key in enumerate(keys):
                if sharded:
                    node_key = p.connection_pool.nodes.node_from_slot(hash_slot(b(key))).node_id
                else:
                    node_key = "legacy"
                counter[node_key] -= 1
                received.add(tuple((await wait_for_message(p)).items()))
                expected.add(tuple(make_message(unsub_type, key, counter[node_key]).items()))
            assert expected == received

    async def test_channel_subscribe_unsubscribe(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_sharded_channel_subscribe_unsubscribe(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.sharded_pubsub(), "channel", sharded=True)
        await self._test_subscribe_unsubscribe(**kwargs, sharded=True)

    async def test_pattern_subscribe_unsubscribe(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "pattern")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def _test_resubscribe_on_reconnection(
        self, p, sub_type, sub_func, keys, *args, sharded=False, **kwargs
    ):
        async with p:
            counter = Counter()

            for key in keys:
                assert await sub_func(key) is None
            expected = set()
            received = set()
            for i, key in enumerate(keys):
                if sharded:
                    node_key = p.connection_pool.nodes.node_from_slot(hash_slot(b(key))).node_id
                else:
                    node_key = "legacy"
                counter[node_key] += 1
                expected.add(tuple(make_message(sub_type, key, counter[node_key]).items()))
                received.add(tuple((await wait_for_message(p)).items()))

            assert expected == received
            if sharded:
                [await c.connection.send_eof() for c in p.shard_connections.values()]
            else:
                await p.connection.connection.send_eof()

            messages = []
            await anyio.sleep(1)
            for i, _ in enumerate(keys):
                messages.append(await wait_for_message(p))
            unique_channels = set()
            assert len(messages) == len(keys)

            for i, message in enumerate(messages):
                assert message["type"] == sub_type
                if not sharded:
                    assert message["data"] == i + 1
                channel = message["channel"]
                unique_channels.add(channel)

            assert len(unique_channels) == len(keys)

            for channel in unique_channels:
                assert channel in keys

    async def test_subscribe_on_construct(self, redis_cluster):
        handled = []

        def handle(message):
            handled.append(message["data"])

        async with redis_cluster.pubsub(
            ignore_subscribe_messages=True,
            channels=["foo"],
            channel_handlers={"bar": handle},
            patterns=["baz*"],
            pattern_handlers={"qu*": handle},
        ) as pubsub:
            assert pubsub.subscribed
            await redis_cluster.publish("foo", "bar")
            await redis_cluster.publish("bar", "foo")
            await redis_cluster.publish("baz", "qux")
            await redis_cluster.publish("qux", "quxx")
            assert (await wait_for_message(pubsub, ignore_subscribe_messages=True))["data"] == "bar"
            assert (await wait_for_message(pubsub, ignore_subscribe_messages=True))["data"] == "qux"

        assert handled == ["foo", "quxx"]
        assert not pubsub.subscribed

    async def test_sharded_subscribe_on_construct(self, redis_cluster):
        handled = []

        def handle(message):
            handled.append(message["data"])

        async with redis_cluster.sharded_pubsub(
            ignore_subscribe_messages=True, channels=["foo"], channel_handlers={"bar": handle}
        ) as pubsub:
            assert pubsub.subscribed
            await redis_cluster.spublish("foo", "bar")
            await redis_cluster.spublish("bar", "foo")
            assert (await wait_for_message(pubsub, ignore_subscribe_messages=True))["data"] == "bar"

        assert handled == ["foo"]
        assert not pubsub.subscribed

    async def test_resubscribe_to_channels_on_reconnection(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "channel")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_sharded_resubscribe_to_channels_on_reconnection(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.sharded_pubsub(), "channel", sharded=True)
        await self._test_resubscribe_on_reconnection(**kwargs, sharded=True)

    async def test_resubscribe_to_patterns_on_reconnection(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "pattern")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def _test_subscribed_property(self, p, sub_type, unsub_type, sub_func, unsub_func, keys):
        async with p:
            assert p.subscribed is False
            await sub_func(keys[0])
            # we're now subscribed even though we haven't processed the
            # reply from the server just yet
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)
            # we're still subscribed
            assert p.subscribed is True

            # unsubscribe from all channels
            await unsub_func()
            # we're still technically subscribed until we process the
            # response messages from the server
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
            # now we're no longer subscribed as no more messages can be delivered
            # to any channels we were listening to
            assert p.subscribed is False

            # subscribing again flips the flag back
            await sub_func(keys[0])
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)

            # unsubscribe again
            await unsub_func()
            assert p.subscribed is True
            # subscribe to another channel before reading the unsubscribe response
            await sub_func(keys[1])
            assert p.subscribed is True
            # read the unsubscribe for key1
            assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
            # we're still subscribed to key2, so subscribed should still be True
            assert p.subscribed is True
            # read the key2 subscribe message
            assert await wait_for_message(p) == make_message(sub_type, keys[1], 1)
            await unsub_func()
            # haven't read the message yet, so we're still subscribed
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(unsub_type, keys[1], 0)
            # now we're finally unsubscribed
            assert p.subscribed is False

    async def test_subscribe_timeout(self, redis_cluster):
        async with redis_cluster.pubsub(subscription_timeout=1e-4) as pubsub:
            with pytest.raises(TimeoutError, match="Subscription timed out"):
                await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        async with redis_cluster.pubsub(subscription_timeout=1e-4) as pubsub:
            with pytest.raises(TimeoutError, match="Subscription timed out"):
                await pubsub.psubscribe(*(f"topic{k}-*" for k in range(100)))
        async with redis_cluster.pubsub(subscription_timeout=1) as pubsub:
            await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        with pytest.RaisesGroup(pytest.RaisesExc(TimeoutError, match="Subscription timed out")):
            async with redis_cluster.pubsub(
                subscription_timeout=1e-4,
                channels=[f"topic{k}" for k in range(100)],
            ) as pubsub:
                pass

    async def test_shareded_subscribe_timeout(self, redis_cluster):
        async with redis_cluster.sharded_pubsub(subscription_timeout=1e-4) as pubsub:
            with pytest.raises(TimeoutError, match="Subscription timed out"):
                await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        async with redis_cluster.sharded_pubsub(subscription_timeout=1) as pubsub:
            await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        with pytest.RaisesGroup(pytest.RaisesExc(TimeoutError, match="Subscription timed out")):
            async with redis_cluster.sharded_pubsub(
                subscription_timeout=1e-4,
                channels=[f"topic{k}" for k in range(100)],
            ) as pubsub:
                pass

    async def test_subscribe_property_with_channels(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "channel")
        await self._test_subscribed_property(**kwargs)

    async def test_subscribe_property_with_patterns(self, redis_cluster):
        kwargs = make_subscribe_test_data(redis_cluster.pubsub(), "pattern")
        await self._test_subscribed_property(**kwargs)

    async def test_ignore_all_subscribe_messages(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            checks = (
                (p.subscribe, "foo"),
                (p.unsubscribe, "foo"),
                (p.psubscribe, "f*"),
                (p.punsubscribe, "f*"),
            )

            assert p.subscribed is False

            for func, channel in checks:
                assert await func(channel) is None
                assert p.subscribed is True
                assert await wait_for_message(p) is None
            assert p.subscribed is False

    async def test_ignore_individual_subscribe_messages(self, redis_cluster):
        async with redis_cluster.pubsub() as p:
            checks = (
                (p.subscribe, "foo"),
                (p.unsubscribe, "foo"),
                (p.psubscribe, "f*"),
                (p.punsubscribe, "f*"),
            )

            assert p.subscribed is False

            for func, channel in checks:
                assert await func(channel) is None
                assert p.subscribed is True
                message = await wait_for_message(p, ignore_subscribe_messages=True)
                assert message is None
            assert p.subscribed is False


class TestPubSubMessages:
    """
    Bug: Currently in cluster mode publish command will behave different then in
         standard/non cluster mode. See (docs/Pubsub.md) for details.

         Currently Redis instances will be used to test pubsub because they
         are easier to work with.
    """

    def setup_method(self, *args):
        self.message = None

    def message_handler(self, message):
        self.message = message

    async def test_published_message_to_channel(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            assert p.subscribed
            await redis_cluster.publish("foo", "test message")
            message = await wait_for_message(p)
            assert message == make_message("message", "foo", "test message")

    @pytest.mark.parametrize(
        "pubsub_arguments",
        [({"read_from_replicas": False}), ({"read_from_replicas": True})],
    )
    async def test_published_message_to_sharded_channel(self, redis_cluster, pubsub_arguments):
        shards = {"a", "b", "c"}
        async with redis_cluster.sharded_pubsub(
            ignore_subscribe_messages=True, **pubsub_arguments
        ) as p:
            for shard in shards:
                await p.subscribe(f"foo{{{shard}}}")

            # no point checking the response since cluster publish only returns the
            # count for consumers listening on the same node.
            for shard in shards:
                await redis_cluster.spublish(f"foo{{{shard}}}", "test message")
            messages = []
            for _ in range(3):
                messages.append(await wait_for_message(p))
            assert all(isinstance(message, dict) for message in messages), messages
            assert {m["channel"] for m in messages} == {f"foo{{{shard}}}" for shard in shards}
            assert not await wait_for_message(p)
            await redis_cluster.spublish("foo{a}", "test message")
            assert await wait_for_message(p) == make_message("message", "foo{a}", "test message")

    async def test_published_message_to_pattern(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            await p.psubscribe("f*")
            # 1 to pattern, 1 to channel
            await redis_cluster.publish("foo", "test message")

            message1 = await wait_for_message(p)
            message2 = await wait_for_message(p)
            assert isinstance(message1, dict)
            assert isinstance(message2, dict)

            expected = [
                make_message("message", "foo", "test message"),
                make_message("pmessage", "foo", "test message", pattern="f*"),
            ]

            assert message1 in expected
            assert message2 in expected
            assert message1 != message2

    async def test_channel_message_handler(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe(foo=self.message_handler)
            await redis_cluster.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message("message", "foo", "test message")

    async def test_pattern_message_handler(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            await p.psubscribe(**{"f*": self.message_handler})
            await redis_cluster.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message("pmessage", "foo", "test message", pattern="f*")

    async def test_unicode_channel_message_handler(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            channel = "uni" + chr(4456) + "code"
            channels = {channel: self.message_handler}
            await p.subscribe(**channels)
            await redis_cluster.publish(channel, "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message("message", channel, "test message")

    async def test_unicode_pattern_message_handler(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            pattern = "uni" + chr(4456) + "*"
            channel = "uni" + chr(4456) + "code"
            await p.psubscribe(**{pattern: self.message_handler})
            await redis_cluster.publish(channel, "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message(
                "pmessage", channel, "test message", pattern=pattern
            )

    async def test_pubsub_message_iterator(self, redis_cluster):
        async with redis_cluster.pubsub(ignore_subscribe_messages=True) as p:
            messages = []
            await p.psubscribe("fu*")
            await p.subscribe("test")
            [await redis_cluster.publish("fubar", str(i)) for i in range(10)]
            [await redis_cluster.publish("test", str(i + 10)) for i in range(10)]

            async def collect():
                [messages.append(message) async for message in p]

            async def unsubscribe():
                await anyio.sleep(0.1)
                await p.punsubscribe("fu*")
                await p.unsubscribe("test")

            async with anyio.create_task_group() as tg:
                tg.start_soon(collect)
                tg.start_soon(unsubscribe)
            assert len(messages) == 20

    async def test_sharded_pubsub_message_iterator(self, redis_cluster):
        async with redis_cluster.sharded_pubsub(ignore_subscribe_messages=True) as p:
            messages = []
            await p.subscribe("test")
            [await redis_cluster.spublish("test", str(i)) for i in range(10)]

            async def collect():
                [messages.append(message) async for message in p]

            async def unsubscribe():
                await anyio.sleep(0.1)
                await p.unsubscribe("test")

            async with anyio.create_task_group() as tg:
                tg.start_soon(collect)
                tg.start_soon(unsubscribe)

            assert len(messages) == 10

    async def test_pubsub_handlers(self, redis_cluster):
        async with redis_cluster.pubsub() as p:
            messages = set()

            def handler(message):
                messages.add(message["data"])

            await p.subscribe(fu=handler)
            await p.psubscribe(**{"bar*": handler})

            await redis_cluster.publish("fu", "bar")
            await redis_cluster.publish("bar", "fu")

            await anyio.sleep(0.1)

            assert messages == {"fu", "bar"}


@targets("redis_cluster", "redis_cluster_raw")
class TestPubSubPubSubSubcommands:
    async def test_pubsub_channels(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo", "bar", "baz", "quux")
            channels = set(await client.pubsub_channels())
            assert {_s("bar"), _s("baz"), _s("foo"), _s("quux")}.issubset(channels)
            await p.unsubscribe()

    async def test_pubsub_shardchannels(self, client, _s):
        async with client.sharded_pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo", "bar", "baz", "quux")
            channels = sorted(await client.pubsub_shardchannels())
            assert channels == [_s("bar"), _s("baz"), _s("foo"), _s("quux")]
            await p.unsubscribe()

    async def test_pubsub_shardnumsub(self, client, _s):
        async with (
            client.sharded_pubsub(ignore_subscribe_messages=True) as p1,
            client.sharded_pubsub(ignore_subscribe_messages=True) as p2,
            client.sharded_pubsub(ignore_subscribe_messages=True) as p3,
        ):
            await p1.subscribe("foo", "bar", "baz")
            await p2.subscribe("bar", "baz")
            await p3.subscribe("baz")

            channels = {_s("foo"): 1, _s("bar"): 2, _s("baz"): 3}
            assert channels == await client.pubsub_shardnumsub("foo", "bar", "baz")

    async def test_pubsub_numsub(self, client, _s):
        async with (
            client.pubsub(ignore_subscribe_messages=True) as p1,
            client.pubsub(ignore_subscribe_messages=True) as p2,
            client.pubsub(ignore_subscribe_messages=True) as p3,
        ):
            await p1.subscribe("foo", "bar", "baz")
            await p2.subscribe("bar", "baz")
            await p3.subscribe("baz")

            channels = {_s("foo"): 1, _s("bar"): 2, _s("baz"): 3}
            assert channels == await client.pubsub_numsub("foo", "bar", "baz")
