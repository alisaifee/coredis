from __future__ import annotations

import pickle
import time

import anyio
import pytest

from coredis.client.basic import Redis
from coredis.commands.pubsub import PubSub
from tests.conftest import targets


async def wait_for_message(pubsub: PubSub, timeout=0.5, ignore_subscribe_messages=False):
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


def make_subscribe_test_data(pubsub, encoder, type):
    if type == "channel":
        return {
            "p": pubsub,
            "encoder": encoder,
            "sub_type": "subscribe",
            "unsub_type": "unsubscribe",
            "sub_func": pubsub.subscribe,
            "unsub_func": pubsub.unsubscribe,
            "keys": ["foo", "bar", "uni" + chr(56) + "code"],
        }
    elif type == "pattern":
        return {
            "p": pubsub,
            "encoder": encoder,
            "sub_type": "psubscribe",
            "unsub_type": "punsubscribe",
            "sub_func": pubsub.psubscribe,
            "unsub_func": pubsub.punsubscribe,
            "keys": ["f*", "b*", "uni" + chr(56) + "*"],
        }
    assert False, f"invalid subscribe type: {type}"


@targets("redis_basic", "redis_basic_raw", "dragonfly", "valkey", "redict")
class TestPubSubSubscribeUnsubscribe:
    async def _test_subscribe_unsubscribe(
        self,
        p,
        encoder,
        sub_type,
        unsub_type,
        sub_func,
        unsub_func,
        keys,
    ):
        async with p:
            for key in keys:
                assert await sub_func(key) is None

            # should be a message for each channel/pattern we just subscribed to

            for i, key in enumerate(keys):
                assert await wait_for_message(p) == make_message(sub_type, encoder(key), i + 1)

            for key in keys:
                assert await unsub_func(key) is None

            # should be a message for each channel/pattern we just unsubscribed
            # from

            for i, key in enumerate(keys):
                i = len(keys) - 1 - i
                assert await wait_for_message(p) == make_message(unsub_type, encoder(key), i)

    async def test_channel_subscribe_unsubscribe(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_pattern_subscribe_unsubscribe(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "pattern")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def _test_resubscribe_on_reconnection(
        self, p: PubSub, encoder, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        async with p:
            p.connection.max_idle_time = 1
            for key in keys:
                assert await sub_func(key) is None
            # should be a message for each channel/pattern we just subscribed to

            for i, key in enumerate(keys):
                assert await wait_for_message(p) == make_message(sub_type, encoder(key), i + 1)

            # wait for disconnect
            await anyio.sleep(2)
            # calling get_message again reconnects and resubscribes
            # note, we may not re-subscribe to channels in exactly the same order
            # so we have to do some extra checks to make sure we got them all
            messages = []

            for i in range(len(keys)):
                messages.append(await wait_for_message(p))
            unique_channels = set()
            assert len(messages) == len(keys)

            for i, message in enumerate(messages):
                assert message["type"] == sub_type
                assert message["data"] == i + 1
                channel = message["channel"]
                unique_channels.add(channel)

            assert len(unique_channels) == len(keys)

            for channel in unique_channels:
                assert channel in [encoder(k) for k in keys]
            await unsub_func()

    async def test_resubscribe_to_channels_on_reconnection(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "channel")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_to_patterns_on_reconnection(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "pattern")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def _test_subscribed_property(
        self, p, encoder, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        async with p:
            assert p.subscribed is False
            await sub_func(keys[0])
            # we're now subscribed even though we haven't processed the
            # reply from the server just yet
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(sub_type, encoder(keys[0]), 1)
            # we're still subscribed
            assert p.subscribed is True

            # unsubscribe from all channels
            await unsub_func()
            # we're still technically subscribed until we process the
            # response messages from the server
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(unsub_type, encoder(keys[0]), 0)
            # now we're no longer subscribed as no more messages can be delivered
            # to any channels we were listening to
            assert p.subscribed is False

            # subscribing again flips the flag back
            await sub_func(keys[0])
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(sub_type, encoder(keys[0]), 1)

            # unsubscribe again
            await unsub_func()
            assert p.subscribed is True
            # subscribe to another channel before reading the unsubscribe response
            await sub_func(keys[1])
            assert p.subscribed is True
            # read the unsubscribe for key1
            assert await wait_for_message(p) == make_message(unsub_type, encoder(keys[0]), 0)
            # we're still subscribed to key2, so subscribed should still be True
            assert p.subscribed is True
            # read the key2 subscribe message
            assert await wait_for_message(p) == make_message(sub_type, encoder(keys[1]), 1)
            await unsub_func()
            # haven't read the message yet, so we're still subscribed
            assert p.subscribed is True
            assert await wait_for_message(p) == make_message(unsub_type, encoder(keys[1]), 0)
            # now we're finally unsubscribed
            assert p.subscribed is False
            await p.unsubscribe()

    async def test_subscribe_property_with_channels(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "channel")
        await self._test_subscribed_property(**kwargs)

    async def test_subscribe_property_with_patterns(self, client, _s):
        kwargs = make_subscribe_test_data(client.pubsub(), _s, "pattern")
        await self._test_subscribed_property(**kwargs)

    async def test_ignore_all_subscribe_messages(self, client):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
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

    async def test_ignore_individual_subscribe_messages(self, client):
        async with client.pubsub() as p:
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

    async def test_subscribe_on_construct(self, client: Redis, _s):
        handled = []

        def handle(message):
            handled.append(message["data"])

        async with client.pubsub(
            ignore_subscribe_messages=True,
            channels=["foo"],
            channel_handlers={"bar": handle},
            patterns=["baz*"],
            pattern_handlers={"qu*": handle},
        ) as pubsub:
            await client.publish("foo", "bar")
            await client.publish("bar", "foo")
            await client.publish("baz", "qux")
            await client.publish("qux", "quxx")
            assert (await wait_for_message(pubsub, ignore_subscribe_messages=True))["data"] == _s(
                "bar"
            )
            assert (await wait_for_message(pubsub, ignore_subscribe_messages=True))["data"] == _s(
                "qux"
            )

        assert handled == [_s("foo"), _s("quxx")]

    async def test_subscribe_timeout(self, client: Redis):
        async with client.pubsub(subscription_timeout=1e-4) as pubsub:
            with pytest.raises(TimeoutError, match="Subscription timed out"):
                await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        async with client.pubsub(subscription_timeout=1e-4) as pubsub:
            with pytest.raises(TimeoutError, match="Subscription timed out"):
                await pubsub.psubscribe(*(f"topic{k}-*" for k in range(100)))
        async with client.pubsub(subscription_timeout=1) as pubsub:
            await pubsub.subscribe(*(f"topic{k}" for k in range(100)))
        with pytest.RaisesGroup(pytest.RaisesExc(TimeoutError, match="Subscription timed out")):
            async with client.pubsub(
                subscription_timeout=1e-4,
                channels=[f"topic{k}" for k in range(100)],
            ) as pubsub:
                pass


@targets("redis_basic", "redis_basic_raw")
class TestPubSubMessages:
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    async def async_message_handler(self, message):
        self.message = message

    async def test_published_message_to_channel(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            # if other tests failed, subscriber may not be cleared
            assert await client.publish("foo", "test message") >= 1

            message = await wait_for_message(p)
            assert isinstance(message, dict)
            assert message == make_message("message", _s("foo"), _s("test message"))

    async def test_published_message_to_pattern(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            await p.psubscribe("f*")
            # 1 to pattern, 1 to channel
            assert await client.publish("foo", "test message") >= 2

            message1 = await wait_for_message(p)
            message2 = await wait_for_message(p)
            assert isinstance(message1, dict)
            assert isinstance(message2, dict)

            expected = [
                make_message("message", _s("foo"), _s("test message")),
                make_message("pmessage", _s("foo"), _s("test message"), pattern=_s("f*")),
            ]

            assert message1 in expected
            assert message2 in expected
            assert message1 != message2

    async def test_published_pickled_obj_to_channel(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            msg = pickle.dumps(Exception(), protocol=0).decode("utf-8")
            # if other tests failed, subscriber may not be cleared
            assert await client.publish("foo", msg) >= 1

            message = await wait_for_message(p)
            assert isinstance(message, dict)
            assert message == make_message("message", _s("foo"), _s(msg))

    async def test_published_pickled_obj_to_pattern(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo")
            await p.psubscribe("f*")
            # 1 to pattern, 1 to channel
            msg = pickle.dumps(Exception(), protocol=0).decode("utf-8")
            assert await client.publish("foo", msg) >= 2

            message1 = await wait_for_message(p)
            message2 = await wait_for_message(p)
            assert isinstance(message1, dict)
            assert isinstance(message2, dict)

            expected = [
                make_message("message", _s("foo"), _s(msg)),
                make_message("pmessage", _s("foo"), _s(msg), pattern=_s("f*")),
            ]

            assert message1 in expected
            assert message2 in expected
            assert message1 != message2

    async def test_channel_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe(foo=self.message_handler)
            assert await client.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message("message", _s("foo"), _s("test message"))

    async def test_channel_async_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe(foo=self.async_message_handler)
            assert await client.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message("message", _s("foo"), _s("test message"))

    async def test_pattern_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.psubscribe(**{"f*": self.message_handler})
            assert await client.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message(
                "pmessage", _s("foo"), _s("test message"), pattern=_s("f*")
            )

    async def test_pattern_async_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.psubscribe(**{"f*": self.async_message_handler})
            assert await client.publish("foo", "test message")
            assert await wait_for_message(p) is None
            assert self.message == make_message(
                "pmessage", _s("foo"), _s("test message"), pattern=_s("f*")
            )

    async def test_unicode_channel_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            channel = "uni" + chr(56) + "code"
            channels = {channel: self.message_handler}
            await p.subscribe(**channels)
            assert await client.publish(channel, "test message") == 1
            assert await wait_for_message(p) is None
            assert self.message == make_message("message", _s(channel), _s("test message"))

    async def test_unicode_pattern_message_handler(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            pattern = "uni" + chr(56) + "*"
            channel = "uni" + chr(56) + "code"
            await p.psubscribe(**{pattern: self.message_handler})
            assert await client.publish(channel, "test message") == 1
            assert await wait_for_message(p) is None
            assert self.message == make_message(
                "pmessage", _s(channel), _s("test message"), pattern=_s(pattern)
            )

    async def test_pubsub_handlers(self, client, _s):
        async with client.pubsub() as p:
            messages = set()

            def handler(message):
                messages.add(message["data"])

            await p.subscribe(fu=handler)
            await p.psubscribe(**{"bar*": handler})

            await client.publish("fu", "bar")
            await client.publish("bar", "fu")

            await anyio.sleep(0.1)

            assert messages == {_s("fu"), _s("bar")}

    async def test_pubsub_message_iterator(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            messages = []
            await p.psubscribe("fu*")
            await p.subscribe("test")
            [await client.publish("fubar", str(i)) for i in range(10)]
            [await client.publish("test", str(i + 10)) for i in range(10)]

            async def collect():
                [messages.append(message) async for message in p]

            async def unsubscribe():
                await anyio.sleep(0.1)
                await p.punsubscribe("fu*")
                await p.unsubscribe("test")

            with anyio.fail_after(1):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(collect)
                    tg.start_soon(unsubscribe)
            assert len(messages) == 20


@targets("redis_basic", "redis_basic_raw")
class TestPubSubPubSubSubcommands:
    async def test_pubsub_channels(self, client, _s):
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.subscribe("foo", "bar", "baz", "quux")
            channels = set(await client.pubsub_channels())
            assert {_s("bar"), _s("baz"), _s("foo"), _s("quux")}.issubset(channels)

    async def test_pubsub_numsub(self, client, _s):
        p1 = client.pubsub(ignore_subscribe_messages=True)
        p2 = client.pubsub(ignore_subscribe_messages=True)
        p3 = client.pubsub(ignore_subscribe_messages=True)
        async with p1, p2, p3:
            await p1.subscribe("foo", "bar", "baz")
            await p2.subscribe("bar", "baz")
            await p3.subscribe("baz")

            channels = {_s("foo"): 1, _s("bar"): 2, _s("baz"): 3}
            assert channels == await client.pubsub_numsub("foo", "bar", "baz")

    async def test_pubsub_numpat(self, client):
        pubsub_count = await client.pubsub_numpat()
        async with client.pubsub(ignore_subscribe_messages=True) as p:
            await p.psubscribe("*oo", "*ar", "b*z")
            assert await client.pubsub_numpat() == 3 + pubsub_count
