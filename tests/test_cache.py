from __future__ import annotations

import coredis.client
from coredis.cache import AbstractCache
from coredis.typing import ResponseType, ValueT
from tests.conftest import targets


class DummyCache(AbstractCache):
    def __init__(self, dummy={}):
        self.dummy = dummy

    async def initialize(self, client: coredis.client.RedisConnection) -> AbstractCache:
        return self

    @property
    def healthy(self) -> bool:
        return True

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        return self.dummy[key]

    def put(self, command: bytes, key: bytes, *args: ValueT, value: ResponseType) -> None:
        self.dummy[key] = value

    def reset(self) -> None:
        self.dummy.clear()

    def invalidate(self, *keys: ValueT) -> None:
        for key in keys:
            self.dummy.pop(key)

    def shutdown(self) -> None:
        self.reset()


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_blocking",
    "redis_cluster_raw",
)
class TestBasicCache:
    async def test_cache_hit(self, client, cloner, _s):
        cache = DummyCache({"fubar": 1})
        cached = await cloner(client, cache=cache)
        assert 1 == await cached.get("fubar")

    async def test_cache_with_no_reply(self, client, cloner, _s):
        cache = DummyCache({"fubar": 1})
        cached = await cloner(client, cache=cache)
        assert 1 == await cached.get("fubar")
        with cached.ignore_replies():
            assert await cached.get("fubar") is None
        assert 1 == await cached.get("fubar")

    async def test_cache_miss(self, client, cloner, _s):
        cache = DummyCache({})
        cached = await cloner(client, cache=cache)
        assert not await cached.get("fubar")
        assert not await cached.get("fubar")
        await cached.set("fubar", 1)
        assert _s(1) == await cached.get("fubar")
