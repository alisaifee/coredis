from __future__ import annotations

import coredis.client
from coredis import BaseConnection
from coredis.cache import AbstractCache, CacheStats
from coredis.typing import RedisValueT, ResponseType
from tests.conftest import targets


class DummyCache(AbstractCache):
    def __init__(self, dummy={}):
        self.dummy = dummy

    async def initialize(self, client: coredis.client.Client) -> AbstractCache:
        return self

    @property
    def healthy(self) -> bool:
        return True

    def get(self, command: bytes, key: bytes, *args: RedisValueT) -> ResponseType:
        return self.dummy[key]

    def put(self, command: bytes, key: bytes, *args: RedisValueT, value: ResponseType) -> None:
        self.dummy[key] = value

    def reset(self) -> None:
        self.dummy.clear()

    def invalidate(self, *keys: RedisValueT) -> None:
        for key in keys:
            self.dummy.pop(key, None)

    @property
    def stats(self) -> CacheStats:
        return CacheStats()

    @property
    def confidence(self) -> float:
        return 100

    def feedback(self, command: bytes, key: bytes, *args: RedisValueT, match: bool) -> None:
        pass

    def get_client_id(self, connection: BaseConnection) -> int | None:
        return connection.tracking_client_id

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
        cache = DummyCache({"fubar": _s("1")})
        cached = await cloner(client, cache=cache)
        assert _s("1") == await cached.get("fubar")

    async def test_cache_with_no_reply(self, client, cloner, _s):
        cache = DummyCache({"fubar": _s("1")})
        cached = await cloner(client, cache=cache)
        assert _s("1") == await cached.get("fubar")
        with cached.ignore_replies():
            assert await cached.get("fubar") is None
        assert _s("1") == await cached.get("fubar")

    async def test_cache_miss(self, client, cloner, _s):
        cache = DummyCache({})
        cached = await cloner(client, cache=cache)
        assert not await cached.get("fubar")
        assert not await cached.get("fubar")
        await cached.set("fubar", 1)
        assert _s("1") == await cached.get("fubar")
