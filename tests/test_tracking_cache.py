from __future__ import annotations

import asyncio

import pytest

from coredis.cache import ClusterTrackingCache, NodeTrackingCache, TrackingCache
from tests.conftest import targets


class CommonExamples:
    @property
    def cache(self):
        return TrackingCache

    async def test_single_entry_cache(self, client, cloner, _s):
        await client.flushall()
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await cached.get("fubar")
        await client.set("fubar", 1)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        await client.incr("fubar")
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("2")
        cache.reset()
        assert await cached.get("fubar") == _s("2")

    @pytest.mark.nopypy
    async def test_max_size(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=1)
        cached = await cloner(client, cache=cache)
        await client.set("fubar", 1)
        assert _s(1) == await cached.get("fubar")
        assert _s(1) == await cached.get("fubar")

    @pytest.mark.pypyonly
    async def test_max_size_skipped(self, client, cloner, _s):
        with pytest.raises(RuntimeError):
            self.cache(max_keys=1, max_size_bytes=1)

    async def test_eviction(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await cached.get("fubar")
        assert not await cached.get("barbar")
        assert not await cached.get("fubar")
        assert not await cached.get("barbar")
        await client.set("fubar", 1)
        await client.set("barbar", 2)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        assert await cached.get("barbar") == _s("2")
        await client.pexpire("fubar", 1)
        await client.pexpire("barbar", 1)
        await asyncio.sleep(0.2)
        assert not await cached.get("fubar")
        assert not await cached.get("barbar")

    @pytest.mark.parametrize(
        "confidence, expectation",
        [
            (10, 100),
            (50, 75),
            (90, 25),
        ],
    )
    async def test_confidence(self, client, cloner, mocker, _s, confidence, expectation):
        cache = self.cache(confidence=confidence, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        [await client.set(f"fubar{i}", i) for i in range(100)]
        create_request = mocker.spy(cached.connection_pool.connection_class, "create_request")
        [await cached.get(f"fubar{i}") for i in range(100)]
        assert create_request.call_count == 100
        [await cached.get(f"fubar{i}") for i in range(100)]
        assert create_request.call_count < 100 + expectation

    async def test_feedback(self, client, cloner, mocker, _s):
        cache = self.cache(confidence=0, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)

        [await client.set(f"fubar{i}", i) for i in range(10)]

        feedback = mocker.spy(cache, "feedback")
        get = mocker.patch.object(cache, "get")
        get.return_value = _s("11")

        [await cached.get(f"fubar{i}") for i in range(10)]
        assert feedback.call_count == 10

    async def test_feedback_adjust(self, client, cloner, mocker, _s):
        cache = self.cache(confidence=50, dynamic_confidence=True, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)

        [await client.set(f"fubar{i}", i) for i in range(100)]
        [await cached.get(f"fubar{i}") for i in range(100)]

        feedback = mocker.spy(cache, "feedback")
        original_get = cache.get
        get = mocker.patch.object(cache, "get")
        get.side_effect = lambda *_: _s("11")

        [await cached.get(f"fubar{i}") for i in range(100)]
        assert feedback.call_count > 0
        assert cache.confidence < 50
        dropped = float(cache.confidence)
        mocker.resetall()
        get.side_effect = original_get

        [await cached.get(f"fubar{i}") for i in range(100)]
        assert cache.confidence > dropped
        cache.reset()
        assert cache.confidence == 50

    async def test_shared_cache(self, client, cloner, mocker, _s):
        cache = self.cache(max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        clones = [await cloner(client, cache=cache) for _ in range(5)]
        [await clone.ping() for clone in clones]
        await client.set("fubar", "test")
        await cached.get("fubar")
        spy = mocker.spy(clones[0].connection_pool.connection_class, "create_request")
        assert {await clone.get("fubar") for clone in clones} == {_s("test")}
        assert spy.call_count == 0, spy.call_args

        await client.set("fubar", "fubar")
        await asyncio.sleep(0.1)
        assert {await clone.get("fubar") for clone in clones} == {_s("fubar")}
        assert spy.call_count < 5, spy.call_args

    async def test_stats(self, client, cloner, mocker, _s):
        cache = self.cache(confidence=0, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        await client.set("barbar", "test")
        await cached.get("fubar")
        await cached.get("fubar")
        await client.set("fubar", "test")
        await asyncio.sleep(0.01)
        await cached.get("fubar")
        await cached.get("fubar")
        await cached.get("barbar")
        await cached.get("barbar")

        get = mocker.patch.object(cache, "get")
        get.side_effect = lambda *_: _s("dirty")

        await cached.get("barbar")

        assert sum(cache.stats.hits.values()) == 3
        assert sum(cache.stats.misses.values()) == 3
        assert sum(cache.stats.invalidations.values()) == 2
        assert sum(cache.stats.dirty.values()) == 1

        assert cache.stats.hits[b"fubar"] == 2
        assert cache.stats.hits[b"barbar"] == 1

        cache.stats.compact()

        assert sum(cache.stats.hits.values()) == 3
        assert sum(cache.stats.misses.values()) == 3
        assert sum(cache.stats.invalidations.values()) == 2

        assert b"fubar" not in cache.stats.hits
        assert b"barbar" not in cache.stats.hits

        assert cache.stats.summary == {
            "hits": 3,
            "misses": 3,
            "invalidations": 2,
            "dirty_hits": 1,
        }

        cache.stats.clear()
        assert cache.stats.summary == {
            "hits": 0,
            "misses": 0,
            "invalidations": 0,
            "dirty_hits": 0,
        }


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
)
class TestProxyInvalidatingCache(CommonExamples):
    async def test_uninitialized_cache(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_idle_seconds=1, max_size_bytes=-1)
        assert not cache.get_client_id(await client.connection_pool.get_connection())
        assert cache.confidence == 100
        _ = await cloner(client, cache=cache)
        assert cache.get_client_id(await client.connection_pool.get_connection()) > 0

    async def test_single_entry_cache_tracker_disconnected(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await client.get("fubar")
        await client.set("fubar", 1)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        await client.incr("fubar")
        cache.instance.connection.disconnect()
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("2")


@targets(
    "redis_cluster",
    "redis_cluster_raw",
)
class TestClusterProxyInvalidatingCache(CommonExamples):
    async def test_uninitialized_cache(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        assert not cache.get_client_id(await client.connection_pool.get_random_connection())
        assert cache.confidence == 100
        _ = await cloner(client, cache=cache)
        assert cache.get_client_id(await client.connection_pool.get_random_connection()) > 0

    async def test_single_entry_cache_tracker_disconnected(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await client.get("fubar")
        await client.set("fubar", 1)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        await client.incr("fubar")
        [ncache.connection.disconnect() for ncache in cache.instance.node_caches.values()]
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("2")

    async def test_reinitialize_cluster(self, client, cloner, _s):
        await client.set("fubar", 1)
        cache = self.cache(max_keys=1, max_idle_seconds=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        pre = dict(cached.cache.instance.node_caches)
        assert await cached.get("fubar") == _s("1")
        cached.connection_pool.disconnect()
        cached.connection_pool.reset()
        await asyncio.sleep(0.1)
        assert await cached.get("fubar") == _s("1")
        post = cached.cache.instance.node_caches
        assert pre != post


@targets(
    "redis_basic",
    "redis_basic_raw",
)
class TestNodeInvalidatingCache(CommonExamples):
    @property
    def cache(self):
        return NodeTrackingCache

    async def test_uninitialized_cache(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_idle_seconds=1, max_size_bytes=-1)
        assert not cache.get_client_id(await client.connection_pool.get_connection())
        assert cache.confidence == 100
        _ = await cloner(client, cache=cache)
        assert cache.get_client_id(await client.connection_pool.get_connection()) > 0

    async def test_single_entry_cache_tracker_disconnected(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await client.get("fubar")
        await client.set("fubar", 1)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        await client.incr("fubar")
        cache.connection.disconnect()
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("2")


@targets(
    "redis_cluster",
    "redis_cluster_blocking",
    "redis_cluster_raw",
)
class TestClusterInvalidatingCache(CommonExamples):
    @property
    def cache(self):
        return ClusterTrackingCache

    async def test_uninitialized_cache(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        assert not cache.get_client_id(await client.connection_pool.get_random_connection())
        assert cache.confidence == 100
        _ = await cloner(client, cache=cache)
        assert cache.get_client_id(await client.connection_pool.get_random_connection()) > 0

    async def test_single_entry_cache_tracker_disconnected(self, client, cloner, _s):
        cache = self.cache(max_keys=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        assert not await client.get("fubar")
        await client.set("fubar", 1)
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("1")
        await client.incr("fubar")
        [ncache.connection.disconnect() for ncache in cache.node_caches.values()]
        await asyncio.sleep(0.2)
        assert await cached.get("fubar") == _s("2")

    async def test_reinitialize_cluster(self, client, cloner, _s):
        await client.set("fubar", 1)
        cache = self.cache(max_keys=1, max_idle_seconds=1, max_size_bytes=-1)
        cached = await cloner(client, cache=cache)
        pre = dict(cached.cache.node_caches)
        assert await cached.get("fubar") == _s("1")
        cached.connection_pool.disconnect()
        cached.connection_pool.reset()
        await asyncio.sleep(0.1)
        assert await cached.get("fubar") == _s("1")
        post = cached.cache.node_caches
        assert pre != post
