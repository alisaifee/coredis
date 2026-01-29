from __future__ import annotations

from contextlib import AsyncExitStack

import pytest
from anyio import sleep

from coredis.cache import LRUCache
from coredis.client.basic import Redis
from coredis.client.cluster import RedisCluster
from coredis.pool.cluster import ClusterConnectionPool
from tests.conftest import targets


class CommonExamples:
    async def test_cache_health(self, client: Redis, cloner):
        cache = LRUCache()
        cached: Redis = await cloner(client, cache=cache)
        async with cached:
            assert cached.connection_pool.cache.healthy
        assert not cached.connection_pool.cache.healthy

    async def test_single_entry_cache(self, client: Redis, cloner, _s):
        await client.flushall()
        cache = LRUCache(max_keys=1)
        cached: Redis = await cloner(client, cache=cache)
        async with cached:
            assert not await cached.get("fubar")
            await client.incr("fubar")
            await sleep(0.2)
            assert await cached.get("fubar") == _s("1")
            await client.incr("fubar")
            await sleep(0.2)
            assert await cached.get("fubar") == _s("2")
            cache.reset()
            assert await cached.get("fubar") == _s("2")

    async def test_eviction(self, client, cloner, _s):
        cache = LRUCache(max_keys=1)
        cached = await cloner(client, cache=cache)
        async with cached:
            assert not await cached.get("fubar")
            assert not await cached.get("barbar")
            assert not await cached.get("fubar")
            assert not await cached.get("barbar")
            await client.set("fubar", 1)
            await client.set("barbar", 2)
            await sleep(0.2)
            assert await cached.get("fubar") == _s("1")
            assert await cached.get("barbar") == _s("2")
            await client.pexpire("fubar", 1)
            await client.pexpire("barbar", 1)
            await sleep(0.2)
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
    async def test_confidence(self, client: Redis, cloner, mocker, _s, confidence, expectation):
        cache = LRUCache(confidence=confidence)
        cached = await cloner(client, cache=cache)
        async with cached:
            await cached.ping()
            [await client.set(f"fubar{i}", i) for i in range(100)]
            create_request = mocker.spy(cached.connection_pool.connection_class, "create_request")
            [await cached.get(f"fubar{i}") for i in range(100)]
            assert create_request.call_count >= 100
            [await cached.get(f"fubar{i}") for i in range(100)]
            assert create_request.call_count < 100 + expectation

    async def test_feedback(self, client, cloner, mocker, _s):
        cache = LRUCache(confidence=0)
        cached = await cloner(client, cache=cache)

        async with cached:
            [await client.set(f"fubar{i}", i) for i in range(10)]

            feedback = mocker.spy(cache, "feedback")
            get = mocker.patch.object(cache, "get")
            get.return_value = _s("11")

            [await cached.get(f"fubar{i}") for i in range(10)]
            assert feedback.call_count == 10

    async def test_feedback_adjust(self, client, cloner, mocker, _s):
        cache = LRUCache(confidence=50, dynamic_confidence=True)
        cached = await cloner(client, cache=cache)

        async with cached:
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

    async def test_shared_cache_and_client_cache(self, client):
        kwargs = client.connection_pool.connection_kwargs
        pool = client.connection_pool.__class__(**kwargs)
        with pytest.raises(Exception, match="mutually exclusive"):
            _ = client.__class__(
                decode_responses=client.decode_responses,
                encoding=client.encoding,
                connection_pool=pool,
                cache=LRUCache(),
            )

    async def test_stats(self, client, cloner, mocker, _s):
        cache = LRUCache(confidence=0)
        cached = await cloner(client, cache=cache)
        async with cached:
            await client.set("barbar", "test")
            await cached.get("fubar")
            await cached.get("fubar")
            await client.set("fubar", "test")
            await sleep(0.01)
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


@targets("redis_basic", "redis_basic_raw")
class TestInvalidatingCache(CommonExamples):
    async def test_uninitialized_cache(self, client, cloner, _s):
        cache = LRUCache(max_keys=1)
        assert cache.confidence == 100
        cached = await cloner(client, cache=cache)
        async with cached:
            assert cached.connection_pool.cache.get_client_id(cached)
            await sleep(0.2)  # can be flaky if we close immediately

    async def test_shared_cache(self, client, cloner, mocker, _s):
        cache = LRUCache()
        cached = await cloner(client, cache=cache)
        clones = [await cloner(client, cache=cache) for _ in range(5)]
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(cached)
            for c in clones:
                await stack.enter_async_context(c)
            [await clone.ping() for clone in clones]
            await client.set("fubar", "test")
            await cached.get("fubar")
            spy = mocker.spy(clones[0].connection_pool.connection_class, "create_request")
            assert {await clone.get("fubar") for clone in clones} == {_s("test")}
            assert spy.call_count == 0, spy.call_args

            await client.set("fubar", "fubar")
            await sleep(0.1)
            assert {await clone.get("fubar") for clone in clones} == {_s("fubar")}
            assert 0 < spy.call_count < 5, spy.call_args

    async def test_shared_cache_via_pool(self, client, cloner, mocker, _s):
        cache = LRUCache()
        kwargs = client.connection_pool.connection_kwargs
        pool = client.connection_pool.__class__(_cache=cache, **kwargs)
        clones = [await cloner(client, connection_pool=pool) for _ in range(5)]
        async with AsyncExitStack() as stack:
            for c in clones:
                await stack.enter_async_context(c)
            [await clone.ping() for clone in clones]
            await client.set("fubar", "test")
            await clones[0].get("fubar")
            spy = mocker.spy(pool.connection_class, "create_request")
            assert {await clone.get("fubar") for clone in clones} == {_s("test")}
            assert spy.call_count == 0, spy.call_args

            await client.set("fubar", "fubar")
            await sleep(0.1)
            assert {await clone.get("fubar") for clone in clones} == {_s("fubar")}
            assert 0 < spy.call_count < 5, spy.call_args


@targets(
    "redis_cluster",
    "redis_cluster_raw",
)
class TestClusterInvalidatingCache(CommonExamples):
    async def test_uninitialized_cache(self, client, _s):
        cache = LRUCache(max_keys=1)
        client = RedisCluster(
            "localhost", 7000, decode_responses=client.decode_responses, cache=cache
        )
        assert cache.confidence == 100
        async with client:
            await client.ping()
            async with client.connection_pool.acquire(node=None) as connection:
                assert client.connection_pool.cache.get_client_id(connection)

    async def test_shared_cache(self, client, mocker, _s):
        cache = LRUCache()
        client1 = RedisCluster(
            "localhost", 7000, decode_responses=client.decode_responses, cache=cache
        )
        client2 = RedisCluster(
            "localhost", 7000, decode_responses=client.decode_responses, cache=cache
        )
        async with client1, client2:
            await client2.ping()  # make sure it's initialized
            await client1.set("fubar", "test")
            await client1.get("fubar")
            spy = mocker.spy(client1.connection_pool.connection_class, "create_request")
            assert await client1.get("fubar") == _s("test")
            assert await client2.get("fubar") == _s("test")
            assert spy.call_count == 0, spy.call_args

            await client1.set("fubar", "fubar")
            await sleep(0.1)
            assert await client2.get("fubar") == _s("fubar")
            assert spy.call_count > 0, spy.call_args

    async def test_shared_cache_via_pool(self, client, mocker, _s):
        cache = LRUCache()
        pool = ClusterConnectionPool(
            startup_nodes=[{"host": "localhost", "port": 7000}],
            _cache=cache,
            decode_responses=client.decode_responses,
        )
        client = RedisCluster(connection_pool=pool)
        async with client:
            await client.set("fubar", "test")
            await client.get("fubar")
            spy = mocker.spy(pool.connection_class, "create_request")
            assert await client.get("fubar") == _s("test")
            assert spy.call_count == 0, spy.call_args

            await client.set("fubar", "fubar")
            await sleep(0.1)
            assert await client.get("fubar") == _s("fubar")
            assert spy.call_count > 0, spy.call_args
