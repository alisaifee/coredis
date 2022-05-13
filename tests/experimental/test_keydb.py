from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from tests.conftest import targets


@targets(
    "keydb",
    "keydb_resp3",
)
@pytest.mark.asyncio()
class TestKeyDBCommands:
    async def test_expiremember_hash(self, client, _s):
        await client.hset("a", {"b": "1"})
        assert await client.hget("a", "b") == _s("1")
        assert await client.expiremember("a", "b", 1, b"ms")
        await asyncio.sleep(0.2)
        assert not await client.hget("a", "b")

    async def test_expiremember_set(self, client, _s):
        await client.sadd("a", {"b"})
        assert await client.smembers("a") == {_s("b")}
        assert await client.expiremember("a", "b", 1, b"ms")
        await asyncio.sleep(0.2)
        assert not await client.smembers("a")

    async def test_expiremember_sorted_set(self, client, _s):
        await client.zadd("a", {"b": 1.0})
        assert await client.zrandmember("a", count=1) == [_s("b")]
        assert await client.expiremember("a", "b", 1, b"ms")
        await asyncio.sleep(0.2)
        assert not await client.zrandmember("a", count=1, withscores=True)
        assert not await client.smembers("a")

    async def test_expirememeberat_hash(self, client, _s):
        await client.hset("a", {"b": "1"})
        assert await client.hget("a", "b") == _s("1")
        assert await client.expirememberat("a", "b", datetime.now())
        await asyncio.sleep(0.2)
        assert not await client.hget("a", "b")

    async def test_expirememeberat_set(self, client, _s):
        await client.sadd("a", {"b"})
        assert await client.smembers("a") == {_s("b")}
        assert await client.expirememberat("a", "b", datetime.now())
        await asyncio.sleep(0.2)
        assert not await client.smembers("a")

    async def test_expirememeberat_sorted_set(self, client, _s):
        await client.zadd("a", {"b": 1.0})
        assert await client.zrandmember("a", count=1) == [_s("b")]
        assert await client.expirememberat("a", "b", datetime.now())
        await asyncio.sleep(0.2)
        assert not await client.zrandmember("a", count=1, withscores=True)
        assert not await client.smembers("a")
