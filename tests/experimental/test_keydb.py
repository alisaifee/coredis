from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from coredis.exceptions import ResponseError
from tests.conftest import targets


@targets(
    "keydb",
    "keydb_resp2",
    "keydb_cluster",
)
@pytest.mark.asyncio()
@pytest.mark.flaky
class TestKeyDBCommands:
    @pytest.mark.nocluster
    async def test_bitop_lshift(self, client, _s):
        await client.bitfield("a{foo}").set("u16", 0, 42).exc()
        assert 42 == (await client.bitfield("a{foo}").get("u16", 0).exc())[0]
        await client.bitop(["a{foo}"], "lshift", "a{foo}", 1)
        assert 84 == (await client.bitfield("a{foo}").get("u16", 0).exc())[0]

    @pytest.mark.nocluster
    async def test_bitop_rshift(self, client, _s):
        await client.bitfield("a{foo}").set("u16", 0, 42).exc()
        assert 42 == (await client.bitfield("a{foo}").get("u16", 0).exc())[0]
        await client.bitop(["a{foo}"], "rshift", "a{foo}", 1)
        assert 21 == (await client.bitfield("a{foo}").get("u16", 0).exc())[0]

    @pytest.mark.nocluster
    async def test_cron_single(self, client, _s):
        scrpt = """
        local value = tonumber(redis.call("LPOP", KEYS[1]))
        redis.call("LPUSH", KEYS[1], value + tonumber(ARGV[1]))
        """
        await client.lpush("cs{fu}", [1])
        assert await client.cron(
            "single{fu}", False, delay=10, script=scrpt, keys=["cs{fu}"], args=[1]
        )
        assert await client.lrange("cs{fu}", 0, -1) == [_s("1")]
        await asyncio.sleep(0.2)
        assert await client.lrange("cs{fu}", 0, -1) == [_s("2")]

    @pytest.mark.nocluster
    async def test_cron_single_start_at(self, client, _s):
        scrpt = """
        local value = tonumber(redis.call("LPOP", KEYS[1]))
        redis.call("LPUSH", KEYS[1], value + tonumber(ARGV[1]))
        """
        await client.lpush("css", [1])
        assert await client.cron(
            "single",
            False,
            delay=1,
            start=datetime.now() + timedelta(milliseconds=100),
            script=scrpt,
            keys=["css"],
            args=[1],
        )
        assert await client.lrange("css", 0, -1) == [_s("1")]
        await asyncio.sleep(0.2)
        assert await client.lrange("css", 0, -1) == [_s("2")]

    @pytest.mark.nocluster
    async def test_cron_repeat(self, client, _s):
        scrpt = """
        local value = tonumber(redis.call("LPOP", KEYS[1]))
        redis.call("LPUSH", KEYS[1], value + tonumber(ARGV[1]))
        """
        await client.lpush("cr", [1])
        assert await client.cron(
            "repeat", True, delay=100, script=scrpt, keys=["cr"], args=[1]
        )
        await asyncio.sleep(0.5)
        value = int((await client.lrange("cr", 0, -1))[0])
        assert value > 2, value
        assert await client.delete(["repeat"])
        await asyncio.sleep(0.5)
        assert await client.lrange("cr", 0, -1) == [_s(value)]

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

    async def test_expirememberat_hash(self, client, _s):
        await client.hset("a", {"b": "1"})
        assert await client.hget("a", "b") == _s("1")
        assert await client.expirememberat("a", "b", datetime.now())
        assert 0 == await client.ttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.hget("a", "b")

    async def test_expirememberat_set(self, client, _s):
        await client.sadd("a", {"b"})
        assert await client.smembers("a") == {_s("b")}
        assert await client.expirememberat("a", "b", datetime.now())
        assert 0 == await client.ttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.smembers("a")

    async def test_expirememberat_sorted_set(self, client, _s):
        await client.zadd("a", {"b": 1.0})
        assert await client.zrandmember("a", count=1) == [_s("b")]
        assert await client.expirememberat("a", "b", datetime.now())
        assert 0 == await client.ttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.zrandmember("a", count=1, withscores=True)

    async def test_pexpirememberat_hash(self, client, _s):
        await client.hset("a", {"b": "1"})
        assert await client.hget("a", "b") == _s("1")
        assert await client.pexpirememberat(
            "a", "b", datetime.now() + timedelta(milliseconds=100)
        )
        assert 0 < await client.pttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.hget("a", "b")

    async def test_pexpirememberat_set(self, client, _s):
        await client.sadd("a", {"b"})
        assert await client.smembers("a") == {_s("b")}
        assert await client.pexpirememberat(
            "a", "b", datetime.now() + timedelta(milliseconds=100)
        )
        assert 0 < await client.pttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.smembers("a")

    async def test_pexpirememberat_sorted_set(self, client, _s):
        await client.zadd("a", {"b": 1.0})
        assert await client.zrandmember("a", count=1) == [_s("b")]
        assert await client.pexpirememberat(
            "a", "b", datetime.now() + timedelta(milliseconds=100)
        )
        assert 0 < await client.pttl("a", "b")
        await asyncio.sleep(0.2)
        assert not await client.zrandmember("a", count=1, withscores=True)

    async def test_hrename(self, client, _s):
        await client.hset("a", {"b": 2, "c": 3, "d": 4})
        assert await client.hrename("a", "b", "f")
        assert await client.hgetall("a") == {
            _s("c"): _s(3),
            _s("d"): _s(4),
            _s("f"): _s(2),
        }
        with pytest.raises(ResponseError):
            await client.hrename("a", "b", "f")

    async def test_mexists(self, client, _s):
        await client.mset({"a{fu}": 1, "b{fu}": 2, "c{fu}": 3, "e{fu}": 5})
        await client.mexists(["a{fu}", "b{fu}", "c{fu}", "d{fu}", "e{fu}"]) == (
            True,
            True,
            True,
            True,
            False,
            True,
        )

    @pytest.mark.xfail
    async def test_object_lastmodified(self, client, _s):
        await client.set("a", 1)
        lm = await client.object_lastmodified("a")
        await asyncio.sleep(1.1)
        assert 1 >= await client.object_lastmodified("a") - lm
