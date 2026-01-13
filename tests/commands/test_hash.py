from __future__ import annotations

import datetime
import time

import anyio
import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError
from tests.conftest import server_deprecation_warning, targets


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
    "redis_cached",
    "redis_cluster_cached",
    "dragonfly",
    "valkey",
    "redict",
)
class TestHash:
    async def test_hget_and_hset(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert await client.hget("a", "1") == _s("1")
        assert await client.hget("a", "2") == _s("2")
        assert await client.hget("a", "3") == _s("3")

        # field was updated, redis returns 0
        assert await client.hset("a", {"2": 5}) == 0
        assert await client.hget("a", "2") == _s("5")

        # field is new, redis returns 1
        assert await client.hset("a", {"4": 4}) == 1
        assert await client.hget("a", "4") == _s("4")

        # key inside of hash that doesn't exist returns null value
        assert await client.hget("a", "b") is None

    async def test_hdel(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert await client.hdel("a", ["2"]) == 1
        assert await client.hget("a", "2") is None
        assert await client.hdel("a", ["1", "3"]) == 2
        assert await client.hlen("a") == 0

    @pytest.mark.min_server_version("8.0.0")
    async def test_hgetdel(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert (_s("1"),) == await client.hgetdel("a", ["1"])
        assert (None,) == await client.hgetdel("a", ["1"])
        assert await client.hlen("a") == 2
        assert (None, _s("2"), _s("3")) == await client.hgetdel("a", ["1", "2", "3"])
        assert 0 == await client.exists(["a"])

    async def test_hexists(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert await client.hexists("a", "1")
        assert not await client.hexists("a", "4")

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hexpire(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (1,) == await client.hexpire("a", 5, ["1"])
        assert (-2,) == await client.hexpire("missing", 1, ["missing"])
        assert (0, 1, -2) == await client.hexpire("a", 5, ["1", "3", "5"], PureToken.NX)
        assert (1, 1, -2) == await client.hexpire("a", 5, ["1", "3", "5"], PureToken.XX)
        assert (0, 0, -2) == await client.hexpire("a", 1, ["1", "3", "5"], PureToken.GT)
        assert (1, -2) == await client.hexpire("a", 1, ["4", "5"], PureToken.LT)
        assert (2, 2, -2) == await client.hexpire(
            "a", datetime.timedelta(seconds=0), ["1", "3", "5"], PureToken.LT
        )
        await anyio.sleep(1)
        assert {_s("2"): _s("2")} == await client.hgetall(_s("a"))

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hexpireat(self, client, _s, redis_server_time):
        now = await redis_server_time(client)
        now_int = int(time.mktime(now.timetuple()))
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (1,) == await client.hexpireat("a", now_int + 5, ["1"])
        assert (-2,) == await client.hexpireat("missing", now_int + 1, ["missing"])
        assert (0, 1, -2) == await client.hexpireat("a", now_int + 5, ["1", "3", "5"], PureToken.NX)
        assert (1, 1, -2) == await client.hexpireat("a", now_int + 5, ["1", "3", "5"], PureToken.XX)
        assert (0, 0, -2) == await client.hexpireat("a", now_int + 1, ["1", "3", "5"], PureToken.GT)
        assert (1, -2) == await client.hexpireat("a", now_int + 1, ["4", "5"], PureToken.LT)
        assert (2, 2, -2) == await client.hexpireat(
            "a",
            now - datetime.timedelta(seconds=1),
            ["1", "3", "5"],
            PureToken.LT,
        )
        await anyio.sleep(1)
        assert {_s("2"): _s("2")} == await client.hgetall(_s("a"))

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hexpiretime(self, client, _s, redis_server_time):
        now = await redis_server_time(client)
        now_int = int(time.mktime(now.timetuple()))
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (-2,) == await client.hexpiretime("missing", ["1"])
        assert (-1,) == await client.hexpiretime("a", ["1"])
        await client.hexpire("a", 5, ["1"])
        assert (pytest.approx(now_int + 5, abs=1), -1, -2) == await client.hexpiretime(
            "a", ["1", "2", "5"]
        )

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_httl(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (-2,) == await client.httl("missing", ["1"])
        assert (-1,) == await client.httl("a", ["1"])
        await client.hexpire("a", 5, ["1"])
        assert (pytest.approx(5, abs=1), -1, -2) == await client.httl("a", ["1", "2", "5"])

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hpexpire(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (1,) == await client.hpexpire("a", 5000, ["1"])
        assert (-2,) == await client.hpexpire("missing", 1000, ["missing"])
        assert (0, 1, -2) == await client.hpexpire("a", 5000, ["1", "3", "5"], PureToken.NX)
        assert (1, 1, -2) == await client.hpexpire("a", 5000, ["1", "3", "5"], PureToken.XX)
        assert (0, 0, -2) == await client.hpexpire("a", 1000, ["1", "3", "5"], PureToken.GT)
        assert (1, -2) == await client.hpexpire("a", 1000, ["4", "5"], PureToken.LT)
        assert (2, 2, -2) == await client.hpexpire(
            "a", datetime.timedelta(milliseconds=0), ["1", "3", "5"], PureToken.LT
        )
        await anyio.sleep(1)
        assert {_s("2"): _s("2")} == await client.hgetall(_s("a"))

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hpexpireat(self, client, _s, redis_server_time):
        now = await redis_server_time(client)
        now_ms = 1000 * int(time.mktime(now.timetuple()))
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (1,) == await client.hpexpireat("a", now_ms + 5000, ["1"])
        assert (-2,) == await client.hpexpireat("missing", now_ms + 1000, ["missing"])
        assert (0, 1, -2) == await client.hpexpireat(
            "a", now_ms + 5000, ["1", "3", "5"], PureToken.NX
        )
        assert (1, 1, -2) == await client.hpexpireat(
            "a", now_ms + 5000, ["1", "3", "5"], PureToken.XX
        )
        assert (0, 0, -2) == await client.hpexpireat(
            "a", now_ms + 1000, ["1", "3", "5"], PureToken.GT
        )
        assert (1, -2) == await client.hpexpireat("a", now_ms + 1000, ["4", "5"], PureToken.LT)
        assert (2, 2, -2) == await client.hpexpireat(
            "a",
            now - datetime.timedelta(milliseconds=1),
            ["1", "3", "5"],
            PureToken.LT,
        )
        await anyio.sleep(1)
        assert {_s("2"): _s("2")} == await client.hgetall(_s("a"))

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hpexpiretime(self, client, _s, redis_server_time):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (-2,) == await client.hpexpiretime("missing", ["1"])
        assert (-1,) == await client.hpexpiretime("a", ["1"])
        now = await redis_server_time(client)
        now_ms = 1000 * int(time.mktime(now.timetuple()))
        await client.hpexpire("a", 5000, ["1"])
        assert (
            pytest.approx(now_ms + 5000, abs=1000),
            -1,
            -2,
        ) == await client.hpexpiretime("a", ["1", "2", "5"])

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hpttl(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (-2,) == await client.hpttl("missing", ["1"])
        assert (-1,) == await client.hpttl("a", ["1"])
        await client.hpexpire("a", 5000, ["1"])
        assert (pytest.approx(5000, abs=1000), -1, -2) == await client.hpttl("a", ["1", "2", "5"])

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hpersist(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3, "4": 4})
        assert (-2,) == await client.hpersist("missing", ["1"])
        await client.hpexpire("a", 5000, ["1"])
        assert (pytest.approx(5000, abs=1000),) == await client.hpttl("a", ["1"])
        assert (1,) == await client.hpersist("a", ["1"])
        assert (-1,) == await client.hpttl("a", ["1"])

    @pytest.mark.min_server_version("8.0.0")
    async def test_hgetex(self, client, _s):
        now = datetime.datetime.now()
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert (_s("1"),) == await client.hgetex("a", ["1"], ex=10)
        assert 0 < (await client.httl("a", ["1"]))[0] <= 10
        assert (_s("1"),) == await client.hgetex("a", ["1"], px=11000)
        assert 0 < (await client.httl("a", ["1"]))[0] <= 11
        assert (_s("1"),) == await client.hgetex(
            "a", ["1"], exat=now + datetime.timedelta(seconds=12)
        )
        assert 0 < (await client.httl("a", ["1"]))[0] <= 12
        assert (_s("1"),) == await client.hgetex(
            "a", ["1"], exat=now + datetime.timedelta(milliseconds=12500)
        )
        assert 0 < (await client.httl("a", ["1"]))[0] <= 13
        assert (_s("1"),) == await client.hgetex("a", ["1"], persist=True)
        assert (-1,) == await client.httl("a", ["1"])
        with pytest.raises(CommandSyntaxError):
            await client.hgetex("a", ["1"])
        with pytest.raises(CommandSyntaxError):
            await client.hgetex("a", ["1"], ex=10, px=10000)

    async def test_hgetall(self, client, _s):
        h = {_s("a1"): _s("1"), _s("a2"): _s("2"), _s("a3"): _s("3")}
        await client.hset("a", h)
        assert await client.hgetall("a") == h

    async def test_hincrby(self, client, _s):
        assert await client.hincrby("a", "1", increment=1) == 1
        assert await client.hincrby("a", "1", increment=2) == 3
        assert await client.hincrby("a", "1", increment=-2) == 1

    async def test_hincrbyfloat(self, client, _s):
        assert await client.hincrbyfloat("a", "1", increment=1.0) == 1.0
        assert await client.hincrbyfloat("a", "1", increment=1.0) == 2.0
        assert await client.hincrbyfloat("a", "1", 1.2) == 3.2

    async def test_hkeys(self, client, _s):
        h = {"a1": "1", "a2": "2", "a3": "3"}
        await client.hset("a", h)
        local_keys = [_s(k) for k in list(iter(h.keys()))]
        remote_keys = await client.hkeys("a")
        assert sorted(local_keys) == sorted(remote_keys)

    async def test_hlen(self, client, _s):
        await client.hset("a", {"1": 1, "2": 2, "3": 3})
        assert await client.hlen("a") == 3

    async def test_hmget(self, client, _s):
        assert await client.hset("a", {"a": 1, "b": 2, "c": 3})
        assert await client.hmget("a", ["a", "b", "c", "d"]) == (
            _s("1"),
            _s("2"),
            _s("3"),
            None,
        )

    async def test_hmset(self, client, _s):
        h = {_s("a"): _s("1"), _s("b"): _s("2"), _s("c"): _s("3")}
        with server_deprecation_warning("Use :meth:`hset`", client, "4.0"):
            assert await client.hmset("a", h)
        assert await client.hgetall("a") == h

    async def test_hsetnx(self, client, _s):
        # Initially set the hash field
        assert await client.hsetnx("a", "1", "1")
        assert await client.hget("a", "1") == _s("1")
        assert not await client.hsetnx("a", "1", "2")
        assert await client.hget("a", "1") == _s("1")

    @pytest.mark.min_server_version("8.0.0")
    async def test_hsetex(self, client, _s):
        now = datetime.datetime.now()
        assert await client.hsetex("a", {"1": 1}, condition=PureToken.FNX, keepttl=True)
        assert not await client.hsetex("a", {"1": 1}, condition=PureToken.FNX, keepttl=True)
        assert await client.hsetex("a", {"1": 1}, condition=PureToken.FXX, keepttl=True)
        assert not await client.hsetex("a", {"1": 1, "2": 2}, condition=PureToken.FXX, keepttl=True)
        assert not await client.hsetex("a", {"1": 1, "2": 2}, condition=PureToken.FNX, keepttl=True)
        assert await client.hsetex("a", {"1": 1}, condition=PureToken.FXX, ex=10)
        assert 0 < (await client.httl("a", ["1"]))[0] <= 10
        assert await client.hsetex("a", {"1": 1}, condition=PureToken.FXX, px=11000)
        assert 0 < (await client.httl("a", ["1"]))[0] <= 11
        assert await client.hsetex(
            "a", {"1": 1}, condition=PureToken.FXX, exat=now + datetime.timedelta(seconds=12)
        )
        assert 0 < (await client.httl("a", ["1"]))[0] <= 12
        with pytest.raises(CommandSyntaxError):
            await client.hsetex("a", {"1": 1})
        with pytest.raises(CommandSyntaxError):
            await client.hsetex("a", {"1": 1}, ex=1, px=1000)

    async def test_hvals(self, client, _s):
        h = {"a1": "1", "a2": "2", "a3": "3"}
        await client.hset("a", h)
        local_vals = [_s(v) for v in list(iter(h.values()))]
        remote_vals = await client.hvals("a")
        assert sorted(local_vals) == sorted(remote_vals)

    async def test_hstrlen(self, client, _s):
        key = "myhash"
        myhash = {"f1": "HelloWorld", "f2": 99, "f3": -256}
        await client.hset(key, myhash)
        assert await client.hstrlen("key_not_exist", "f1") == 0
        assert await client.hstrlen(key, "f4") == 0
        assert await client.hstrlen(key, "f1") == 10
        assert await client.hstrlen(key, "f2") == 2
        assert await client.hstrlen(key, "f3") == 4

    async def test_hrandfield(self, client, _s):
        assert await client.hrandfield("key") is None
        await client.hset("key", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
        assert await client.hrandfield("key") is not None
        assert len(await client.hrandfield("key", count=2)) == 2
        # with values
        assert len(await client.hrandfield("key", count=2, withvalues=True)) == 2
        # without duplications
        assert len(await client.hrandfield("key", count=10)) == 5
        # with duplications
        assert len(await client.hrandfield("key", count=-10)) == 10
        assert await client.hrandfield("key-not-exist") is None

    async def test_hscan(self, client, _s):
        await client.hset("a", {"a": 1, "b": 2, "c": 3})
        await client.hset("b", {str(i): i for i in range(1000)})
        cursor, dic = await client.hscan("a")
        assert cursor == 0
        assert dic == {_s("a"): _s("1"), _s("b"): _s("2"), _s("c"): _s("3")}
        _, dic = await client.hscan("a", match="a")
        assert dic == {_s("a"): _s("1")}
        _, dic = await client.hscan("b", count=100)
        assert len(dic) < 1000

    @pytest.mark.min_server_version("7.4.0")
    @pytest.mark.nodragonfly
    async def test_hscan_novalues(self, client, _s):
        await client.hset("a", {"a": 1, "b": 2, "c": 3})
        await client.hset("b", {str(i): i for i in range(1000)})
        cursor, fields = await client.hscan("a", novalues=True)
        assert cursor == 0
        assert fields == (_s("a"), _s("b"), _s("c"))
        _, fields = await client.hscan("a", match="a", novalues=True)
        assert fields == (_s("a"),)
        _, fields = await client.hscan("b", count=100, novalues=True)
        assert len(fields) < 1000

    async def test_hscan_iter(self, client, _s):
        await client.hset("a", {"a": 1, "b": 2, "c": 3})
        dic = dict()
        async for data in client.hscan_iter("a"):
            dic.update(dict([data]))
        assert dic == {_s("a"): _s("1"), _s("b"): _s("2"), _s("c"): _s("3")}
        async for data in client.hscan_iter("a", match="a"):
            assert dict([data]) == {_s("a"): _s("1")}
