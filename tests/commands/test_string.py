from __future__ import annotations

import datetime

import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError
from tests.conftest import server_deprecation_warning, targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_cluster",
    "redis_cluster_raw",
    "redis_cached",
    "redis_cached_resp2",
    "redis_cluster_cached",
    "keydb",
    "keydb_resp2",
    "dragonfly",
)
@pytest.mark.asyncio()
class TestString:
    async def test_append(self, client, _s):
        assert await client.append("a", "a1") == 2
        assert await client.get("a") == _s("a1")
        assert await client.append("a", "a2") == 4
        assert await client.get("a") == _s("a1a2")

    async def test_decr(self, client, _s):
        assert await client.decr("a") == -1
        assert await client.get("a") == _s("-1")
        assert await client.decr("a") == -2
        assert await client.get("a") == _s("-2")

    async def test_decr_by(self, client, _s):
        assert await client.decrby("a", 2) == -2
        assert await client.get("a") == _s("-2")
        assert await client.decrby("a", 2) == -4
        assert await client.get("a") == _s("-4")

    async def test_incr(self, client, _s):
        assert await client.incr("a") == 1
        assert await client.get("a") == _s("1")
        assert await client.incr("a") == 2
        assert await client.get("a") == _s("2")

    async def test_incrby(self, client, _s):
        assert await client.incrby("a", 1) == 1
        assert await client.incrby("a", 4) == 5
        assert await client.get("a") == _s("5")

    async def test_incrbyfloat(self, client, _s):
        assert await client.incrbyfloat("a", 1.0) == 1.0
        assert await client.get("a") == _s("1")
        assert await client.incrbyfloat("a", 1.1) == 2.1
        assert float(await client.get("a")) == float(2.1)

    async def test_getrange(self, client, _s):
        await client.set("a", "foo")
        assert await client.getrange("a", 0, 0) == _s("f")
        assert await client.getrange("a", 0, 2) == _s("foo")
        assert await client.getrange("a", 3, 4) == _s("")

    async def test_getset(self, client, _s):
        with server_deprecation_warning("Use :meth:`set`", client, "6.2"):
            assert await client.getset("a", "foo") is None
            assert await client.getset("a", "bar") == _s("foo")
            assert await client.get("a") == _s("bar")

    async def test_get_and_set(self, client, _s):
        # get and set can't be tested independently of each other
        assert await client.get("a") is None
        byte_string = "value"
        integer = 5
        unicode_string = chr(33) + "abcd" + chr(22)
        assert await client.set("byte_string", byte_string)
        assert await client.set("integer", "5")
        assert await client.set("unicode_string", unicode_string)
        assert await client.get("byte_string") == _s(byte_string)
        assert await client.get("integer") == _s(integer)
        assert await client.get("unicode_string") == _s(unicode_string)

    @pytest.mark.min_server_version("6.2.0")
    async def test_getdel(self, client, _s):
        assert await client.getdel("a") is None
        await client.set("a", "1")
        assert await client.getdel("a") == _s("1")
        assert await client.getdel("a") is None

    @pytest.mark.min_server_version("6.2.0")
    async def test_getex(self, client, redis_server_time, _s):
        await client.set("a", "1")
        assert await client.getex("a") == _s("1")
        assert await client.ttl("a") == -1
        assert await client.getex("a", ex=60) == _s("1")
        assert await client.ttl("a") == 60
        assert await client.getex("a", px=6000) == _s("1")
        assert await client.ttl("a") == 6
        expire_at = await redis_server_time(client) + datetime.timedelta(
            minutes=1, microseconds=1000
        )
        assert await client.getex("a", pxat=expire_at) == _s("1")
        assert await client.pttl("a") < 61000
        expire_at = await redis_server_time(client) + datetime.timedelta(
            minutes=1, microseconds=1000
        )
        assert await client.getex("a", exat=expire_at) == _s("1")
        assert await client.pttl("a") < 61000
        assert await client.getex("a", persist=True) == _s("1")
        assert await client.ttl("a") == -1
        with pytest.raises(CommandSyntaxError):
            await client.getex("a", ex=1, px=1)

    @pytest.mark.min_server_version("7.0.0")
    async def test_lcs(self, client, _s):
        await client.mset({"a{fu}": "abcdefg", "b{fu}": "abdefg"})
        assert await client.lcs("a{fu}", "b{fu}") == _s("abdefg")
        assert await client.lcs("a{fu}", "b{fu}", len_=True) == 6
        assert (await client.lcs("a{fu}", "b{fu}", idx=True)).matches == (
            ((3, 6), (2, 5), None),
            ((0, 1), (0, 1), None),
        )
        match = await client.lcs("a{fu}", "b{fu}", idx=True, withmatchlen=True)
        assert match.matches == (
            ((3, 6), (2, 5), 4),
            ((0, 1), (0, 1), 2),
        )
        assert match.length == 6
        match = await client.lcs(
            "a{fu}", "b{fu}", idx=True, minmatchlen=4, withmatchlen=True
        )
        assert match.matches == (((3, 6), (2, 5), 4),)
        assert match.length == 6

    async def test_mget(self, client, _s):
        assert await client.mget(["a{foo}", "b{foo}"]) == (None, None)
        await client.set("a{foo}", "1")
        await client.set("b{foo}", "2")
        await client.set("c{foo}", "3")
        assert await client.mget(["a{foo}", "other{foo}", "b{foo}", "c{foo}"]) == (
            _s("1"),
            None,
            _s("2"),
            _s("3"),
        )

    async def test_mset(self, client, _s):
        d = {"a{foo}": "1", "b{foo}": "2", "c{foo}": "3"}
        assert await client.mset(d)

        assert await client.get("a{foo}") == _s("1")
        assert await client.get("b{foo}") == _s("2")
        assert await client.get("c{foo}") == _s("3")

    async def test_msetnx(self, client, _s):
        d = {"a{foo}": "1", "b{foo}": "2", "c{foo}": "3"}
        assert await client.msetnx(d)
        d2 = {"a{foo}": "x", "d{foo}": "4"}
        assert not await client.msetnx(d2)

        for k, v in d.items():
            assert await client.get(k) == _s(v)
        assert await client.get("d") is None

    async def test_psetex(self, client, _s):
        assert await client.psetex("a", 1000, "value")
        assert await client.get("a") == _s("value")
        assert 0 < await client.pttl("a") <= 1000

    async def test_psetex_timedelta(self, client, _s):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert await client.psetex("a", expire_at, "value")
        assert await client.get("a") == _s("value")
        assert 0 < await client.pttl("a") <= 1000

    async def test_set_nx(self, client, _s):
        assert await client.set("a", "1", condition=PureToken.NX)
        assert not await client.set("a", "2", condition=PureToken.NX)
        assert await client.get("a") == _s("1")

    async def test_set_xx(self, client, _s):
        assert not await client.set("a", "1", condition=PureToken.XX)
        assert await client.get("a") is None
        await client.set("a", "bar")
        assert await client.set("a", "2", condition=PureToken.XX)
        assert await client.get("a") == _s("2")

    async def test_set_px(self, client, _s):
        assert await client.set("a", "1", px=10000)
        assert await client.get("a") == _s("1")
        assert 0 < await client.pttl("a") <= 10000
        assert 0 < await client.ttl("a") <= 10

    async def test_set_px_timedelta(self, client, _s):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert await client.set("a", "1", px=expire_at)
        assert 0 < await client.pttl("a") <= 1000
        assert 0 < await client.ttl("a") <= 1

    async def test_set_ex(self, client, _s):
        assert await client.set("a", "1", ex=10)
        assert 0 < await client.ttl("a") <= 10

    async def test_set_ex_timedelta(self, client, _s):
        expire_at = datetime.timedelta(seconds=60)
        assert await client.set("a", "1", ex=expire_at)
        assert 0 < await client.ttl("a") <= 60

    @pytest.mark.min_server_version("6.2.0")
    async def test_set_exat(self, client, redis_server_time, _s):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        assert await client.set("a", "1", exat=expire_at)
        assert 0 < await client.ttl("a") <= 61

    @pytest.mark.min_server_version("6.2.0")
    async def test_set_pxat(self, client, redis_server_time, _s):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        assert await client.set("a", "1", pxat=expire_at)
        assert 0 < await client.ttl("a") <= 61

    @pytest.mark.min_server_version("6.2.0")
    async def test_set_get(self, client, _s):
        assert await client.set("a", "1", get=True) is None
        assert await client.set("a", "2", get=True) == _s("1")
        assert await client.set("a", "3", condition=PureToken.XX, get=True) == _s("2")

    @pytest.mark.min_server_version("7.0.0")
    async def test_set_get_nx(self, client, _s):
        assert await client.set("a", "1")
        assert await client.set("a", "2", condition=PureToken.NX, get=True) == _s("1")
        assert await client.get("a") == _s("1")

    @pytest.mark.min_server_version("6.2.0")
    async def test_set_keepttl(self, client, _s):
        assert await client.set("a", "1")
        assert await client.pttl("a") == -1
        assert await client.set("a", "1", ex=120)
        assert await client.pttl("a") > 0
        assert await client.set("a", "2", keepttl=True)
        assert await client.pttl("a") > 0
        assert await client.set("a", "3")
        assert await client.pttl("a") == -1

    async def test_set_multipleoptions(self, client, _s):
        await client.set("a", "val")
        assert await client.set("a", "1", condition=PureToken.XX, px=10000)
        assert 0 < await client.ttl("a") <= 10

    async def test_setex(self, client, _s):
        assert await client.setex("a", "1", 60)
        assert await client.get("a") == _s("1")
        assert 0 < await client.ttl("a") <= 60

    @pytest.mark.nodragonfly
    async def test_setnx(self, client, _s):
        assert await client.setnx("a", "1")
        assert await client.get("a") == _s("1")
        assert not await client.setnx("a", "2")
        assert await client.get("a") == _s("1")

    async def test_setrange(self, client, _s):
        assert await client.setrange("a", 5, "foo") == 8
        assert await client.get("a") == _s("\0\0\0\0\0foo")
        await client.set("a", "abcdefghijh")
        assert await client.setrange("a", 6, "12345") == 11
        assert await client.get("a") == _s("abcdef12345")

    async def test_strlen(self, client, _s):
        await client.set("a", "foo")
        assert await client.strlen("a") == 3

    async def test_substr(self, client, _s):
        await client.set("a", "0123456789")
        with server_deprecation_warning("Use :meth:`getrange`", client):
            assert await client.substr("a", 0, -1) == _s("0123456789")
            assert await client.substr("a", 2, -1) == _s("23456789")
            assert await client.substr("a", 3, 5) == _s("345")
            assert await client.substr("a", 3, -2) == _s("345678")

    async def test_binary_get_set(self, client, _s):
        assert await client.set(" foo bar ", "123")
        assert await client.get(" foo bar ") == _s("123")

        assert await client.set(" foo\r\nbar\r\n ", "456")
        assert await client.get(" foo\r\nbar\r\n ") == _s("456")

        assert await client.set(" \r\n\t\x07\x13 ", "789")
        assert await client.get(" \r\n\t\x07\x13 ") == _s("789")

        assert sorted(await client.keys("*")) == [
            _s(" \r\n\t\x07\x13 "),
            _s(" foo\r\nbar\r\n "),
            _s(" foo bar "),
        ]

        assert await client.delete([" foo bar "])
        assert await client.delete([" foo\r\nbar\r\n "])
        assert await client.delete([" \r\n\t\x07\x13 "])
