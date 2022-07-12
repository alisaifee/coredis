from __future__ import annotations

import asyncio

import pytest

from coredis import PureToken
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
    "dragonfly",
)
@pytest.mark.asyncio()
class TestList:
    @pytest.mark.nodragonfly
    async def test_large_list(self, client, _s):
        ints = [int(i) for i in range(10000)]
        assert await client.lpush("a{foo}", ints)
        assert len(set(await client.lrange("a{foo}", 0, -1))) == 10000

    async def test_blpop(self, client, _s):
        await client.rpush("a{foo}", ["1", "2"])
        await client.rpush("b{foo}", ["3", "4"])
        assert await client.blpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("b{foo}"),
            _s("3"),
        ]
        assert await client.blpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("b{foo}"),
            _s("4"),
        ]
        assert await client.blpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("a{foo}"),
            _s("1"),
        ]
        assert await client.blpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("a{foo}"),
            _s("2"),
        ]
        assert await client.blpop(["b{foo}", "a{foo}"], timeout=1) is None
        await client.rpush("c{foo}", ["1"])
        assert await client.blpop(["c{foo}"], timeout=1) == [_s("c{foo}"), _s("1")]

    @pytest.mark.min_server_version("7.0.0")
    async def test_lmpop(self, client, _s):
        await client.rpush("a{foo}", [1, 2, 3])
        await client.rpush("b{foo}", [4, 5, 6])
        result = await client.lmpop(["a{foo}", "b{foo}"], PureToken.LEFT)
        assert result[0] == _s("a{foo}")
        assert result[1] == [_s("1")]
        result = await client.lmpop(["a{foo}", "b{foo}"], PureToken.LEFT, count=2)
        assert result[0] == _s("a{foo}")
        assert result[1] == [_s("2"), _s("3")]
        result = await client.lmpop(["a{foo}", "b{foo}"], PureToken.RIGHT)
        assert result[0] == _s("b{foo}")
        assert result[1] == [_s("6")]

    async def test_brpop(self, client, _s):
        await client.rpush("a{foo}", ["1", "2"])
        await client.rpush("b{foo}", ["3", "4"])
        assert await client.brpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("b{foo}"),
            _s("4"),
        ]
        assert await client.brpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("b{foo}"),
            _s("3"),
        ]
        assert await client.brpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("a{foo}"),
            _s("2"),
        ]
        assert await client.brpop(["b{foo}", "a{foo}"], timeout=1) == [
            _s("a{foo}"),
            _s("1"),
        ]
        assert await client.brpop(["b{foo}", "a{foo}"], timeout=1) is None
        await client.rpush("c{foo}", ["1"])
        assert await client.brpop(["c{foo}"], timeout=1) == [_s("c{foo}"), _s("1")]

    @pytest.mark.nodragonfly
    async def test_brpoplpush(self, client, _s):
        await client.rpush("a{foo}", ["1", "2"])
        await client.rpush("b{foo}", ["3", "4"])
        with server_deprecation_warning("Use :meth:`blmove`", client, "6.2"):
            assert await client.brpoplpush("a{foo}", "b{foo}", timeout=1) == _s("2")
            assert await client.brpoplpush("a{foo}", "b{foo}", timeout=1) == _s("1")
            assert await client.brpoplpush("a{foo}", "b{foo}", timeout=1) is None
        assert await client.lrange("a{foo}", 0, -1) == []
        assert await client.lrange("b{foo}", 0, -1) == [
            _s("1"),
            _s("2"),
            _s("3"),
            _s("4"),
        ]

    @pytest.mark.nodragonfly
    async def test_brpoplpush_empty_string(self, client, _s):
        await client.rpush("a{foo}", [""])
        with server_deprecation_warning("Use :meth:`blmove`", client, "6.2"):
            assert await client.brpoplpush("a{foo}", "b{foo}", timeout=1) == _s("")

    async def test_lindex(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lindex("a", 0) == _s("1")
        assert await client.lindex("a", 1) == _s("2")
        assert await client.lindex("a", 2) == _s("3")
        assert await client.lindex("a", 10) is None

    async def test_linsert(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.linsert("a", PureToken.AFTER, "2", "2.5") == 4
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2"), _s("2.5"), _s("3")]
        assert await client.linsert("a", PureToken.BEFORE, "2", "1.5") == 5
        assert await client.lrange("a", 0, -1) == [
            _s("1"),
            _s("1.5"),
            _s("2"),
            _s("2.5"),
            _s("3"),
        ]

    async def test_llen(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.llen("a") == 3

    async def test_lpop(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lpop("a") == _s("1")
        assert await client.lpop("a") == _s("2")
        assert await client.lpop("a") == _s("3")
        assert await client.lpop("a") is None

    @pytest.mark.min_server_version("6.2.0")
    async def test_lpop_count(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lpop("a", 3) == [_s("1"), _s("2"), _s("3")]

    async def test_lpush(self, client, _s):
        assert await client.lpush("a", ["1"]) == 1
        assert await client.lpush("a", ["2"]) == 2
        assert await client.lpush("a", ["3", "4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("4"), _s("3"), _s("2"), _s("1")]

    @pytest.mark.nodragonfly
    async def test_lpushx(self, client, _s):
        assert await client.lpushx("a", ["1"]) == 0
        assert await client.lrange("a", 0, -1) == []
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lpushx("a", ["4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("4"), _s("1"), _s("2"), _s("3")]

    async def test_lrange(self, client, _s):
        await client.rpush("a", ["1", "2", "3", "4", "5"])
        assert await client.lrange("a", 0, 2) == [_s("1"), _s("2"), _s("3")]
        assert await client.lrange("a", 2, 10) == [_s("3"), _s("4"), _s("5")]
        assert await client.lrange("a", 0, -1) == [
            _s("1"),
            _s("2"),
            _s("3"),
            _s("4"),
            _s("5"),
        ]

    async def test_lrem(self, client, _s):
        await client.rpush("a", ["1", "1", "1", "1"])
        assert await client.lrem("a", 1, "1") == 1
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("1"), _s("1")]
        assert await client.lrem("a", 3, "1") == 3
        assert await client.lrange("a", 0, -1) == []

    async def test_lset(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2"), _s("3")]
        assert await client.lset("a", 1, "4")
        assert await client.lrange("a", 0, 2) == [_s("1"), _s("4"), _s("3")]

    async def test_ltrim(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.ltrim("a", 0, 1)
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2")]

    async def test_rpop(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.rpop("a") == _s("3")
        assert await client.rpop("a") == _s("2")
        assert await client.rpop("a") == _s("1")
        assert await client.rpop("a") is None

    @pytest.mark.min_server_version("6.2.0")
    async def test_rpop_count(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.rpop("a", 3) == [_s("3"), _s("2"), _s("1")]

    async def test_rpoplpush(self, client, _s):
        await client.rpush("a{foo}", ["a1", "a2", "a3"])
        await client.rpush("b{foo}", ["b1", "b2", "b3"])
        with server_deprecation_warning("Use :meth:`lmove`", client, "6.2"):
            assert await client.rpoplpush("a{foo}", "b{foo}") == _s("a3")
        assert await client.lrange("a{foo}", 0, -1) == [_s("a1"), _s("a2")]
        assert await client.lrange("b{foo}", 0, -1) == [
            _s("a3"),
            _s("b1"),
            _s("b2"),
            _s("b3"),
        ]

    async def test_rpush(self, client, _s):
        assert await client.rpush("a", ["1"]) == 1
        assert await client.rpush("a", ["2"]) == 2
        assert await client.rpush("a", ["3", "4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2"), _s("3"), _s("4")]

    @pytest.mark.nodragonfly
    async def test_rpushx(self, client, _s):
        assert await client.rpushx("a", ["b"]) == 0
        assert await client.lrange("a", 0, -1) == []
        await client.rpush("a", ["1", "2", "3"])
        assert await client.rpushx("a", ["4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2"), _s("3"), _s("4")]

    @pytest.mark.min_server_version("6.0.6")
    async def test_lpos(self, client, _s):
        assert await client.rpush("a", ["a", "b", "c", "1", "2", "3", "c", "c"]) == 8
        assert await client.lpos("a", "a") == 0
        assert await client.lpos("a", "c") == 2

        assert await client.lpos("a", "c", rank=1) == 2
        assert await client.lpos("a", "c", rank=2) == 6
        assert await client.lpos("a", "c", rank=4) is None
        assert await client.lpos("a", "c", rank=-1) == 7
        assert await client.lpos("a", "c", rank=-2) == 6

        assert await client.lpos("a", "c", count=0) == [2, 6, 7]
        assert await client.lpos("a", "c", count=1) == [2]
        assert await client.lpos("a", "c", count=2) == [2, 6]
        assert await client.lpos("a", "c", count=100) == [2, 6, 7]

        assert await client.lpos("a", "c", count=0, rank=2) == [6, 7]
        assert await client.lpos("a", "c", count=2, rank=-1) == [7, 6]

        assert await client.lpos("axxx", "c", count=0, rank=2) == []
        assert await client.lpos("axxx", "c") is None

        assert await client.lpos("a", "x", count=2) == []
        assert await client.lpos("a", "x") is None

        assert await client.lpos("a", "a", count=0, maxlen=1) == [0]
        assert await client.lpos("a", "c", count=0, maxlen=1) == []
        assert await client.lpos("a", "c", count=0, maxlen=3) == [2]
        assert await client.lpos("a", "c", count=0, maxlen=3, rank=-1) == [7, 6]
        assert await client.lpos("a", "c", count=0, maxlen=7, rank=2) == [6]

    @pytest.mark.min_server_version("6.2.0")
    async def test_lmove(self, client, _s):
        await client.rpush("a{foo}", ["one", "two", "three", "four"])
        assert await client.lmove("a{foo}", "b{foo}", PureToken.LEFT, PureToken.RIGHT)
        assert await client.lmove("a{foo}", "b{foo}", PureToken.RIGHT, PureToken.LEFT)

    @pytest.mark.min_server_version("7.0.0")
    @pytest.mark.nocluster
    async def test_blmpop(self, client, _s):
        await client.rpush("a{foo}", [1, 2, 3])
        await client.rpush("b{foo}", [4, 5, 6])
        result = await client.blmpop(["a{foo}", "b{foo}"], 1, PureToken.LEFT)
        assert result[0] == _s("a{foo}")
        assert result[1] == [_s("1")]
        result = await client.blmpop(["a{foo}", "b{foo}"], 1, PureToken.LEFT, count=2)
        assert result[0] == _s("a{foo}")
        assert result[1] == [_s("2"), _s("3")]
        result = await client.blmpop(["a{foo}", "b{foo}"], 1, PureToken.RIGHT)
        assert result[0] == _s("b{foo}")
        assert result[1] == [_s("6")]

        async def _delayadd():
            await asyncio.sleep(0.1)
            return await client.rpush("a{foo}", ["42"])

        result = await asyncio.gather(
            client.blmpop(["a{foo}"], 1, PureToken.LEFT), _delayadd()
        )
        assert result[0][1] == [_s("42")]

    @pytest.mark.min_server_version("6.2.0")
    async def test_blmove(self, client, _s):
        await client.rpush("a{foo}", ["one", "two", "three", "four"])
        assert await client.blmove(
            "a{foo}", "b{foo}", PureToken.LEFT, PureToken.RIGHT, timeout=5
        )
        assert await client.blmove(
            "a{foo}", "b{foo}", PureToken.RIGHT, PureToken.LEFT, timeout=1
        )

    async def test_binary_lists(self, client, _s):
        mapping = {
            "foo bar": ["1", "2", "3"],
            "foo\r\nbar\r\n": ["4", "5", "6"],
            "foo\tbar\x07": ["7", "8", "9"],
        }
        # fill in lists

        for key, value in mapping.items():
            await client.rpush(key, value)

        # check that KEYS returns all the keys as they are
        assert sorted(await client.keys("*")) == [
            _s(k) for k in sorted(list(iter(mapping.keys())))
        ]

        # check that it is possible to get list content by key name

        for key, value in mapping.items():
            assert await client.lrange(key, 0, -1) == [_s(v) for v in value]
