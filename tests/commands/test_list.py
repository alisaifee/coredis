from __future__ import annotations

import anyio
import pytest

from coredis import PureToken
from coredis._concurrency import gather
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
)
class TestList:
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

    async def test_lpop_count(self, client, _s):
        await client.rpush("a", ["1", "2", "3"])
        assert await client.lpop("a", 3) == [_s("1"), _s("2"), _s("3")]

    async def test_lpush(self, client, _s):
        assert await client.lpush("a", ["1"]) == 1
        assert await client.lpush("a", ["2"]) == 2
        assert await client.lpush("a", ["3", "4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("4"), _s("3"), _s("2"), _s("1")]

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

    async def test_rpushx(self, client, _s):
        assert await client.rpushx("a", ["b"]) == 0
        assert await client.lrange("a", 0, -1) == []
        await client.rpush("a", ["1", "2", "3"])
        assert await client.rpushx("a", ["4"]) == 4
        assert await client.lrange("a", 0, -1) == [_s("1"), _s("2"), _s("3"), _s("4")]

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

    async def test_lmove(self, client, _s):
        await client.rpush("a{foo}", ["one", "two", "three", "four"])
        assert _s("one") == await client.lmove("a{foo}", "b{foo}", PureToken.LEFT, PureToken.RIGHT)
        assert _s("four") == await client.lmove("a{foo}", "b{foo}", PureToken.RIGHT, PureToken.LEFT)
        assert await client.lmove("x{foo}", "b{foo}", PureToken.RIGHT, PureToken.LEFT) is None
        assert _s("three") == await client.lmove(
            "a{foo}", "x{foo}", PureToken.RIGHT, PureToken.LEFT
        )
        assert 1 == await client.llen("x{foo}")

    @pytest.mark.nocluster
    @pytest.mark.nodragonfly
    async def test_blmpop(self, client, cloner, _s):
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
            await anyio.sleep(0.1)
            clone = await cloner(client)
            async with clone:
                return await clone.rpush("a{foo}", ["42"])

        result = await gather(client.blmpop(["a{foo}"], 1, PureToken.LEFT), _delayadd())
        assert result[0][1] == [_s("42")]

    async def test_blmove(self, client, _s):
        await client.rpush("a{foo}", ["one", "two", "three", "four"])
        assert await client.blmove("a{foo}", "b{foo}", PureToken.LEFT, PureToken.RIGHT, timeout=5)
        assert await client.blmove("a{foo}", "b{foo}", PureToken.RIGHT, PureToken.LEFT, timeout=1)

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
        assert sorted(await client.keys("*")) == [_s(k) for k in sorted(iter(mapping.keys()))]

        # check that it is possible to get list content by key name

        for key, value in mapping.items():
            assert await client.lrange(key, 0, -1) == [_s(v) for v in value]


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
    "redis_cached",
    "redis_cluster_cached",
)
@pytest.mark.min_server_version("8.10.0")
class TestListMove:
    async def test_lmovem(self, client, _s):
        await client.rpush("a{foo}", ["1", "2", "3", "4", "5"])
        # Without a count a single element is moved, still as a list reply.
        assert await client.lmovem("a{foo}", "b{foo}", PureToken.LEFT, PureToken.LEFT) == [_s("1")]
        # OBO moves the elements one by one, so pushing left reverses them.
        assert await client.lmovem(
            "a{foo}",
            "b{foo}",
            PureToken.LEFT,
            PureToken.LEFT,
            count=3,
            ordering=PureToken.OBO,
        ) == [_s("4"), _s("3"), _s("2")]
        assert await client.lrange("b{foo}", 0, -1) == [_s("4"), _s("3"), _s("2"), _s("1")]
        # COUNT is an upper bound, and BULK preserves the relative order.
        assert await client.lmovem(
            "a{foo}",
            "b{foo}",
            PureToken.LEFT,
            PureToken.LEFT,
            count=5,
            ordering=PureToken.BULK,
        ) == [_s("5")]
        # The source is deleted once it is drained ...
        assert not await client.exists(["a{foo}"])
        # ... and moving from an empty source moves nothing.
        assert (
            await client.lmovem(
                "a{foo}",
                "b{foo}",
                PureToken.LEFT,
                PureToken.LEFT,
                count=2,
                ordering=PureToken.BULK,
            )
            is None
        )

    async def test_lmovem_exactly(self, client, _s):
        await client.rpush("names{foo}", ["john"])
        # EXACTLY is all-or-nothing: too few elements moves none of them.
        assert (
            await client.lmovem(
                "names{foo}",
                "processed{foo}",
                PureToken.LEFT,
                PureToken.RIGHT,
                exactly=2,
                ordering=PureToken.BULK,
            )
            is None
        )
        assert await client.lrange("names{foo}", 0, -1) == [_s("john")]
        await client.rpush("names{foo}", ["doe"])
        assert await client.lmovem(
            "names{foo}",
            "processed{foo}",
            PureToken.LEFT,
            PureToken.RIGHT,
            exactly=2,
            ordering=PureToken.BULK,
        ) == [_s("john"), _s("doe")]
        assert await client.lrange("processed{foo}", 0, -1) == [_s("john"), _s("doe")]

    async def test_lmovem_invalid_arguments(self, client, _s):
        # ordering is mandatory whenever count/exactly is given, and vice versa.
        with pytest.raises(CommandSyntaxError):
            await client.lmovem("a{foo}", "b{foo}", PureToken.LEFT, PureToken.LEFT, count=2)
        with pytest.raises(CommandSyntaxError):
            await client.lmovem("a{foo}", "b{foo}", PureToken.LEFT, PureToken.LEFT, exactly=2)
        with pytest.raises(CommandSyntaxError):
            await client.lmovem(
                "a{foo}", "b{foo}", PureToken.LEFT, PureToken.LEFT, ordering=PureToken.BULK
            )
        # count and exactly are two spellings of the same slot.
        with pytest.raises(CommandSyntaxError):
            await client.lmovem(
                "a{foo}",
                "b{foo}",
                PureToken.LEFT,
                PureToken.LEFT,
                count=2,
                exactly=2,
                ordering=PureToken.BULK,
            )

    async def test_blmovem(self, client, _s):
        await client.rpush("a{foo}", ["1", "2", "3", "4", "5"])
        assert await client.blmovem(
            "a{foo}", "b{foo}", PureToken.LEFT, PureToken.LEFT, timeout=1
        ) == [_s("1")]
        assert await client.blmovem(
            "a{foo}",
            "b{foo}",
            PureToken.LEFT,
            PureToken.RIGHT,
            timeout=1,
            count=3,
            ordering=PureToken.BULK,
        ) == [_s("2"), _s("3"), _s("4")]
        # Up to count, with fewer available.
        assert await client.blmovem(
            "a{foo}",
            "b{foo}",
            PureToken.LEFT,
            PureToken.RIGHT,
            timeout=1,
            count=5,
            ordering=PureToken.BULK,
        ) == [_s("5")]

    async def test_blmovem_timeout(self, client, _s):
        # Nothing to move: block for the timeout and then report nothing moved.
        assert (
            await client.blmovem(
                "x{foo}",
                "y{foo}",
                PureToken.LEFT,
                PureToken.RIGHT,
                timeout=1,
                count=2,
                ordering=PureToken.BULK,
            )
            is None
        )
