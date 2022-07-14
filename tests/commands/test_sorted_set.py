from __future__ import annotations

import asyncio

import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError, DataError
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
class TestSortedSet:
    async def test_zadd(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrange("a{foo}", 0, -1) == (_s("a1"), _s("a2"), _s("a3"))

    async def test_zadd_options(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1))
        assert int(await client.zscore("a{foo}", "a1")) == 1
        assert int(await client.zadd("a{foo}", dict(a1=2), condition=PureToken.NX)) == 0
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a1=2), condition=PureToken.NX, change=True
                )
            )
            == 0
        )
        assert int(await client.zscore("a{foo}", "a1")) == 1
        assert await client.zcard("a{foo}") == 1
        assert int(await client.zadd("a{foo}", dict(a2=1), condition=PureToken.XX)) == 0
        assert await client.zcard("a{foo}") == 1
        assert int(await client.zadd("a{foo}", dict(a1=2), condition=PureToken.XX)) == 0
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a1=3), condition=PureToken.XX, change=True
                )
            )
            == 1
        )
        assert int(await client.zscore("a{foo}", "a1")) == 3
        assert int(await client.zadd("a{foo}", dict(a2=1), condition=PureToken.NX)) == 1
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), condition=PureToken.NX, change=True
                )
            )
            == 1
        )
        assert await client.zcard("a{foo}") == 3
        await client.zadd("a{foo}", dict(a3=1), increment=True)
        assert int(await client.zscore("a{foo}", "a3")) == 2

    @pytest.mark.min_server_version("6.2.0")
    async def test_zadd_options_comparison(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1))
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), comparison=PureToken.LT, change=True
                )
            )
            == 1
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=2), comparison=PureToken.LT, change=True
                )
            )
            == 0
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=2), comparison=PureToken.GT, change=True
                )
            )
            == 1
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), comparison=PureToken.GT, change=True
                )
            )
            == 0
        )

    async def test_zcard(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zcard("a{foo}") == 3

    async def test_zcount(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zcount("a{foo}", "-inf", "+inf") == 3
        assert await client.zcount("a{foo}", 1, 2) == 2
        assert await client.zcount("a{foo}", 10, 20) == 0

    @pytest.mark.min_server_version("6.2.0")
    async def test_zdiff(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=1, a2=2))
        assert (await client.zdiff(["a{foo}", "b{foo}"])) == (_s("a3"),)
        assert (await client.zdiff(["a{foo}", "b{foo}"], withscores=True)) == (
            (_s("a3"), 3.0),
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_zdiffstore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=1, a2=2))
        assert await client.zdiffstore(["a{foo}", "b{foo}"], destination=_s("out{foo}"))
        assert (await client.zrange("out{foo}", 0, -1)) == (_s("a3"),)
        assert (await client.zrange("out{foo}", 0, -1, withscores=True)) == (
            (_s("a3"), 3.0),
        )

    async def test_zincrby(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zincrby("a{foo}", member=_s("a2"), increment=1) == 3.0
        assert await client.zincrby("a{foo}", member=_s("a3"), increment=5) == 8.0
        assert await client.zscore("a{foo}", "a2") == 3.0
        assert await client.zscore("a{foo}", "a3") == 8.0

    async def test_zlexcount(self, client, _s):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zlexcount("a{foo}", "-", "+") == 7
        assert await client.zlexcount("a{foo}", "[b", "[f") == 5

    @pytest.mark.min_server_version("6.2.0")
    async def test_zinter_sum(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert await client.zinter(["a{foo}", "b{foo}", "c{foo}"]) == (
            _s("a3"),
            _s("a1"),
        )
        assert await client.zinter(["a{foo}", "b{foo}", "c{foo}"], withscores=True) == (
            (_s("a3"), 8),
            (_s("a1"), 9),
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_zinter_max(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert await client.zinter(
            [
                "a{foo}",
                "b{foo}",
                "c{foo}",
            ],
            aggregate=PureToken.MAX,
        ) == (_s("a3"), _s("a1"))
        assert await client.zinter(
            ["a{foo}", "b{foo}", "c{foo}"], aggregate=PureToken.MAX, withscores=True
        ) == ((_s("a3"), 5), (_s("a1"), 6))

    async def test_zinterstore_sum(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                ["a{foo}", "b{foo}", "c{foo}"], destination="d{foo}"
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a3"), 8),
            (_s("a1"), 9),
        )

    async def test_zinterstore_max(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                ["a{foo}", "b{foo}", "c{foo}"],
                destination="d{foo}",
                aggregate=PureToken.MAX,
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a3"), 5),
            (_s("a1"), 6),
        )

    async def test_zinterstore_min(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=2, a2=3, a3=5))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                ["a{foo}", "b{foo}", "c{foo}"],
                destination="d{foo}",
                aggregate=PureToken.MIN,
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a1"), 1),
            (_s("a3"), 3),
        )

    async def test_zinterstore_with_weight(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                ["a{foo}", "b{foo}", "c{foo}"], destination="d{foo}", weights=[1, 2, 3]
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a3"), 20),
            (_s("a1"), 23),
        )

    @pytest.mark.min_server_version("7.0.0")
    async def test_zintercard(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a3=2, a4=2, a5=2))
        assert await client.zintercard(["a{foo}", "c{foo}"]) == 0
        assert await client.zintercard(["a{foo}"]) == 3
        assert await client.zintercard(["a{foo}", "b{foo}"]) == 1
        assert await client.zintercard(["a{foo}"], limit=1) == 1

    @pytest.mark.nodragonfly
    async def test_zpopmax(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert (await client.zpopmax("a{foo}")) == (_s("a3"), 3)
        # with count
        assert (await client.zpopmax("a{foo}", count=2)) == (
            (_s("a2"), 2),
            (_s("a1"), 1),
        )

    @pytest.mark.nodragonfly
    async def test_zpopmin(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert (await client.zpopmin("a{foo}")) == (_s("a1"), 1)
        # with count
        assert (await client.zpopmin("a{foo}", count=2)) == (
            (_s("a2"), 2),
            (_s("a3"), 3),
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_zrandmember(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert (await client.zrandmember("a{foo}")) is not None
        assert len(await client.zrandmember("a{foo}", 2)) == 2
        # with scores
        assert len(await client.zrandmember("a{foo}", 2, True)) == 2
        # without duplications
        assert len(await client.zrandmember("a{foo}", 10)) == 5
        # with duplications
        assert len(await client.zrandmember("a{foo}", -10)) == 10

    @pytest.mark.nodragonfly
    async def test_bzpopmax(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2))
        await client.zadd("b{foo}", dict(b1=10, b2=20))
        assert (await client.bzpopmax(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("b{foo}"),
            _s("b2"),
            20,
        )
        assert (await client.bzpopmax(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("b{foo}"),
            _s("b1"),
            10,
        )
        assert (await client.bzpopmax(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("a{foo}"),
            _s("a2"),
            2,
        )
        assert (await client.bzpopmax(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("a{foo}"),
            _s("a1"),
            1,
        )
        assert (await client.bzpopmax(["b{foo}", "a{foo}"], timeout=1)) is None
        await client.zadd("c{foo}", dict(c1=100))
        assert (await client.bzpopmax(["c{foo}"], timeout=1)) == (
            _s("c{foo}"),
            _s("c1"),
            100,
        )

    @pytest.mark.nodragonfly
    async def test_bzpopmin(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2))
        await client.zadd("b{foo}", dict(b1=10, b2=20))
        assert (await client.bzpopmin(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("b{foo}"),
            _s("b1"),
            10,
        )
        assert (await client.bzpopmin(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("b{foo}"),
            _s("b2"),
            20,
        )
        assert (await client.bzpopmin(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("a{foo}"),
            _s("a1"),
            1,
        )
        assert (await client.bzpopmin(["b{foo}", "a{foo}"], timeout=1)) == (
            _s("a{foo}"),
            _s("a2"),
            2,
        )
        assert (await client.bzpopmin(["b{foo}", "a{foo}"], timeout=1)) is None
        await client.zadd("c{foo}", dict(c1=100))
        assert (await client.bzpopmin(["c{foo}"], timeout=1)) == (
            _s("c{foo}"),
            _s("c1"),
            100,
        )

    async def test_zrange_basic(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrange("a{foo}", 0, 1) == (_s("a1"), _s("a2"))
        assert await client.zrange("a{foo}", 1, 2) == (_s("a2"), _s("a3"))

        # withscores
        assert await client.zrange("a{foo}", 0, 1, withscores=True) == (
            (_s("a1"), 1.0),
            (_s("a2"), 2.0),
        )
        assert await client.zrange("a{foo}", 1, 2, withscores=True) == (
            (_s("a2"), 2.0),
            (_s("a3"), 3.0),
        )

        # custom score function
        assert await client.zrange("a{foo}", 0, 1, withscores=True,) == (
            (_s("a1"), 1),
            (_s("a2"), 2),
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_zrange_options(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrange("a{foo}", 0, 1) == (_s("a1"), _s("a2"))
        assert await client.zrange("a{foo}", 1, 2) == (_s("a2"), _s("a3"))

        # withscores
        assert await client.zrange("a{foo}", 0, 1, withscores=True) == (
            (_s("a1"), 1.0),
            (_s("a2"), 2.0),
        )
        assert await client.zrange("a{foo}", 1, 2, withscores=True) == (
            (_s("a2"), 2.0),
            (_s("a3"), 3.0),
        )

        # custom score function
        assert await client.zrange("a{foo}", 0, 1, withscores=True,) == (
            (_s("a1"), 1),
            (_s("a2"), 2),
        )
        # with offset
        assert await client.zrange(
            "a{foo}", 1, 2, sortby=PureToken.BYSCORE, offset=0, count=1
        ) == (_s("a1"),)
        assert await client.zrange(
            "a{foo}", 1, 2, sortby=PureToken.BYSCORE, offset=1, count=1
        ) == (_s("a2"),)
        with pytest.raises(CommandSyntaxError):
            await client.zrange("a{foo}", 0, 1, offset=1)

    @pytest.mark.min_server_version("6.2.0")
    async def test_zrangestore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrangestore("b{foo}", "a{foo}", 0, 1)
        assert await client.zrange("b{foo}", 0, -1) == (_s("a1"), _s("a2"))
        assert await client.zrangestore("b{foo}", "a{foo}", 1, 2)
        assert await client.zrange("b{foo}", 0, -1) == (_s("a2"), _s("a3"))
        assert await client.zrange("b{foo}", 0, -1, withscores=True) == (
            (_s("a2"), 2),
            (_s("a3"), 3),
        )
        # reversed order
        assert await client.zrangestore("b{foo}", "a{foo}", 1, 2, rev=True)
        assert await client.zrange("b{foo}", 0, -1) == (_s("a1"), _s("a2"))
        # by score
        assert await client.zrangestore(
            "b{foo}",
            "a{foo}",
            2,
            1,
            sortby=PureToken.BYSCORE,
            offset=0,
            count=1,
            rev=True,
        )
        assert await client.zrange("b{foo}", 0, -1) == (_s("a2"),)
        # by lex
        assert await client.zrangestore(
            "b{foo}", "a{foo}", "[a2", "(a3", sortby=PureToken.BYLEX, offset=0, count=1
        )
        assert await client.zrange("b{foo}", 0, -1) == (_s("a2"),)

    @pytest.mark.nodragonfly
    async def test_zrangebylex(self, client, _s):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        with server_deprecation_warning("Use :meth:`zrange`", client, "6.2"):
            assert await client.zrangebylex("a{foo}", "-", "[c") == (
                _s("a"),
                _s("b"),
                _s("c"),
            )
            assert await client.zrangebylex("a{foo}", "-", "(c") == (_s("a"), _s("b"))
            assert await client.zrangebylex("a{foo}", "[aaa", "(g") == (
                _s("b"),
                _s("c"),
                _s("d"),
                _s("e"),
                _s("f"),
            )
            assert await client.zrangebylex("a{foo}", "[f", "+") == (_s("f"), _s("g"))
            assert await client.zrangebylex("a{foo}", "-", "+", offset=3, count=2) == (
                _s("d"),
                _s("e"),
            )

    @pytest.mark.nodragonfly
    async def test_zrevrangebylex(self, client, _s):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        with server_deprecation_warning("Use :meth:`zrange`", client, "6.2"):
            assert await client.zrevrangebylex("a{foo}", "[c", "-") == (
                _s("c"),
                _s("b"),
                _s("a"),
            )
            assert await client.zrevrangebylex("a{foo}", "(c", "-") == (
                _s("b"),
                _s("a"),
            )
            assert await client.zrevrangebylex("a{foo}", "(g", "[aaa") == (
                _s("f"),
                _s("e"),
                _s("d"),
                _s("c"),
                _s("b"),
            )
            assert await client.zrevrangebylex("a{foo}", "+", "[f") == (
                _s("g"),
                _s("f"),
            )
            assert await client.zrevrangebylex(
                "a{foo}", "+", "-", offset=3, count=2
            ) == (
                _s("d"),
                _s("c"),
            )

    async def test_zrangebyscore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        with server_deprecation_warning("Use :meth:`zrange`", client, "6.2"):
            assert await client.zrangebyscore("a{foo}", 2, 4) == (
                _s("a2"),
                _s("a3"),
                _s("a4"),
            )

            # slicing with start/num
            assert await client.zrangebyscore("a{foo}", 2, 4, offset=1, count=2) == (
                _s("a3"),
                _s("a4"),
            )

            # withscores
            assert await client.zrangebyscore("a{foo}", 2, 4, withscores=True) == (
                (_s("a2"), 2.0),
                (_s("a3"), 3.0),
                (_s("a4"), 4.0),
            )

            # custom score function
            assert await client.zrangebyscore(
                "a{foo}",
                2,
                4,
                withscores=True,
            ) == ((_s("a2"), 2), (_s("a3"), 3), (_s("a4"), 4))

    async def test_zrank(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrank("a{foo}", "a1") == 0
        assert await client.zrank("a{foo}", "a2") == 1
        assert await client.zrank("a{foo}", "a6") is None

    async def test_zrem(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrem("a{foo}", ["a2"]) == 1
        assert await client.zrange("a{foo}", 0, -1) == (_s("a1"), _s("a3"))
        assert await client.zrem("a{foo}", ["junk"]) == 0
        assert await client.zrange("a{foo}", 0, -1) == (_s("a1"), _s("a3"))

    async def test_zrem_multiple_keys(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrem("a{foo}", ["a1", "a2"]) == 2
        assert await client.zrange("a{foo}", 0, 5) == (_s("a3"),)

    async def test_zremrangebylex(self, client, _s):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zremrangebylex("a{foo}", "-", "[c") == 3
        assert await client.zrange("a{foo}", 0, -1) == (
            _s("d"),
            _s("e"),
            _s("f"),
            _s("g"),
        )
        assert await client.zremrangebylex("a{foo}", "[f", "+") == 2
        assert await client.zrange("a{foo}", 0, -1) == (_s("d"), _s("e"))
        assert await client.zremrangebylex("a{foo}", "[h", "+") == 0
        assert await client.zrange("a{foo}", 0, -1) == (_s("d"), _s("e"))

    async def test_zremrangebyrank(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zremrangebyrank("a{foo}", 1, 3) == 3
        assert await client.zrange("a{foo}", 0, 5) == (_s("a1"), _s("a5"))

    async def test_zremrangebyscore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zremrangebyscore("a{foo}", 2, 4) == 3
        assert await client.zrange("a{foo}", 0, -1) == (_s("a1"), _s("a5"))
        assert await client.zremrangebyscore("a{foo}", 2, 4) == 0
        assert await client.zrange("a{foo}", 0, -1) == (_s("a1"), _s("a5"))

    async def test_zrevrange(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        with server_deprecation_warning("Use :meth:`zrange`", client, "6.2"):
            assert await client.zrevrange("a{foo}", 0, 1) == (_s("a3"), _s("a2"))
            assert await client.zrevrange("a{foo}", 1, 2) == (_s("a2"), _s("a1"))

            # withscores
            assert await client.zrevrange("a{foo}", 0, 1, withscores=True) == (
                (_s("a3"), 3.0),
                (_s("a2"), 2.0),
            )
            assert await client.zrevrange("a{foo}", 1, 2, withscores=True) == (
                (_s("a2"), 2.0),
                (_s("a1"), 1.0),
            )

            # custom score function
            assert await client.zrevrange("a{foo}", 0, 1, withscores=True,) == (
                (_s("a3"), 3.0),
                (_s("a2"), 2.0),
            )

    async def test_zrevrangebyscore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        with server_deprecation_warning("Use :meth:`zrange`", client, "6.2"):
            assert await client.zrevrangebyscore("a{foo}", 4, 2) == (
                _s("a4"),
                _s("a3"),
                _s("a2"),
            )

            # slicing with start/num
            assert await client.zrevrangebyscore("a{foo}", 4, 2, offset=1, count=2) == (
                _s("a3"),
                _s("a2"),
            )

            # withscores
            assert await client.zrevrangebyscore("a{foo}", 4, 2, withscores=True) == (
                (_s("a4"), 4.0),
                (_s("a3"), 3.0),
                (_s("a2"), 2.0),
            )

            # custom score function
            assert await client.zrevrangebyscore(
                "a{foo}",
                4,
                2,
                withscores=True,
            ) == ((_s("a4"), 4), (_s("a3"), 3), (_s("a2"), 2))

    async def test_zrevrank(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrevrank("a{foo}", "a1") == 4
        assert await client.zrevrank("a{foo}", "a2") == 3
        assert await client.zrevrank("a{foo}", "a6") is None

    async def test_zscore(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zscore("a{foo}", "a1") == 1.0
        assert await client.zscore("a{foo}", "a2") == 2.0
        assert await client.zscore("a{foo}", "a4") is None

    @pytest.mark.min_server_version("6.2.0")
    async def test_zunion_sum(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert await client.zunion(["a{foo}", "b{foo}", "c{foo}"]) == (
            _s("a2"),
            _s("a4"),
            _s("a3"),
            _s("a1"),
        )
        assert await client.zunion(["a{foo}", "b{foo}", "c{foo}"], withscores=True) == (
            (_s("a2"), 3.0),
            (_s("a4"), 4.0),
            (_s("a3"), 8.0),
            (_s("a1"), 9.0),
        )

    async def test_zunionstore_sum(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                ["a{foo}", "b{foo}", "c{foo}"], destination="d{foo}"
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a2"), 3),
            (_s("a4"), 4),
            (_s("a3"), 8),
            (_s("a1"), 9),
        )

    async def test_zunionstore_max(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                ["a{foo}", "b{foo}", "c{foo}"],
                destination="d{foo}",
                aggregate=PureToken.MAX,
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a2"), 2),
            (_s("a4"), 4),
            (_s("a3"), 5),
            (_s("a1"), 6),
        )

    async def test_zunionstore_min(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=4))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                ["a{foo}", "b{foo}", "c{foo}"],
                destination="d{foo}",
                aggregate=PureToken.MIN,
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a1"), 1),
            (_s("a2"), 2),
            (_s("a3"), 3),
            (_s("a4"), 4),
        )

    async def test_zunionstore_with_weight(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                ["a{foo}", "b{foo}", "c{foo}"], destination="d{foo}", weights=[1, 2, 3]
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == (
            (_s("a2"), 5),
            (_s("a4"), 12),
            (_s("a3"), 20),
            (_s("a1"), 23),
        )

    @pytest.mark.min_server_version("7.0.0")
    async def test_zmpop(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=4, a2=5, a3=6))
        result = await client.zmpop(["a{foo}", "b{foo}"], PureToken.MIN)
        assert result[0] == _s("a{foo}")
        assert result[1] == ((_s("a1"), 1.0),)
        result = await client.zmpop(["a{foo}", "b{foo}"], PureToken.MAX, count=2)
        assert result[0] == _s("a{foo}")
        assert result[1] == ((_s("a3"), 3.0), (_s("a2"), 2.0))
        result = await client.zmpop(["a{foo}", "b{foo}"], PureToken.MAX)
        assert result[0] == _s("b{foo}")
        assert result[1] == ((_s("a3"), 6.0),)

    @pytest.mark.min_server_version("7.0.0")
    @pytest.mark.nocluster
    async def test_bzmpop(self, client, _s):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=4, a2=5, a3=6))
        result = await client.bzmpop(["a{foo}", "b{foo}"], 1, PureToken.MIN)
        assert result[0] == _s("a{foo}")
        assert result[1] == ((_s("a1"), 1.0),)
        result = await client.bzmpop(["a{foo}", "b{foo}"], 1, PureToken.MAX, count=2)
        assert result[0] == _s("a{foo}")
        assert result[1] == ((_s("a3"), 3.0), (_s("a2"), 2.0))
        result = await client.bzmpop(["a{foo}", "b{foo}"], 1, PureToken.MAX)
        assert result[0] == _s("b{foo}")
        assert result[1] == ((_s("a3"), 6.0),)
        assert await client.bzmpop(["a{foo}"], 1, PureToken.MAX) is None

        async def _delayadd():
            await asyncio.sleep(0.1)
            return await client.zadd("a{foo}", dict(a1=42))

        result = await asyncio.gather(
            client.bzmpop(["a{foo}"], 1, PureToken.MIN), _delayadd()
        )
        assert result[0][1] == ((_s("a1"), 42.0),)

    @pytest.mark.min_server_version("6.1.240")
    async def test_zmscore(self, client, _s):
        with pytest.raises(DataError):
            await client.zmscore("invalid_key", [])

        assert await client.zmscore("invalid_key", ["invalid_member"]) == (None,)

        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3.5))
        assert (await client.zmscore("a{foo}", ["a1", "a2", "a3", "a4"])) == (
            1.0,
            2.0,
            3.5,
            None,
        )

    @pytest.mark.nodragonfly
    async def test_zscan(self, client, _s):
        await client.zadd("a", dict(a=1, b=2, c=3))
        cursor, pairs = await client.zscan("a", count=10)
        assert cursor == 0
        assert set(pairs) == {(_s("a"), 1), (_s("b"), 2), (_s("c"), 3)}
        _, pairs = await client.zscan("a", match="a")
        assert set(pairs) == {(_s("a"), 1)}

    @pytest.mark.nodragonfly
    async def test_zscan_iter(self, client, _s):
        await client.zadd("a", dict(a=1, b=2, c=3))
        pairs = set()
        async for pair in client.zscan_iter("a"):
            pairs.add(pair)
        assert pairs == {(_s("a"), 1), (_s("b"), 2), (_s("c"), 3)}
        async for pair in client.zscan_iter("a", match="a"):
            assert pair == (_s("a"), 1)
