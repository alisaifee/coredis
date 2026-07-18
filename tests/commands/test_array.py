from __future__ import annotations

import pytest

from coredis.commands.core import Predicate
from coredis.exceptions import DataError
from coredis.tokens import PureToken
from tests.conftest import targets


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
@pytest.mark.min_server_version("8.8.0")
class TestArray:
    async def test_arcount(self, client, _s):
        assert await client.arcount("arr") == 0
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arcount("arr") == 3

    async def test_arset(self, client, _s):
        assert await client.arcount("arr") == 0
        assert await client.arset("arr", 0, ["a", "b", "c"]) == 3
        assert await client.arcount("arr") == 3

    async def test_ardel(self, client, _s):
        assert await client.arcount("arr") == 0
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arcount("arr") == 3
        assert await client.ardel("arr", [0, 1, 2]) == 3
        assert await client.arcount("arr") == 0

    async def test_ardelrange(self, client, _s):
        assert await client.arcount("arr") == 0
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arcount("arr") == 3
        assert await client.ardelrange("arr", [(0, 2)]) == 3
        assert await client.arcount("arr") == 0
        with pytest.raises(DataError):
            await client.ardelrange("arr", [])

    async def test_arget(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arget("arr", 0) == _s("a")
        assert await client.arget("arr", 4) is None

    async def test_argetrange(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.argetrange("arr", 0, 2) == [_s("a"), _s("b"), _s("c")]

    async def test_argrep(self, client, _s):
        await client.arinsert("arr", ["ab", "bc", "cd"])
        pred = Predicate(match="b") | Predicate(exact="cd")
        assert await client.argrep("arr", 0, 2, pred) == [0, 1, 2]
        pred = Predicate(glob="b*") & Predicate(re=r"[a-z]+")
        assert await client.argrep("arr", 0, 2, pred) == [1]
        pred = Predicate(match="B")
        assert await client.argrep("arr", 0, 2, pred, limit=1, nocase=True) == [0]
        pred = Predicate(match="b")
        assert await client.argrep("arr", 0, 1, pred, withvalues=True) == [
            (0, _s("ab")),
            (1, _s("bc")),
        ]

    async def test_arinfo(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        assert (await client.arinfo("arr"))[_s("count")] == 3
        assert _s("avg-sparse-size") in await client.arinfo("arr", full=True)

    async def test_arinsert_seek_next(self, client, _s):
        assert await client.arset("arr", 0, ["a", "b", "c"]) == 3
        assert await client.arnext("arr") == 0
        assert await client.arseek("arr", 1)
        assert await client.arinsert("arr", ["d", "e", "f"]) == 3
        assert await client.argetrange("arr", 0, 3) == [_s("a"), _s("d"), _s("e"), _s("f")]
        assert await client.arnext("arr") == 4

    async def test_arlastitems(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        await client.arset("arr", 3, ["d", "e", "f"])
        assert await client.arlastitems("arr", 2, rev=True) == (_s("c"), _s("b"))

    async def test_arlen(self, client, _s):
        assert await client.arlen("arr") == await client.arcount("arr") == 0
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arlen("arr") == await client.arcount("arr") == 3
        await client.ardel("arr", [1])
        assert await client.arcount("arr") == 2
        assert await client.arlen("arr") == 3

    async def test_armget(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.armget("arr", [0, 3]) == (_s("a"), None)

    async def test_armset(self, client, _s):
        assert await client.armset("arr", {0: "a", 2: "c"}) == 2
        assert await client.argetrange("arr", 0, 2) == [_s("a"), None, _s("c")]

    async def test_arring(self, client, _s):
        assert await client.arring("arr", 3, ["a", "b", "c"]) == 2
        assert await client.arring("arr", 3, ["d"]) == 0
        assert await client.argetrange("arr", 0, 2) == [_s("d"), _s("b"), _s("c")]

    async def test_arop(self, client, _s):
        await client.arinsert("arr", ["a", 100, 200, 300])
        assert await client.arop("arr", 0, 3, match="a") == 1
        assert await client.arop("arr", 0, 3, op=PureToken.SUM) == 600
        assert await client.arop("arr", 0, 3, op=PureToken.MAX) == 300
        assert await client.arop("arr", 0, 3, op=PureToken.MIN) == 100
        assert await client.arop("arr", 0, 3, op=PureToken.AND) == 0
        assert await client.arop("arr", 0, 3, op=PureToken.OR) == 492
        assert await client.arop("arr", 0, 3, op=PureToken.XOR) == 384

    async def test_arscan(self, client, _s):
        await client.arinsert("arr", ["a", "b", "c"])
        assert await client.arscan("arr", 0, 2, limit=2) == [(0, _s("a")), (1, _s("b"))]
