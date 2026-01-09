from __future__ import annotations

import pytest

from coredis import Redis
from coredis._concurrency import gather
from tests.conftest import module_targets


@module_targets()
class TestTopK:
    async def test_reserve(self, client: Redis, _s):
        assert await client.topk.reserve("topk", 3)
        assert await client.topk.reserve("topkcustom", 3, 16, 14, 0.8)
        infos = await gather(client.topk.info("topk"), client.topk.info("topkcustom"))
        assert infos[0][_s("width")] == 8
        assert infos[0][_s("depth")] == 7
        assert infos[1][_s("width")] == 16
        assert infos[1][_s("depth")] == 14

    async def test_add(self, client: Redis, _s):
        assert await client.topk.reserve("topk", 3)
        assert (None, None, None) == await client.topk.add("topk", ["1", "2", "3"])
        assert (_s("1"), _s("3"), _s("4")) == await client.topk.add("topk", ["4", "5", "6"])

    async def test_incrby(self, client: Redis, _s):
        assert await client.topk.reserve("topk", 3)
        assert (None, None, None) == await client.topk.add("topk", ["1", "2", "3"])
        assert (None, None, None) == await client.topk.incrby("topk", {"1": 2, "2": 2, "3": 2})
        assert (None, None, None) == await client.topk.add("topk", ["4", "5", "6"])
        assert (None, None, None) == await client.topk.add("topk", ["4", "5", "6"])
        assert (_s("1"), _s("3"), _s("4")) == await client.topk.add("topk", ["4", "5", "6"])

    async def test_query(self, client: Redis, _s):
        assert await client.topk.reserve("topk", 3)
        assert (None, None, None, _s("1"), _s("3"), _s("4")) == await client.topk.add(
            "topk", ["1", "2", "3", "4", "5", "6"]
        )
        assert (_s("5"), _s("6"), _s("2")) == await client.topk.add("topk", ["4", "5", "6"])
        assert (False, False, False, True, True, True) == await client.topk.query(
            "topk", ["1", "2", "3", "4", "5", "6"]
        )
        assert (2, 2, 2) == await client.topk.count("topk", ["4", "5", "6"])
        assert (_s("4"), _s("6"), _s("5")) == await client.topk.list("topk")
        assert {_s("4"): 2, _s("5"): 2, _s("6"): 2} == await client.topk.list(
            "topk", withcount=True
        )

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        async with client.pipeline(transaction=transaction) as p:
            results = [
                p.topk.reserve("topk", 3),
                p.topk.add("topk", ["1", "2", "3"]),
                p.topk.query("topk", ["1", "2", "3"]),
            ]
        assert await gather(*results) == (True, (None, None, None), (True, True, True))
