from __future__ import annotations

import asyncio

from coredis import Redis
from tests.conftest import targets


@targets(
    "redis_stack",
    "redis_stack_cached",
    "redis_stack_cluster",
)
class TestTopK:
    async def test_reserve(self, client: Redis):
        assert await client.topk.reserve("topk", 3)
        assert await client.topk.reserve("topkcustom", 3, 16, 14, 0.8)
        infos = await asyncio.gather(
            client.topk.info("topk"), client.topk.info("topkcustom")
        )
        assert infos[0]["width"] == 8
        assert infos[0]["depth"] == 7
        assert infos[1]["width"] == 16
        assert infos[1]["depth"] == 14

    async def test_add(self, client: Redis):
        assert await client.topk.reserve("topk", 3)
        assert (None, None, None) == await client.topk.add("topk", ["1", "2", "3"])
        assert ("1", "3", "4") == await client.topk.add("topk", ["4", "5", "6"])

    async def test_query(self, client: Redis):
        assert await client.topk.reserve("topk", 3)
        assert (None, None, None, "1", "3", "4") == await client.topk.add(
            "topk", ["1", "2", "3", "4", "5", "6"]
        )
        assert ("5", "6", "2") == await client.topk.add("topk", ["4", "5", "6"])
        assert (False, False, False, True, True, True) == await client.topk.query(
            "topk", ["1", "2", "3", "4", "5", "6"]
        )
        assert (2, 2, 2) == await client.topk.count("topk", ["4", "5", "6"])
        assert ("4", "6", "5") == await client.topk.list("topk")
        assert {"4": 2, "5": 2, "6": 2} == await client.topk.list(
            "topk", withcount=True
        )
