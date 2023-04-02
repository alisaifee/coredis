from __future__ import annotations

import asyncio

import pytest

from coredis import Redis
from coredis.exceptions import ResponseError
from tests.conftest import targets


@targets(
    "redis_stack",
    "redis_stack_cluster",
)
class TestBloomFilter:
    async def test_reserve(self, client: Redis):
        assert await client.bf.reserve("filter", 0.1, 1000)
        with pytest.raises(ResponseError):
            await client.bf.reserve("filter", 0.1, 1000)
        assert await client.bf.reserve("filter_ex", 0.1, 1000, 3)
        info = await asyncio.gather(
            client.bf.info("filter"),
            client.bf.info("filter_ex"),
        )
        assert info[0]["Expansion rate"] == 2
        assert info[1]["Expansion rate"] == 3

    async def test_reserve_non_scaling(self, client: Redis):
        assert await client.bf.reserve("filter_nonscaling", 0.1, 1, nonscaling=True)
        assert await client.bf.add("filter_nonscaling", 1)
        assert not await client.bf.add("filter_nonscaling", 1)
        with pytest.raises(ResponseError):
            assert await client.bf.add("filter_nonscaling", 2)

    async def test_multi_add(self, client: Redis):
        await client.bf.add("filter", 1)
        assert (False, True, True) == await client.bf.madd("filter", [1, 2, 3])
        assert (False, False, False) == await client.bf.madd("filter", [1, 2, 3])

    async def test_insert(self, client: Redis):
        assert (True, True, True) == await client.bf.insert("filter", [1, 2, 3])
        assert (True, True, True) == await client.bf.insert(
            "filter_custom", [1, 2, 3], 3, 0.1
        )
        assert (True, True, True) == await client.bf.insert(
            "filter_custom_noscale", [1, 2, 3], 3, 0.1, nonscaling=True
        )
        with pytest.raises(ResponseError):
            await client.bf.insert("filter_missing", [1, 2, 3], nocreate=True)

    @pytest.mark.min_module_version("bf", "2.4.4")
    async def test_cardinality(self, client: Redis):
        assert await client.bf.add("filter", 1)
        assert 1 == await client.bf.card("filter")
        assert await client.bf.add("filter", 2)
        assert 2 == await client.bf.card("filter")
        assert 0 == await client.bf.card("nonexistent")
        await client.set("nonfilter", 1)
        with pytest.raises(ResponseError):
            await client.bf.card("nonfilter")

    async def test_exists(self, client: Redis):
        assert await client.bf.add("filter", 1)
        assert await client.bf.exists("filter", 1)
        assert not await client.bf.exists("filter", 2)
        assert (True, False) == await client.bf.mexists("filter", [1, 2])

    async def test_dump_load(self, client: Redis):
        await client.bf.add("filter", 1)
        it = None
        scanned = []
        while True:
            it, data = await client.bf.scandump("filter", it or 0)
            if it == 0:
                break
            else:
                scanned.append([it, data])

        for chunk in scanned:
            await client.bf.loadchunk("newfilter", *chunk)
        assert not await client.bf.add("newfilter", 1)
