from __future__ import annotations

import pytest

from coredis import Redis
from coredis._concurrency import gather
from coredis.exceptions import ResponseError
from tests.conftest import module_targets


@module_targets()
class TestCuckooFilter:
    async def test_reserve(self, client: Redis, _s):
        assert await client.cf.reserve("filter", 1000)
        with pytest.raises(ResponseError):
            await client.cf.reserve("filter", 1000)
        assert await client.cf.reserve("filter_bucket", 1000, 3)
        info = await gather(
            client.cf.info("filter"),
            client.cf.info("filter_bucket"),
        )
        assert info[0][_s("Bucket size")] == 2
        assert info[1][_s("Bucket size")] == 3

        assert await client.cf.reserve("filter_custom_expansion", 4, 2, 1, 2)
        [await client.cf.add("filter_custom_expansion", i) for i in range(0, 4)]
        info = await client.cf.info("filter_custom_expansion")
        assert info[_s("Number of filters")] == 1
        assert info[_s("Max iterations")] == 1
        assert info[_s("Expansion rate")] == 2

    async def test_add(self, client: Redis):
        assert True is await client.cf.add("filter", 1)
        assert True is await client.cf.add("filter", 1)
        assert False is await client.cf.addnx("filter", 1)
        assert True is await client.cf.addnx("filter", 2)

    async def test_delete(self, client: Redis):
        assert True is await client.cf.add("filter", 1)
        assert True is await client.cf.add("filter", 1)
        assert False is await client.cf.addnx("filter", 1)
        assert True is await client.cf.delete("filter", 1)
        assert False is await client.cf.addnx("filter", 1)

        assert True is await client.cf.add("filter", 2)
        assert False is await client.cf.addnx("filter", 2)
        assert True is await client.cf.delete("filter", 2)
        assert True is await client.cf.addnx("filter", 2)

    async def test_insert(self, client: Redis):
        assert (True, True, True) == await client.cf.insert("filter", [1, 2, 3])
        assert (True, True, True) == await client.cf.insert("filter_custom", [1, 2, 3], 10)
        assert (True, True, True) == await client.cf.insert("filter", [1, 2, 3], 10)
        assert (False, False, False) == await client.cf.insertnx("filter", [1, 2, 3], 10)
        with pytest.raises(ResponseError):
            await client.cf.insert("filter_missing", [1, 2, 3], nocreate=True)
        with pytest.raises(ResponseError):
            await client.cf.insertnx("filter_missing", [1, 2, 3], nocreate=True)

    async def test_count(self, client: Redis):
        assert await client.cf.add("filter", 1)
        assert 1 == await client.cf.count("filter", 1)
        assert await client.cf.add("filter", 1)
        assert 2 == await client.cf.count("filter", 1)

    async def test_exists(self, client: Redis):
        assert await client.cf.add("filter", 1)
        assert await client.cf.exists("filter", 1)
        assert not await client.cf.exists("filter", 2)
        assert (True, False) == await client.cf.mexists("filter", [1, 2])

    async def test_dump_load(self, client: Redis):
        await client.cf.add("filter", 1)
        it = None
        scanned = []
        while True:
            it, data = await client.cf.scandump("filter", it or 0)
            if it == 0:
                break
            else:
                scanned.append([it, data])

        for chunk in scanned:
            await client.cf.loadchunk("newfilter", *chunk)
        assert not await client.cf.addnx("newfilter", 1)

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        async with client.pipeline(transaction=transaction) as p:
            results = [
                p.cf.add("filter", 1),
                p.cf.add("filter", 2),
                p.cf.exists("filter", 2),
                p.cf.mexists("filter", [1, 2, 3]),
                p.cf.delete("filter", 2),
                p.cf.exists("filter", 2),
            ]
        assert await gather(*results) == (True, True, True, (True, True, False), True, False)
