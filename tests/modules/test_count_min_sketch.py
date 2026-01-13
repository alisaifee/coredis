from __future__ import annotations

import pytest

from coredis import Redis
from coredis._concurrency import gather
from coredis.exceptions import ResponseError
from tests.conftest import module_targets


@module_targets()
class TestCountMinSketch:
    async def test_init(self, client: Redis, _s):
        assert await client.cms.initbydim("sketch", 2, 50)
        assert await client.cms.initbyprob("sketchprob", 0.042, 0.42)
        infos = await gather(client.cms.info("sketch"), client.cms.info("sketchprob"))
        assert infos[0][_s("width")] == 2
        assert infos[0][_s("depth")] == 50
        assert infos[1][_s("width")] == 48
        assert infos[1][_s("depth")] == 2

    async def test_incrby(self, client: Redis):
        assert await client.cms.initbydim("sketch", 2, 50)
        assert (1, 2) == await client.cms.incrby(
            "sketch",
            {
                "fu": 1,
                "bar": 2,
            },
        )

        with pytest.raises(ResponseError):
            await client.cms.incrby(
                "missingsketch",
                {
                    "fu": 1,
                },
            )
        await client.set("notsketch", 1)
        with pytest.raises(ResponseError):
            await client.cms.incrby(
                "notsketch",
                {
                    "fu": 1,
                },
            )

    @pytest.mark.nocluster
    async def test_merge(self, client):
        assert await client.cms.initbydim("sketch1", 2, 50)
        assert await client.cms.initbydim("sketch2", 2, 50)
        assert await client.cms.initbydim("sketch", 2, 50)
        assert await client.cms.initbydim("sketchweighed", 2, 50)
        assert (1, 1) == await client.cms.incrby(
            "sketch1",
            {
                "fu": 1,
                "bar": 1,
            },
        )
        assert (2,) == await client.cms.incrby(
            "sketch2",
            {
                "bar": 2,
            },
        )
        await client.cms.merge("sketch", ["sketch1", "sketch2"])
        await client.cms.merge("sketchweighed", ["sketch1", "sketch2"], [2, 1])

        assert (1, 3) == await client.cms.query("sketch", ["fu", "bar"])
        assert (2, 4) == await client.cms.query("sketchweighed", ["fu", "bar"])

    @pytest.mark.clusteronly
    async def test_merge_cluster(self, client):
        assert await client.cms.initbydim("sketch1{a}", 2, 50)
        assert await client.cms.initbydim("sketch2{a}", 2, 50)
        assert await client.cms.initbydim("sketch{a}", 2, 50)
        assert await client.cms.initbydim("sketchweighed{a}", 2, 50)
        assert (1, 1) == await client.cms.incrby(
            "sketch1{a}",
            {
                "fu": 1,
                "bar": 1,
            },
        )
        assert (2,) == await client.cms.incrby(
            "sketch2{a}",
            {
                "bar": 2,
            },
        )
        await client.cms.merge("sketch{a}", ["sketch1{a}", "sketch2{a}"])
        await client.cms.merge("sketchweighed{a}", ["sketch1{a}", "sketch2{a}"], [2, 1])

        assert (1, 3) == await client.cms.query("sketch{a}", ["fu", "bar"])
        assert (2, 4) == await client.cms.query("sketchweighed{a}", ["fu", "bar"])

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        async with client.pipeline(transaction=transaction) as p:
            results = [
                p.cms.initbydim("sketch", 2, 50),
                p.cms.incrby("sketch", {"fu": 1, "bar": 2}),
                p.cms.incrby("sketch", {"fu": 3}),
                p.cms.query("sketch", ["fu", "bar"]),
            ]
        assert await gather(*results) == (True, (1, 2), (4,), (4, 2))
