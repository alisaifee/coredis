from __future__ import annotations

import math
import time
from datetime import datetime, timedelta

import anyio
import pytest

from coredis import PureToken, Redis
from coredis._concurrency import gather
from tests.conftest import module_targets


@module_targets()
class TestTimeseries:
    async def test_create(self, client: Redis, _s):
        assert await client.timeseries.create("ts1")
        assert await client.timeseries.create("ts2", retention=5)
        assert await client.timeseries.create("ts3", labels={"Redis": "Labs"})
        assert await client.timeseries.create("4", retention=20, labels={"Time": "Series"})
        info = await client.timeseries.info("4")
        assert 20 == info[_s("retentionTime")]
        assert _s("Series") == info[_s("labels")][_s("Time")]

        # Test for a chunk size of 128 Bytes
        assert await client.timeseries.create("ts4", chunk_size=128)
        info = await client.timeseries.info("ts4")
        assert 128, info[_s("chunkSize")]

    @pytest.mark.parametrize(
        "duplicate_policy",
        [
            PureToken.BLOCK,
            PureToken.FIRST,
            PureToken.LAST,
            PureToken.MAX,
            PureToken.MIN,
            PureToken.SUM,
        ],
    )
    async def test_create_duplicate_policy(self, client: Redis, duplicate_policy, _s):
        # Test for duplicate policy
        ts_name = f"ts-{duplicate_policy}"
        assert await client.timeseries.create(ts_name, duplicate_policy=duplicate_policy)
        info = await client.timeseries.info(ts_name)
        assert duplicate_policy == info[_s("duplicatePolicy")]

    async def test_alter(self, client: Redis, _s):
        assert await client.timeseries.create("ts1")
        res = await client.timeseries.info("ts1")
        assert 0 == res[_s("retentionTime")]
        assert await client.timeseries.alter("ts1", retention=10)
        res = await client.timeseries.info("ts1")
        assert {} == res[_s("labels")]
        res = await client.timeseries.info("ts1")
        assert 10 == res[_s("retentionTime")]
        assert await client.timeseries.alter("ts1", labels={"Time": "Series"})
        res = await client.timeseries.info("ts1")
        assert _s("Series") == res[_s("labels")][_s("Time")]
        res = await client.timeseries.info("ts1")
        assert 10 == res[_s("retentionTime")]
        assert await client.timeseries.alter("ts1", chunk_size=8192)
        res = await client.timeseries.info("ts1")
        assert 8192 == res[_s("chunkSize")]

    async def test_alter_duplicate_policy(self, client: Redis, _s):
        assert await client.timeseries.create("ts1")
        assert await client.timeseries.alter("ts1", duplicate_policy=PureToken.MIN)
        info = await client.timeseries.info("ts1")
        assert _s("min") == info[_s("duplicatePolicy")]

    async def test_add(self, client: Redis, _s):
        assert 1 == await client.timeseries.add("ts1", 1, 1)
        assert 2 == await client.timeseries.add("ts2", 2, 3, retention=10)
        assert 3 == await client.timeseries.add("ts3", 3, 2, labels={"Redis": "Labs"})
        assert 4 == await client.timeseries.add(
            "4", 4, 2, retention=10, labels={"Redis": "Labs", "Time": "Series"}
        )
        res = await client.timeseries.add("ts5", "*", 1)
        assert abs(time.time() - round(float(res) / 1000)) < 1.0

        info = await client.timeseries.info("4")
        assert 10 == info[_s("retentionTime")]
        assert _s("Labs") == info[_s("labels")][_s("Redis")]

        # Test for a chunk size of 128 Bytes on TS.ADD
        assert await client.timeseries.add("ts6", 1, 10.0, chunk_size=128)
        info = await client.timeseries.info("ts6")
        assert 128 == info[_s("chunkSize")]

        assert await client.timeseries.add("ts7", 4, 10.0, encoding=PureToken.UNCOMPRESSED)
        info = await client.timeseries.info("ts7")
        assert _s("uncompressed") == info[_s("chunkType")]

    async def test_add_duplicate_policy(self, client: Redis):
        # Test for duplicate policy BLOCK
        assert 1 == await client.timeseries.add("ts-add-block", 1, 5.0)
        with pytest.raises(Exception):
            await client.timeseries.add("ts-add-block", 1, 5.0, duplicate_policy=PureToken.BLOCK)

        # Test for duplicate policy LAST
        assert 1 == await client.timeseries.add("ts-add-last", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "ts-add-last", 1, 10.0, duplicate_policy=PureToken.LAST
        )
        res = await client.timeseries.get("ts-add-last")
        assert 10.0 == res[1]

        # Test for duplicate policy FIRST
        assert 1 == await client.timeseries.add("ts-add-first", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "ts-add-first", 1, 10.0, duplicate_policy=PureToken.FIRST
        )
        res = await client.timeseries.get("ts-add-first")
        assert 5.0 == res[1]

        # Test for duplicate policy MAX
        assert 1 == await client.timeseries.add("ts-add-max", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "ts-add-max", 1, 10.0, duplicate_policy=PureToken.MAX
        )
        res = await client.timeseries.get("ts-add-max")
        assert 10.0 == res[1]

        # Test for duplicate policy MIN
        assert 1 == await client.timeseries.add("ts-add-min", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "ts-add-min", 1, 10.0, duplicate_policy=PureToken.MIN
        )
        res = await client.timeseries.get("ts-add-min")
        assert 5.0 == res[1]

    async def test_madd(self, client: Redis):
        await client.timeseries.create("a")
        assert (1, 2, 3) == await client.timeseries.madd([("a", 1, 5), ("a", 2, 10), ("a", 3, 15)])

    async def test_incrby(self, client: Redis, _s):
        for _ in range(100):
            assert await client.timeseries.incrby("ts1", 1)
            await anyio.sleep(0.001)
        assert 100 == (await client.timeseries.get("ts1"))[1]

        assert await client.timeseries.incrby("ts2", 1.5, timestamp=5)
        assert (5, 1.5) == await client.timeseries.get("ts2")

        assert await client.timeseries.incrby("ts3", 10, chunk_size=128)
        info = await client.timeseries.info("ts3")
        assert 128 == info[_s("chunkSize")]

        assert await client.timeseries.incrby(
            "ts4",
            10,
            uncompressed=True,
        )
        info = await client.timeseries.info("ts4")
        assert _s("uncompressed") == info[_s("chunkType")]

        assert await client.timeseries.incrby(
            "ts5",
            10,
            retention=timedelta(seconds=120),
        )
        info = await client.timeseries.info("ts5")
        assert 120 * 1000 == info[_s("retentionTime")]

        assert await client.timeseries.incrby("ts6", 10, labels={"fu": "bar"})
        info = await client.timeseries.info("ts6")
        assert {_s("fu"): _s("bar")} == info[_s("labels")]

    async def test_decrby(self, client: Redis, _s):
        for _ in range(100):
            assert await client.timeseries.decrby("ts1", 1)
            await anyio.sleep(0.001)
        assert -100 == (await client.timeseries.get("ts1"))[1]

        assert await client.timeseries.decrby("ts2", 1.5, timestamp=5)
        assert (5, -1.5) == await client.timeseries.get("ts2")

        assert await client.timeseries.decrby("ts3", 10, chunk_size=128)
        info = await client.timeseries.info("ts3")
        assert 128 == info[_s("chunkSize")]

        assert await client.timeseries.decrby(
            "ts4",
            10,
            uncompressed=True,
        )
        info = await client.timeseries.info("ts4")
        assert _s("uncompressed") == info[_s("chunkType")]

        assert await client.timeseries.decrby(
            "ts5",
            10,
            retention=timedelta(seconds=120),
        )
        info = await client.timeseries.info("ts5")
        assert 120 * 1000 == info[_s("retentionTime")]

        assert await client.timeseries.decrby("ts6", 10, labels={"fu": "bar"})
        info = await client.timeseries.info("ts6")
        assert {_s("fu"): _s("bar")} == info[_s("labels")]

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_create_and_delete_rule(self, client: Redis, _s):
        # test rule creation
        time = 100
        await client.timeseries.create("ts1{a}")
        await client.timeseries.create("ts2{a}")
        await client.timeseries.create("ts3{a}")

        await client.timeseries.createrule("ts1{a}", "ts2{a}", PureToken.AVG, 100)
        await client.timeseries.createrule(
            "ts1{a}", "ts3{a}", PureToken.AVG, 100, aligntimestamp=True
        )

        for i in range(50):
            await client.timeseries.add("ts1{a}", time + i * 2, 1)
            await client.timeseries.add("ts1{a}", time + i * 2 + 1, 2)
        await client.timeseries.add("ts1{a}", time * 2, 1.5)

        assert round((await client.timeseries.get("ts2{a}"))[1], 5) == 1.5
        assert round((await client.timeseries.get("ts3{a}"))[1], 5) == 1.0

        info = await client.timeseries.info("ts1{a}")
        assert info[_s("rules")][_s("ts2{a}")][0] == 100

        # test rule deletion
        await client.timeseries.deleterule("ts1{a}", "ts2{a}")
        await client.timeseries.deleterule("ts1{a}", "ts3{a}")
        info = await client.timeseries.info("ts1{a}")
        assert not info[_s("rules")]

    async def test_del_range(self, client: Redis):
        try:
            await client.timeseries.delete("test", 0, 100)
        except Exception as e:
            assert e.__str__() != ""

        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        assert 22 == await client.timeseries.delete("ts1", 0, 21)
        assert () == await client.timeseries.range("ts1", 0, 21)
        assert ((22, 1.0),) == await client.timeseries.range("ts1", 22, 22)

    async def test_range(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        assert 100 == len(await client.timeseries.range("ts1", 0, 200))
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        assert 200 == len(await client.timeseries.range("ts1", 0, 500))
        # last sample isn't returned
        assert 20 == len(
            await client.timeseries.range(
                "ts1", 0, 500, aggregator=PureToken.AVG, bucketduration=10
            )
        )

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_range_advanced(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts1", i + 200, i % 7)

        assert 2 == len(
            await client.timeseries.range(
                "ts1",
                0,
                500,
                filter_by_ts=[i for i in range(10, 20)],
                min_value=1,
                max_value=2,
            )
        )
        assert ((0, 10.0), (10, 1.0)) == await client.timeseries.range(
            "ts1", 0, 10, aggregator=PureToken.COUNT, bucketduration=10, align="+"
        )
        assert ((0, 5.0), (5, 6.0)) == await client.timeseries.range(
            "ts1", 0, 10, aggregator=PureToken.COUNT, bucketduration=10, align=5
        )
        assert ((0, 2.55), (10, 3.0)) == await client.timeseries.range(
            "ts1", 0, 10, aggregator=PureToken.TWA, bucketduration=10
        )

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_range_empty_buckets(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)

        # test empty buckets
        res = await client.timeseries.range(
            "ts1",
            0,
            300,
            aggregator=PureToken.AVG,
            bucketduration=10,
            empty=True,
        )

        assert all(math.isnan(k[1]) for k in res[10:20])

    async def test_revrange(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        assert 100 == len(await client.timeseries.range("ts1", 0, 200))
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        assert 200 == len(await client.timeseries.range("ts1", 0, 500))
        # first sample isn't returned
        assert 20 == len(
            await client.timeseries.revrange(
                "ts1", 0, 500, aggregator=PureToken.AVG, bucketduration=10
            )
        )

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_revrange_advanced(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        assert 100 == len(await client.timeseries.range("ts1", 0, 200))
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        assert 2 == len(
            await client.timeseries.revrange(
                "ts1",
                0,
                500,
                filter_by_ts=[i for i in range(10, 20)],
                min_value=1,
                max_value=2,
            )
        )
        assert ((10, 1.0), (0, 10.0)) == await client.timeseries.revrange(
            "ts1", 0, 10, aggregator=PureToken.COUNT, bucketduration=10, align="+"
        )
        assert ((1, 10.0), (0, 1.0)) == await client.timeseries.revrange(
            "ts1", 0, 10, aggregator=PureToken.COUNT, bucketduration=10, align=1
        )

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_revrange_empty_buckets(self, client: Redis):
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)

        # test empty buckets
        res = await client.timeseries.revrange(
            "ts1",
            0,
            300,
            aggregator=PureToken.AVG,
            bucketduration=10,
            empty=True,
        )

        assert all(math.isnan(k[1]) for k in res[10:20])

    async def test_mrange(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"])
        assert 2 == len(res)
        assert 100 == len(res[_s("ts1")][1])

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"], count=10)
        assert 2 == len(res)
        assert 10 == len(res[_s("ts1")][1])

        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)

        res = await client.timeseries.mrange(
            0,
            500,
            filters=["Test=This"],
            aggregator=PureToken.AVG,
            bucketduration=10,
            buckettimestamp="-",
        )
        assert 2 == len(res)
        assert 20 == len(res[_s("ts1")][1])

        # test withlabels
        assert {} == res[_s("ts1")][0]
        res = await client.timeseries.mrange(0, 200, filters=["Test=This"], withlabels=True)
        assert {_s("Test"): _s("This"), _s("team"): _s("ny")} == res[_s("ts1")][0]

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_mrange_empty_buckets(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        # test empty buckets
        res = await client.timeseries.mrange(
            0,
            300,
            filters=["Test=This"],
            aggregator=PureToken.AVG,
            bucketduration=10,
            empty=True,
        )

        assert all(math.isnan(k[1]) for k in res[_s("ts1")][1][10:20])

    async def test_mrange_filter_align(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        # test with selected labels
        res = await client.timeseries.mrange(
            0, 200, filters=["Test=This"], selected_labels=["team"]
        )
        assert {_s("team"): _s("ny")} == res[_s("ts1")][0]
        assert {_s("team"): _s("sf")} == res[_s("ts2")][0]

        # test with filterby
        res = await client.timeseries.mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            min_value=1,
            max_value=2,
        )
        assert ((15, 1.0), (16, 2.0)) == res[_s("ts1")][1]

        # test align
        res = await client.timeseries.mrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align="-",
        )
        assert ((0, 10.0), (10, 1.0)) == res[_s("ts1")][1]
        res = await client.timeseries.mrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align=5,
        )
        assert ((0, 5.0), (5, 6.0)) == res[_s("ts1")][1]

    @pytest.mark.nocluster
    async def test_mrange_grouped(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        # test groupby
        res = await client.timeseries.mrange(
            0,
            3,
            filters=["Test=This"],
            groupby="Test",
            reducer=PureToken.SUM,
        )
        assert ((0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)) == res[_s("Test=This")][1]
        res = await client.timeseries.mrange(
            0,
            3,
            filters=["Test=This"],
            groupby="Test",
            reducer=PureToken.MAX,
        )
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res[_s("Test=This")][1]
        res = await client.timeseries.mrange(
            0,
            3,
            filters=["Test=This"],
            groupby="team",
            reducer=PureToken.MIN,
        )
        assert 2 == len(res)
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res[_s("team=ny")][1]
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res[_s("team=sf")][1]

    async def test_mrevrange(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        res = await client.timeseries.mrevrange(0, 200, filters=["Test=This"])
        assert 2 == len(res)
        assert 100 == len(res[_s("ts1")][1])

        res = await client.timeseries.mrevrange(
            0,
            200,
            filters=["Test=This"],
            count=10,
        )
        assert 10 == len(res[_s("ts1")][1])

        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        res = await client.timeseries.mrevrange(
            0,
            500,
            filters=["Test=This"],
            aggregator=PureToken.AVG,
            bucketduration=10,
            buckettimestamp="-",
        )
        assert 2 == len(res)
        assert 20 == len(res[_s("ts1")][1])
        assert {} == res[_s("ts1")][0]

        # test withlabels
        res = await client.timeseries.mrevrange(0, 200, filters=["Test=This"], withlabels=True)
        assert {_s("Test"): _s("This"), _s("team"): _s("ny")} == res[_s("ts1")][0]

        # test with selected labels
        res = await client.timeseries.mrevrange(
            0, 200, filters=["Test=This"], selected_labels=["team"]
        )
        assert {_s("team"): _s("ny")} == res[_s("ts1")][0]
        assert {_s("team"): _s("sf")} == res[_s("ts2")][0]

        # test filterby
        res = await client.timeseries.mrevrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            min_value=1,
            max_value=2,
        )
        assert ((16, 2.0), (15, 1.0)) == res[_s("ts1")][1]

        # test align
        res = await client.timeseries.mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align="-",
        )
        assert ((10, 1.0), (0, 10.0)) == res[_s("ts1")][1]

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_mrevrange_empty_buckets(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)

        # test empty buckets
        res = await client.timeseries.mrevrange(
            0,
            300,
            filters=["Test=This"],
            aggregator=PureToken.AVG,
            bucketduration=10,
            empty=True,
        )

        assert all(math.isnan(k[1]) for k in res[_s("ts1")][1][10:20])

    @pytest.mark.nocluster
    async def test_mrevrange_grouped(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        # test groupby
        res = await client.timeseries.mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reducer=PureToken.SUM
        )
        assert ((3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)) == res[_s("Test=This")][1]
        res = await client.timeseries.mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reducer=PureToken.MAX
        )
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res[_s("Test=This")][1]
        res = await client.timeseries.mrevrange(
            0, 3, filters=["Test=This"], groupby="team", reducer=PureToken.MIN
        )
        assert 2 == len(res)
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res[_s("team=ny")][1]
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res[_s("team=sf")][1]

    async def test_get(self, client: Redis):
        name = "test"
        await client.timeseries.create(name)
        assert not await client.timeseries.get(name)
        await client.timeseries.add(name, 2, 3)
        assert (2, 3.0) == (await client.timeseries.get(name))
        await client.timeseries.add(name, 3, 4.1)
        assert (3, 4.1) == (await client.timeseries.get(name))

    async def test_mget(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This"})
        await client.timeseries.create("ts2", labels={"Test": "This", "Taste": "That"})
        act_res = await client.timeseries.mget(["Test=This"])
        exp_res = {_s("ts1"): ({}, ()), _s("ts2"): ({}, ())}
        assert act_res == exp_res
        await client.timeseries.add("ts1", "*", 15)
        await client.timeseries.add("ts2", "*", 25)
        res = await client.timeseries.mget(["Test=This"])
        assert 15 == res[_s("ts1")][1][1]
        assert 25 == res[_s("ts2")][1][1]
        res = await client.timeseries.mget(["Taste=That"])
        assert 25 == res[_s("ts2")][1][1]

        # test withlabels
        assert {} == res[_s("ts2")][0]
        res = await client.timeseries.mget(["Taste=That"], withlabels=True)
        assert {_s("Taste"): _s("That"), _s("Test"): _s("This")} == res[_s("ts2")][0]

        res = await client.timeseries.mget(["Taste=That"], selected_labels=["Test"])
        assert {_s("Test"): _s("This")} == res[_s("ts2")][0]

    @pytest.mark.min_module_version("timeseries", "1.8.0")
    async def test_compaction_latest(self, client: Redis, _s):
        await client.timeseries.create("ts1{a}")
        await client.timeseries.create("ts1{a}-avg", labels={"fu": "bar"})
        await client.timeseries.createrule(
            "ts1{a}", "ts1{a}-avg", PureToken.AVG, timedelta(seconds=60)
        )
        ref = datetime.fromtimestamp(0)

        for i in range(140):
            await client.timeseries.add("ts1{a}", ref + timedelta(seconds=i), i)

        sample = await client.timeseries.get("ts1{a}-avg")
        assert sample[0] == 60000
        sample_latest = await client.timeseries.get("ts1{a}-avg", latest=True)
        assert sample_latest[0] == 120000

        sample = (await client.timeseries.mget(["fu=bar"]))[_s("ts1{a}-avg")][1]
        assert sample[0] == 60000
        sample_latest = (await client.timeseries.mget(["fu=bar"], latest=True))[_s("ts1{a}-avg")][1]
        assert sample_latest[0] == 120000

        assert 2 == len(await client.timeseries.range("ts1{a}-avg", 0, 140000))
        assert 2 == len(await client.timeseries.revrange("ts1{a}-avg", 0, 140000))
        assert 3 == len(await client.timeseries.range("ts1{a}-avg", 0, 140000, latest=True))
        assert 3 == len(await client.timeseries.revrange("ts1{a}-avg", 0, 140000, latest=True))

        assert 2 == len(
            (await client.timeseries.mrange(0, 140000, filters=["fu=bar"]))[_s("ts1{a}-avg")][1]
        )
        assert 2 == len(
            (await client.timeseries.mrevrange(0, 140000, filters=["fu=bar"]))[_s("ts1{a}-avg")][1]
        )
        assert 3 == len(
            (await client.timeseries.mrange(0, 140000, filters=["fu=bar"], latest=True))[
                _s("ts1{a}-avg")
            ][1]
        )
        assert 3 == len(
            (await client.timeseries.mrevrange(0, 140000, filters=["fu=bar"], latest=True))[
                _s("ts1{a}-avg")
            ][1]
        )

    async def test_info(self, client: Redis, _s):
        await client.timeseries.create("ts1", retention=5, labels={"currentLabel": "currentData"})
        info = await client.timeseries.info("ts1")
        assert 5 == info[_s("retentionTime")]
        assert info[_s("labels")][_s("currentLabel")] == _s("currentData")
        await client.timeseries.add("ts1", 0, 1)

        info = await client.timeseries.info("ts1", debug=True)
        chunks = info[_s("Chunks")]
        assert chunks[0][_s("startTimestamp")] == chunks[0][_s("endTimestamp")] == 0

    async def test_info_duplicate_policy(self, client: Redis, _s):
        await client.timeseries.create("ts2", duplicate_policy=PureToken.MIN)
        info = await client.timeseries.info("ts2")
        assert _s("min") == info[_s("duplicatePolicy")]

    async def test_query_index(self, client: Redis, _s):
        await client.timeseries.create("ts1", labels={"Test": "This"})
        await client.timeseries.create("ts2", labels={"Test": "This", "Taste": "That"})
        assert 2 == len(await client.timeseries.queryindex(["Test=This"]))
        assert 1 == len(await client.timeseries.queryindex(["Taste=That"]))
        assert {_s("ts2")} == await client.timeseries.queryindex(["Taste=That"])

    async def test_uncompressed(self, client: Redis, _s):
        await client.timeseries.create("compressed")
        await client.timeseries.create("uncompressed", encoding=PureToken.UNCOMPRESSED)
        compressed_info = await client.timeseries.info("compressed")
        uncompressed_info = await client.timeseries.info("uncompressed")
        assert compressed_info[_s("memoryUsage")] != uncompressed_info[_s("memoryUsage")]

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        async with client.pipeline(transaction=transaction) as p:
            results = [
                p.timeseries.create("ts"),
                p.timeseries.add("ts", 1, 1),
                p.timeseries.get("ts"),
            ]
        assert await gather(*results) == (True, 1, (1, 1.0))
