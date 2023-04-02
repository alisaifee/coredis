from __future__ import annotations

import time

import pytest

from coredis import PureToken, Redis
from tests.conftest import targets


@targets(
    "redis_stack",
    "redis_stack_cluster",
)
class TestTimeseries:
    async def test_create(self, client: Redis):
        assert await client.timeseries.create("ts1")
        assert await client.timeseries.create("ts2", retention=5)
        assert await client.timeseries.create("ts3", labels={"Redis": "Labs"})
        assert await client.timeseries.create(
            "4", retention=20, labels={"Time": "Series"}
        )
        info = await client.timeseries.info("4")
        assert 20 == info["retentionTime"]
        assert "Series" == info["labels"]["Time"]

        # Test for a chunk size of 128 Bytes
        assert await client.timeseries.create("time-serie-1", chunk_size=128)
        info = await client.timeseries.info("time-serie-1")
        assert 128, info["chunkSize"]

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
    async def test_create_duplicate_policy(self, client: Redis, duplicate_policy):
        # Test for duplicate policy
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert await client.timeseries.create(
            ts_name, duplicate_policy=duplicate_policy
        )
        info = await client.timeseries.info(ts_name)
        assert duplicate_policy == info["duplicatePolicy"]

    async def test_alter(self, client: Redis):
        assert await client.timeseries.create("ts1")
        res = await client.timeseries.info("ts1")
        assert 0 == res["retentionTime"]
        assert await client.timeseries.alter("ts1", retention=10)
        res = await client.timeseries.info("ts1")
        assert {} == res["labels"]
        res = await client.timeseries.info("ts1")
        assert 10 == res["retentionTime"]
        assert await client.timeseries.alter("ts1", labels={"Time": "Series"})
        res = await client.timeseries.info("ts1")
        assert "Series" == res["labels"]["Time"]
        res = await client.timeseries.info("ts1")
        assert 10 == res["retentionTime"]

    async def test_alter_diplicate_policy(self, client: Redis):
        assert await client.timeseries.create("ts1")
        info = await client.timeseries.info("ts1")
        assert info["duplicatePolicy"] is None
        assert await client.timeseries.alter("ts1", duplicate_policy=PureToken.MIN)
        info = await client.timeseries.info("ts1")
        assert "min" == info["duplicatePolicy"]

    async def test_add(self, client: Redis):
        assert 1 == await client.timeseries.add("ts1", 1, 1)
        assert 2 == await client.timeseries.add("ts2", 2, 3, retention=10)
        assert 3 == await client.timeseries.add("ts3", 3, 2, labels={"Redis": "Labs"})
        assert 4 == await client.timeseries.add(
            "4", 4, 2, retention=10, labels={"Redis": "Labs", "Time": "Series"}
        )
        res = await client.timeseries.add("ts5", "*", 1)
        assert abs(time.time() - round(float(res) / 1000)) < 1.0

        info = await client.timeseries.info("4")
        assert 10 == info["retentionTime"]
        assert "Labs" == info["labels"]["Redis"]

        # Test for a chunk size of 128 Bytes on TS.ADD
        assert await client.timeseries.add("time-serie-1", 1, 10.0, chunk_size=128)
        info = await client.timeseries.info("time-serie-1")
        assert 128 == info["chunkSize"]

    async def test_add_duplicate_policy(self, client: Redis):
        # Test for duplicate policy BLOCK
        assert 1 == await client.timeseries.add("time-serie-add-ooo-block", 1, 5.0)
        with pytest.raises(Exception):
            await client.timeseries.add(
                "time-serie-add-ooo-block", 1, 5.0, duplicate_policy=PureToken.BLOCK
            )

        # Test for duplicate policy LAST
        assert 1 == await client.timeseries.add("time-serie-add-ooo-last", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "time-serie-add-ooo-last", 1, 10.0, duplicate_policy=PureToken.LAST
        )
        res = await client.timeseries.get("time-serie-add-ooo-last")
        assert 10.0 == res[1]

        # Test for duplicate policy FIRST
        assert 1 == await client.timeseries.add("time-serie-add-ooo-first", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "time-serie-add-ooo-first", 1, 10.0, duplicate_policy=PureToken.FIRST
        )
        res = await client.timeseries.get("time-serie-add-ooo-first")
        assert 5.0 == res[1]

        # Test for duplicate policy MAX
        assert 1 == await client.timeseries.add("time-serie-add-ooo-max", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "time-serie-add-ooo-max", 1, 10.0, duplicate_policy=PureToken.MAX
        )
        res = await client.timeseries.get("time-serie-add-ooo-max")
        assert 10.0 == res[1]

        # Test for duplicate policy MIN
        assert 1 == await client.timeseries.add("time-serie-add-ooo-min", 1, 5.0)
        assert 1 == await client.timeseries.add(
            "time-serie-add-ooo-min", 1, 10.0, duplicate_policy=PureToken.MIN
        )
        res = await client.timeseries.get("time-serie-add-ooo-min")
        assert 5.0 == res[1]

    async def test_madd(self, client: Redis):
        await client.timeseries.create("a")
        assert (1, 2, 3) == await client.timeseries.madd(
            [("a", 1, 5), ("a", 2, 10), ("a", 3, 15)]
        )

    async def test_incrby_decrby(self, client: Redis):
        for _ in range(100):
            assert await client.timeseries.incrby("ts1", 1)
            time.sleep(0.001)
        assert 100 == (await client.timeseries.get("ts1"))[1]
        for _ in range(100):
            assert await client.timeseries.decrby("ts1", 1)
            time.sleep(0.001)
        assert 0 == (await client.timeseries.get("ts1"))[1]

        assert await client.timeseries.incrby("ts2", 1.5, timestamp=5)
        assert (5, 1.5) == await client.timeseries.get("ts2")
        assert await client.timeseries.incrby("ts2", 2.25, timestamp=7)
        assert (7, 3.75) == await client.timeseries.get("ts2")
        assert await client.timeseries.decrby("ts2", 1.5, timestamp=15)
        assert (15, 2.25) == await client.timeseries.get("ts2")

        # Test for a chunk size of 128 Bytes on TS.INCRBY
        assert await client.timeseries.incrby("time-serie-1", 10, chunk_size=128)
        info = await client.timeseries.info("time-serie-1")
        assert 128 == info["chunkSize"]

        # Test for a chunk size of 128 Bytes on TS.DECRBY
        assert await client.timeseries.decrby("time-serie-2", 10, chunk_size=128)
        info = await client.timeseries.info("time-serie-2")
        assert 128 == info["chunkSize"]

    async def test_create_and_delete_rule(self, client: Redis):
        # test rule creation
        time = 100
        await client.timeseries.create("ts1{a}")
        await client.timeseries.create("ts2{a}")
        await client.timeseries.createrule("ts1{a}", "ts2{a}", PureToken.AVG, 100)
        for i in range(50):
            await client.timeseries.add("ts1{a}", time + i * 2, 1)
            await client.timeseries.add("ts1{a}", time + i * 2 + 1, 2)
        await client.timeseries.add("ts1{a}", time * 2, 1.5)
        assert round((await client.timeseries.get("ts2{a}"))[1], 5) == 1.5
        info = await client.timeseries.info("ts1{a}")
        assert info["rules"][0][1] == 100

        # test rule deletion
        await client.timeseries.deleterule("ts1{a}", "ts2{a}")
        info = await client.timeseries.info("ts1{a}")
        assert not info["rules"]

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
        assert 10 == len(await client.timeseries.range("ts1", 0, 500, count=10))

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

    async def test_rev_range(self, client: Redis):
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
        assert 10 == len(await client.timeseries.revrange("ts1", 0, 500, count=10))
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

    async def test_multi_range(self, client: Redis):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"])
        assert 2 == len(res)
        assert 100 == len(res["ts1"][1])

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res["ts1"][1])

        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        res = await client.timeseries.mrange(
            0, 500, filters=["Test=This"], aggregator=PureToken.AVG, bucketduration=10
        )
        assert 2 == len(res)
        assert 20 == len(res["ts1"][1])

        # test withlabels
        assert {} == res["ts1"][0]
        res = await client.timeseries.mrange(
            0, 200, filters=["Test=This"], withlabels=True
        )
        assert {"Test": "This", "team": "ny"} == res["ts1"][0]

    async def test_multi_range_filter_align(self, client: Redis):
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
        assert {"team": "ny"} == res["ts1"][0]
        assert {"team": "sf"} == res["ts2"][0]

        # test with filterby
        res = await client.timeseries.mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            min_value=1,
            max_value=2,
        )
        assert ((15, 1.0), (16, 2.0)) == res["ts1"][1]

        # test align
        res = await client.timeseries.mrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align="-",
        )
        assert ((0, 10.0), (10, 1.0)) == res["ts1"][1]
        res = await client.timeseries.mrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align=5,
        )
        assert ((0, 5.0), (5, 6.0)) == res["ts1"][1]

    @pytest.mark.nocluster
    async def test_multi_range_grouped(self, client: Redis):
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
        assert ((0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)) == res["Test=This"][1]
        res = await client.timeseries.mrange(
            0,
            3,
            filters=["Test=This"],
            groupby="Test",
            reducer=PureToken.MAX,
        )
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res["Test=This"][1]
        res = await client.timeseries.mrange(
            0,
            3,
            filters=["Test=This"],
            groupby="team",
            reducer=PureToken.MIN,
        )
        assert 2 == len(res)
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res["team=ny"][1]
        assert ((0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)) == res["team=sf"][1]

    async def test_multi_reverse_range(self, client: Redis):
        await client.timeseries.create("ts1", labels={"Test": "This", "team": "ny"})
        await client.timeseries.create(
            "ts2", labels={"Test": "This", "Taste": "That", "team": "sf"}
        )
        for i in range(100):
            await client.timeseries.add("ts1", i, i % 7)
            await client.timeseries.add("ts2", i, i % 11)

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"])
        assert 2 == len(res)
        assert 100 == len(res["ts1"][1])

        res = await client.timeseries.mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res["ts1"][1])

        for i in range(100):
            await client.timeseries.add("ts1", i + 200, i % 7)
        res = await client.timeseries.mrevrange(
            0, 500, filters=["Test=This"], aggregator=PureToken.AVG, bucketduration=10
        )
        assert 2 == len(res)
        assert 20 == len(res["ts1"][1])
        assert {} == res["ts1"][0]

        # test withlabels
        res = await client.timeseries.mrevrange(
            0, 200, filters=["Test=This"], withlabels=True
        )
        assert {"Test": "This", "team": "ny"} == res["ts1"][0]

        # test with selected labels
        res = await client.timeseries.mrevrange(
            0, 200, filters=["Test=This"], selected_labels=["team"]
        )
        assert {"team": "ny"} == res["ts1"][0]
        assert {"team": "sf"} == res["ts2"][0]

        # test filterby
        res = await client.timeseries.mrevrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            min_value=1,
            max_value=2,
        )
        assert ((16, 2.0), (15, 1.0)) == res["ts1"][1]

        # test align
        res = await client.timeseries.mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align="-",
        )
        assert ((10, 1.0), (0, 10.0)) == res["ts1"][1]
        res = await client.timeseries.mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregator=PureToken.COUNT,
            bucketduration=10,
            align=1,
        )
        assert ((1, 10.0), (0, 1.0)) == res["ts1"][1]

    @pytest.mark.nocluster
    async def test_multi_reverse_range_grouped(self, client: Redis):
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
        assert ((3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)) == res["Test=This"][1]
        res = await client.timeseries.mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reducer=PureToken.MAX
        )
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res["Test=This"][1]
        res = await client.timeseries.mrevrange(
            0, 3, filters=["Test=This"], groupby="team", reducer=PureToken.MIN
        )
        assert 2 == len(res)
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res["team=ny"][1]
        assert ((3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)) == res["team=sf"][1]

    async def test_get(self, client: Redis):
        name = "test"
        await client.timeseries.create(name)
        assert not await client.timeseries.get(name)
        await client.timeseries.add(name, 2, 3)
        assert (2, 3.0) == (await client.timeseries.get(name))
        await client.timeseries.add(name, 3, 4.1)
        assert (3, 4.1) == (await client.timeseries.get(name))

    async def test_mget(self, client: Redis):
        await client.timeseries.create("ts1", labels={"Test": "This"})
        await client.timeseries.create("ts2", labels={"Test": "This", "Taste": "That"})
        act_res = await client.timeseries.mget(["Test=This"])
        exp_res = {"ts1": ({}, ()), "ts2": ({}, ())}
        assert act_res == exp_res
        await client.timeseries.add("ts1", "*", 15)
        await client.timeseries.add("ts2", "*", 25)
        res = await client.timeseries.mget(["Test=This"])
        assert 15 == res["ts1"][1][1]
        assert 25 == res["ts2"][1][1]
        res = await client.timeseries.mget(["Taste=That"])
        assert 25 == res["ts2"][1][1]

        # test withlabels
        assert {} == res["ts2"][0]
        res = await client.timeseries.mget(["Taste=That"], withlabels=True)
        assert {"Taste": "That", "Test": "This"} == res["ts2"][0]

    async def test_info(self, client: Redis):
        await client.timeseries.create(
            "ts1", retention=5, labels={"currentLabel": "currentData"}
        )
        info = await client.timeseries.info("ts1")
        assert 5 == info["retentionTime"]
        assert info["labels"]["currentLabel"] == "currentData"
        await client.timeseries.add("ts1", 0, 1)

        info = await client.timeseries.info("ts1", debug=True)
        chunks = info["Chunks"]
        assert chunks[0]["startTimestamp"] == chunks[0]["endTimestamp"] == 0

    async def test_info_duplicate_policy(self, client: Redis):
        await client.timeseries.create(
            "ts1", retention=5, labels={"currentLabel": "currentData"}
        )
        info = await client.timeseries.info("ts1")
        assert info["duplicatePolicy"] is None

        await client.timeseries.create("time-serie-2", duplicate_policy=PureToken.MIN)
        info = await client.timeseries.info("time-serie-2")
        assert "min" == info["duplicatePolicy"]

    async def test_query_index(self, client: Redis):
        await client.timeseries.create("ts1", labels={"Test": "This"})
        await client.timeseries.create("ts2", labels={"Test": "This", "Taste": "That"})
        assert 2 == len(await client.timeseries.queryindex(["Test=This"]))
        assert 1 == len(await client.timeseries.queryindex(["Taste=That"]))
        assert {"ts2"} == await client.timeseries.queryindex(["Taste=That"])

    async def test_uncompressed(self, client: Redis):
        await client.timeseries.create("compressed")
        await client.timeseries.create("uncompressed", encoding=PureToken.UNCOMPRESSED)
        compressed_info = await client.timeseries.info("compressed")
        uncompressed_info = await client.timeseries.info("uncompressed")
        assert compressed_info["memoryUsage"] != uncompressed_info["memoryUsage"]
