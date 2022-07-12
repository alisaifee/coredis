from __future__ import annotations

import pytest

from tests.conftest import targets


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
class TestSet:
    async def test_sadd(self, client, _s):
        members = {"1", "2", "3"}
        await client.sadd("a", members)
        assert await client.smembers("a") == {_s(k) for k in members}

    async def test_scard(self, client, _s):
        await client.sadd("a", [_s("1"), _s("2"), _s("3")])
        assert await client.scard("a") == 3

    async def test_sdiff(self, client, _s):
        await client.sadd("a{foo}", ["1", "2", "3"])
        assert await client.sdiff(["a{foo}", "b{foo}"]) == {_s("1"), _s("2"), _s("3")}
        await client.sadd("b{foo}", ["2", "3"])
        assert await client.sdiff(["a{foo}", "b{foo}"]) == {_s("1")}

    async def test_sdiffstore(self, client, _s):
        await client.sadd("a{foo}", ["1", "2", "3"])
        assert (
            await client.sdiffstore(["a{foo}", "b{foo}"], destination=_s("c{foo}")) == 3
        )
        assert await client.smembers("c{foo}") == {_s("1"), _s("2"), _s("3")}
        await client.sadd("b{foo}", ["2", "3"])
        assert (
            await client.sdiffstore(["a{foo}", "b{foo}"], destination=_s("c{foo}")) == 1
        )
        assert await client.smembers("c{foo}") == {_s("1")}

    async def test_sinter(self, client, _s):
        await client.sadd("a{foo}", ["1", "2", "3"])
        assert await client.sinter(["a{foo}", "b{foo}"]) == set()
        await client.sadd("b{foo}", ["2", "3"])
        assert await client.sinter(["a{foo}", "b{foo}"]) == {_s("2"), _s("3")}

    async def test_sinterstore(self, client, _s):
        await client.sadd("a{foo}", ["1", "2", "3"])
        assert (
            await client.sinterstore(["a{foo}", "b{foo}"], destination=_s("c{foo}"))
            == 0
        )
        assert await client.smembers("c{foo}") == set()
        await client.sadd("b{foo}", ["2", "3"])
        assert (
            await client.sinterstore(["a{foo}", "b{foo}"], destination=_s("c{foo}"))
            == 2
        )
        assert await client.smembers("c{foo}") == {_s("2"), _s("3")}

    @pytest.mark.min_server_version("7.0.0")
    async def test_sintercard(self, client, _s):
        await client.sadd("a{fu}", ["1", "2", "3", "4"])
        await client.sadd("b{fu}", ["3", "4", "5", "6"])
        assert await client.sintercard(["a{fu}", "c{fu}"]) == 0
        assert await client.sintercard(["a{fu}"]) == 4
        assert await client.sintercard(["a{fu}", "b{fu}"]) == 2
        assert await client.sintercard(["a{fu}", "b{fu}"], limit=1) == 1

    async def test_sismember(self, client, _s):
        await client.sadd("a", ["1", "2", "3"])
        assert await client.sismember("a", "1")
        assert await client.sismember("a", "2")
        assert await client.sismember("a", "3")
        assert not await client.sismember("a", "4")

    async def test_smembers(self, client, _s):
        await client.sadd("a", ["1", "2", "3"])
        assert await client.smembers("a") == {_s("1"), _s("2"), _s("3")}

    @pytest.mark.min_server_version("6.2.0")
    async def test_smismember(self, client, _s):
        await client.sadd("a", ["1", "2", "3"])
        result_list = (True, False, True, True)
        assert (await client.smismember("a", ["1", "4", "2", "3"])) == result_list

    async def test_smove(self, client, _s):
        await client.sadd("a{foo}", ["a1", "a2"])
        await client.sadd("b{foo}", ["b1", "b2"])
        assert await client.smove("a{foo}", "b{foo}", "a1")
        assert await client.smembers("a{foo}") == {_s("a2")}
        assert await client.smembers("b{foo}") == {_s("b1"), _s("b2"), _s("a1")}

    async def test_spop(self, client, _s):
        s = ["1", "2", "3"]
        await client.sadd("a", s)
        value = await client.spop("a")
        assert set(await client.smembers("a")) == {_s(m) for m in s} - {value}

    async def test_spop_multi_value(self, client, _s):
        s = ["1", "2", "3"]
        await client.sadd("a", s)
        values = await client.spop("a", 2)
        assert await client.smembers("a") == {_s(m) for m in s} - values

    @pytest.mark.nodragonfly
    async def test_srandmember(self, client, _s):
        s = ["1", "2", "3"]
        await client.sadd("a", s)
        assert await client.srandmember("a") in {_s(m) for m in s}

    @pytest.mark.nodragonfly
    async def test_srandmember_multi_value(self, client, _s):
        s = ["1", "2", "3"]
        await client.sadd("a", s)
        randoms = await client.srandmember("a", count=2)
        assert len(randoms) == 2
        assert set(randoms).intersection({_s(m) for m in s}) == set(randoms)

    async def test_srem(self, client, _s):
        await client.sadd("a", ["1", "2", "3", "4"])
        assert await client.srem("a", ["5"]) == 0
        assert await client.srem("a", ["2", "4"]) == 2
        assert await client.smembers("a") == {_s("1"), _s("3")}

    async def test_sunion(self, client, _s):
        await client.sadd("a{foo}", ["1", "2"])
        await client.sadd("b{foo}", ["2", "3"])
        assert await client.sunion(["a{foo}", "b{foo}"]) == {_s("1"), _s("2"), _s("3")}

    async def test_sunionstore(self, client, _s):
        await client.sadd("a{foo}", ["1", "2"])
        await client.sadd("b{foo}", ["2", "3"])
        assert (
            await client.sunionstore(["a{foo}", "b{foo}"], destination=_s("c{foo}"))
            == 3
        )
        assert await client.smembers("c{foo}") == {_s("1"), _s("2"), _s("3")}

    @pytest.mark.nodragonfly
    async def test_sscan(self, client, _s):
        await client.sadd("a", ["1", "2", "3"])
        cursor, members = await client.sscan("a", count=10)
        assert cursor == 0
        assert set(members) == {_s("1"), _s("2"), _s("3")}
        _, members = await client.sscan("a", match="1")
        assert set(members) == {_s("1")}

    @pytest.mark.nodragonfly
    async def test_sscan_iter(self, client, _s):
        await client.sadd("a", ["1", "2", "3"])
        members = set()
        async for member in client.sscan_iter("a"):
            members.add(member)
        assert members == {_s("1"), _s("2"), _s("3")}
        async for member in client.sscan_iter("a", match="1"):
            assert member == _s("1")
