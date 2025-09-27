from __future__ import annotations

import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError, ReadOnlyError, RedisError
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_resp2",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_blocking",
    "redis_cluster_raw",
    "valkey",
    "redict",
)
class TestBitmap:
    async def test_bitcount(self, client, _s):
        await client.setbit("a", 5, True)
        assert await client.bitcount("a") == 1
        await client.setbit("a", 6, True)
        assert await client.bitcount("a") == 2
        await client.setbit("a", 5, False)
        assert await client.bitcount("a") == 1
        await client.setbit("a", 9, True)
        await client.setbit("a", 17, True)
        await client.setbit("a", 25, True)
        await client.setbit("a", 33, True)
        assert await client.bitcount("a") == 5
        assert await client.bitcount("a", 0, -1) == 5
        assert await client.bitcount("a", 2, 3) == 2
        assert await client.bitcount("a", 2, -1) == 3
        assert await client.bitcount("a", -2, -1) == 2
        assert await client.bitcount("a", 1, 1) == 1
        with pytest.raises(CommandSyntaxError):
            await client.bitcount("a", 1)

    @pytest.mark.min_server_version("7.0")
    async def test_bitcount_index_unit(self, client, _s):
        await client.setbit("a", 5, True)
        assert await client.bitcount("a", 0, -1, index_unit=PureToken.BIT) == 1
        assert await client.bitcount("a", 0, -1, index_unit=PureToken.BYTE) == 1

    async def test_bitop_not_empty_string(self, client, _s):
        await client.set("a{foo}", "")
        await client.bitop(["a{foo}"], operation="not", destkey="r{foo}")
        assert await client.get("r{foo}") is None

    @pytest.mark.nocluster
    async def test_bitop_not(self, client, _s):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await client.set("a", test_str)
        await client.bitop(["a"], operation="not", destkey="r")
        assert (await client.bitfield("r").get("i32", 0).exc()) == [correct]

    @pytest.mark.nocluster
    async def test_bitop_not_in_place(self, client, _s):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await client.set("a", test_str)
        await client.bitop(["a"], operation="not", destkey="a")
        assert (await client.bitfield("a").get("i32", 0).exc()) == [correct]

    @pytest.mark.nocluster
    async def test_bitop_single_string(self, client, _s):
        test_str = "\x01\x02\xff"
        await client.set("a", test_str)
        await client.bitop(["a"], operation="and", destkey="res1")
        await client.bitop(["a"], operation="or", destkey="res2")
        await client.bitop(["a"], operation="xor", destkey="res3")
        assert await client.get("res1") == _s(test_str)
        assert await client.get("res2") == _s(test_str)
        assert await client.get("res3") == _s(test_str)

    @pytest.mark.nocluster
    async def test_bitop_string_operands(self, client, _s):
        await client.set("a", b"\x01\x02\xff\xff")
        await client.set("b", b"\x01\x02\xff")
        await client.bitop(["a", "b"], operation="and", destkey="res1")
        await client.bitop(["a", "b"], operation="or", destkey="res2")
        await client.bitop(["a", "b"], operation="xor", destkey="res3")
        assert (await client.bitfield("res1").get("i32", 0).exc()) == [0x0102FF00]
        assert (await client.bitfield("res2").get("i32", 0).exc()) == [0x0102FFFF]
        assert (await client.bitfield("res3").get("i32", 0).exc()) == [0x000000FF]

    async def test_bitpos(self, client, _s):
        key = "key:bitpos"
        await client.set(key, b"\xff\xf0\x00")
        assert await client.bitpos(key, 0) == 12
        assert await client.bitpos(key, 0, 2, -1) == 16
        assert await client.bitpos(key, 0, -2, -1) == 12
        await client.set(key, b"\x00\xff\xf0")
        assert await client.bitpos(key, 1, 0) == 8
        assert await client.bitpos(key, 1, 1) == 8
        await client.set(key, b"\x00\x00\x00")
        assert await client.bitpos(key, 1) == -1

    @pytest.mark.min_server_version("7.0")
    async def test_bitpos_unit(self, client, _s):
        key = "key:bitpos"
        await client.set(key, b"\xff\xf0\x00")
        assert await client.bitpos(key, 0, 0, 24, index_unit=PureToken.BIT) == 12
        assert await client.bitpos(key, 0, 0, 3, index_unit=PureToken.BYTE) == 12

    async def test_bitpos_wrong_arguments(self, client, _s):
        key = "key:bitpos:wrong:args"
        await client.set(key, b"\xff\xf0\x00")
        with pytest.raises(RedisError):
            await client.bitpos(key, 0, end=1) == 12
        with pytest.raises(RedisError):
            await client.bitpos(key, 7) == 12

    async def test_bitfield_set(self, client, _s):
        key = "key:bitfield:set"
        assert [0] == await client.bitfield(key).set("i4", "#1", 100).exc()
        assert [4] == await client.bitfield(key).set("i4", "4", 101).exc()

    async def test_bitfield_get(self, client, _s):
        key = "key:bitfield:get"
        await client.set(key, b"\x00d")
        assert [100] == await client.bitfield(key).get("i8", "#1").exc()

    async def test_bitfield_ro_get(self, client, _s):
        key = "key:bitfield_ro:get"
        await client.set(key, b"\x00d")
        assert [100] == await client.bitfield_ro(key).get("i8", "#1").exc()

    async def test_bitfield_ro_set(self, client, _s):
        key = "key:bitfield_ro:set"
        with pytest.raises(ReadOnlyError):
            await client.bitfield_ro(key).set("i4", "#1", 100).exc()

    async def test_bitfield_ro_incrby(self, client, _s):
        key = "key:bitfield_ro:set"
        with pytest.raises(ReadOnlyError):
            await client.bitfield_ro(key).incrby("i8", 0, 128).exc()

    async def test_bitfield_incrby(self, client, _s):
        key = "key:bitfield:incrby"
        await client.bitfield(key).incrby("u2", 10, 1).exc()
        assert await client.get(key) == _s("\x00\x10")
        # overflow
        await client.delete([key])
        assert [-128] == await client.bitfield(key).incrby("i8", 0, 128).exc()

    async def test_bitfield_overflow(self, client, _s):
        key = "key:bitfield:overflow"
        # nothing too happen
        assert not await client.bitfield(key).overflow().exc()
        assert [-128] == await client.bitfield(key).overflow("WRAP").incrby("i8", 0, 128).exc()
        await client.delete([key])
        assert [127] == await client.bitfield(key).overflow("SAT").incrby("i8", 0, 128).exc()
        await client.delete([key])
        assert [None] == await client.bitfield(key).overflow("fail").incrby("i8", 0, 128).exc()

    async def test_get_set_bit(self, client, _s):
        # no value
        assert not await client.getbit("a", 5)
        # set bit 5
        assert not await client.setbit("a", 5, True)
        assert await client.getbit("a", 5)
        # unset bit 4
        assert not await client.setbit("a", 4, False)
        assert not await client.getbit("a", 4)
        # set bit 4
        assert not await client.setbit("a", 4, True)
        assert await client.getbit("a", 4)
        # set bit 5 again
        assert await client.setbit("a", 5, True)
        assert await client.getbit("a", 5)
