from __future__ import annotations

import pytest

from coredis import RedisClusterException
from tests.conftest import targets

pytestmarks = pytest.mark.asyncio


@pytest.fixture
async def cross_slot_keys(client):
    for k in {"a", "b", "c"}:
        for shard in {"a", "b", "c"}:
            await client.set(f"{k}{{{shard}}}", 1)
    keys = await client.keys("*")
    for k in {"d", "e", "f"}:
        for shard in {"a", "b", "c"}:
            keys.add(f"{k}{{{shard}}}")
    return keys


@pytest.mark.parametrize("client_arguments", [({"non_atomic_cross_slot": True})])
@targets(
    "redis_cluster",
)
class TestCommandSplit:
    async def test_delete(self, client, cross_slot_keys, client_arguments):
        assert await client.delete(cross_slot_keys) == 9
        assert not await client.keys("*")

    async def test_exists(self, client, cross_slot_keys, client_arguments):
        assert await client.exists(cross_slot_keys) == 9

    async def test_touch(self, client, cross_slot_keys, client_arguments):
        assert await client.touch(cross_slot_keys) == 9

    async def test_unlink(self, client, cross_slot_keys, client_arguments):
        assert await client.unlink(cross_slot_keys) == 9
        assert not await client.keys("*")


@pytest.mark.parametrize("client_arguments", [({"non_atomic_cross_slot": False})])
@targets(
    "redis_cluster",
)
class TestCommandSplitDisabled:
    async def test_delete(self, client, cross_slot_keys, client_arguments):
        with pytest.raises(RedisClusterException):
            assert await client.delete(cross_slot_keys) == 9

    async def test_exists(self, client, cross_slot_keys, client_arguments):
        with pytest.raises(RedisClusterException):
            assert await client.exists(cross_slot_keys) == 9

    async def test_touch(self, client, cross_slot_keys, client_arguments):
        with pytest.raises(RedisClusterException):
            assert await client.touch(cross_slot_keys) == 9

    async def test_unlink(self, client, cross_slot_keys, client_arguments):
        with pytest.raises(RedisClusterException):
            assert await client.unlink(cross_slot_keys) == 9
