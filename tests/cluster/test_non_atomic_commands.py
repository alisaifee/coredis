from __future__ import annotations

import pytest

from coredis.exceptions import RedisClusterError
from tests.conftest import targets

pytestmarks = pytest.mark.asyncio


@pytest.fixture
async def existing_keys(client):
    keys = []
    for k in {"a", "b", "c"}:
        for shard in {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}:
            keys.append(f"{k}{{{shard}}}")
            await client.set(f"{k}{{{shard}}}", 1)
    return tuple(keys)


@pytest.fixture
async def missing_keys(client):
    keys = []
    for k in {"d", "e", "f"}:
        for shard in {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}:
            keys.append(f"{k}{{{shard}}}")
    return tuple(keys)


@targets(
    "redis_cluster",
    "redis_cluster_cached",
)
class TestCommandSplit:
    async def test_delete(self, client, existing_keys, missing_keys):
        assert await client.delete(existing_keys) == 36
        assert await client.delete(missing_keys) == 0
        assert not await client.keys("*")

    async def test_exists(self, client, existing_keys, missing_keys):
        assert await client.exists(existing_keys) == 36
        assert await client.exists(missing_keys) == 0

    async def test_touch(self, client, existing_keys):
        assert await client.touch(existing_keys) == 36

    async def test_unlink(self, client, existing_keys, missing_keys):
        assert await client.unlink(existing_keys) == 36
        assert await client.unlink(missing_keys) == 0
        assert not await client.keys("*")

    async def test_mset_mget(self, client, existing_keys):
        assert await client.mset(dict(zip(existing_keys, existing_keys)))
        assert existing_keys == await client.mget(existing_keys)

    @pytest.mark.min_server_version("8.4.0")
    async def test_msetex(self, client, existing_keys, _s):
        assert await client.msetex(dict(zip(existing_keys, existing_keys)))
        assert tuple(_s(k) for k in existing_keys) == await client.mget(existing_keys)

    async def test_msetnx(self, client, existing_keys, missing_keys, _s):
        assert not await client.msetnx(dict(zip(existing_keys, existing_keys)))
        assert await client.msetnx(dict(zip(missing_keys, missing_keys)))

    @pytest.mark.min_module_version("ReJSON", "2.6.0")
    async def test_json_mset(self, client, missing_keys, _s):
        assert await client.json.mset([(key, ".", {key: i}) for i, key in enumerate(missing_keys)])
        assert [{key: i} for i, key in enumerate(missing_keys)] == await client.json.mget(
            missing_keys, "."
        )


@pytest.mark.parametrize("client_arguments", [{"non_atomic_cross_slot": False}])
@targets(
    "redis_cluster",
    "redis_cluster_cached",
)
class TestCommandSplitDisabled:
    async def test_delete(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            await client.delete(existing_keys)

    async def test_exists(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            await client.exists(existing_keys)

    async def test_touch(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            await client.touch(existing_keys)

    async def test_unlink(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            await client.unlink(existing_keys)

    async def test_mset_mget(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            assert await client.mset(dict(zip(existing_keys, existing_keys)))
        with pytest.raises(RedisClusterError):
            assert existing_keys == await client.mget(existing_keys)

    @pytest.mark.min_server_version("8.4.0")
    async def test_msetex(self, client, existing_keys, client_arguments):
        with pytest.raises(RedisClusterError):
            await client.msetex({"a{a}": 1, "z{z}": 2})
