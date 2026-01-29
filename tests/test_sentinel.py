from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import coredis
from coredis.exceptions import (
    PrimaryNotFoundError,
    ReadOnlyError,
    ReplicaNotFoundError,
    ReplicationError,
)
from coredis.sentinel import Sentinel, SentinelConnectionPool
from tests.conftest import targets


async def test_init_compose_sentinel(redis_sentinel: Sentinel):
    master = redis_sentinel.primary_for("mymaster")
    async with master:
        await master.ping()


async def test_discover_primary(redis_sentinel: Sentinel, host_ip):
    address = await redis_sentinel.discover_primary("mymaster")
    assert address == (host_ip, 6380)


async def test_discover_primary_error(redis_sentinel: Sentinel, mocker):
    with pytest.raises(PrimaryNotFoundError):
        await redis_sentinel.discover_primary("xxx")
    sentinel_masters = mocker.patch.object(
        redis_sentinel.sentinels[0], "sentinel_masters", new_callable=AsyncMock
    )
    sentinel_masters.return_value = {
        "mymaster": {
            "ip": "127.0.0.1",
            "port": 6380,
            "is_master": True,
            "is_sdown": True,
            "is_odown": True,
        }
    }
    async with redis_sentinel.primary_for("mymaster") as primary:
        with pytest.raises(PrimaryNotFoundError):
            await primary.ping()


async def test_replica_for_slave_not_found_error(redis_sentinel: Sentinel, mocker):
    sentinel_replicas = mocker.patch.object(
        redis_sentinel.sentinels[0], "sentinel_replicas", new_callable=AsyncMock
    )
    sentinel_masters = mocker.patch.object(
        redis_sentinel.sentinels[0], "sentinel_masters", new_callable=AsyncMock
    )
    sentinel_replicas.return_value = []
    sentinel_masters.return_value = {}
    replica = redis_sentinel.replica_for("mymaster", db=9)
    async with replica:
        with pytest.raises(ReplicaNotFoundError):
            await replica.ping()


async def test_replica_round_robin(redis_sentinel: Sentinel, mocker, host_ip):
    pool = SentinelConnectionPool("mymaster", redis_sentinel)
    sentinel_replicas = mocker.patch.object(
        redis_sentinel.sentinels[0], "sentinel_replicas", new_callable=AsyncMock
    )
    sentinel_replicas.return_value = [
        {"ip": "replica0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    async for rotator in pool.rotate_replicas():
        assert rotator in {("replica0", 6379), ("replica1", 6379)}
    sentinel_replicas.return_value = [
        {"ip": "replica0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 6379, "is_odown": False, "is_sdown": True},
    ]
    async for rotator in pool.rotate_replicas():
        assert rotator in {("replica0", 6379)}


async def test_autodecode(redis_sentinel_server: tuple[str, int]):
    sentinel = Sentinel(sentinels=[redis_sentinel_server], decode_responses=True)
    async with sentinel:
        client = sentinel.primary_for("mymaster")
        async with client:
            assert await client.ping() == "PONG"
        client = sentinel.primary_for("mymaster", decode_responses=False)
        async with client:
            assert await client.ping() == b"PONG"


@targets("redis_sentinel", "redis_sentinel_raw")
class TestSentinelCommand:
    async def test_primary_for(self, client: Sentinel, host_ip):
        primary = client.primary_for("mymaster")
        async with primary:
            assert await primary.ping()
            assert primary.connection_pool.primary_address == (host_ip, 6380)

        # Use internal connection check
        primary = client.primary_for("mymaster", check_connection=True)
        async with primary:
            assert await primary.ping()

    async def test_replica_for(self, client):
        replica = client.replica_for("mymaster")
        async with replica:
            assert await replica.ping()

    async def test_ckquorum(self, client):
        assert await client.sentinels[0].sentinel_ckquorum("mymaster")

    async def test_sentinel_config_get(self, client, _s):
        configs = await client.sentinels[0].sentinel_config_get("*")
        assert configs[_s("resolve-hostnames")] == _s("yes")

    async def test_sentinel_config_set(self, client, _s):
        await client.sentinels[0].sentinel_config_set("resolve-hostnames", "no")
        configs = await client.sentinels[0].sentinel_config_get("*")
        assert configs[_s("resolve-hostnames")] == _s("no")

    async def test_master_address_by_name(self, client):
        master_address = await client.sentinels[0].sentinel_get_master_addr_by_name("mymaster")
        assert master_address == await client.discover_primary("mymaster")

    async def test_failover(self, client, mocker):
        mock_exec = mocker.patch.object(client.sentinels[0], "execute_command", autospec=True)
        mock_exec.return_value = True
        assert await client.sentinels[0].sentinel_failover("mymaster")
        assert mock_exec.call_args[0][0].arguments[0] == "mymaster"

    async def test_flush_config(self, client):
        assert await client.sentinels[0].sentinel_flushconfig()

    async def test_role(self, client: Sentinel):
        assert (await client.sentinels[0].role()).role == "sentinel"
        primary = client.primary_for("mymaster")
        replica = client.replica_for("mymaster")
        async with primary, replica:
            assert (await primary.role()).role == "master"
            assert (await replica.role()).role == "slave"

    async def test_infocache(self, client, _s):
        assert await client.sentinels[0].sentinel_flushconfig()
        info_cache = await client.sentinels[0].sentinel_infocache("mymaster")
        roles = {info["role"] for info in list(info_cache[_s("mymaster")].values())}
        assert {"master", "slave"} & roles

    async def test_sentinel_master(self, client):
        assert (await client.sentinels[0].sentinel_master("mymaster"))["is_master"]

    async def test_sentinel_masters(self, client):
        assert (await client.sentinels[0].sentinel_masters())["mymaster"]["is_master"]

    async def test_sentinel_replicas(self, client):
        assert not any(
            [k["is_master"] for k in (await client.sentinels[0].sentinel_replicas("mymaster"))]
        )

    async def test_no_replicas(self, client: Sentinel, mocker):
        p = client.replica_for("mymaster")
        replica_rotate = mocker.patch.object(p.connection_pool, "rotate_replicas")

        async def async_iter(items):
            for item in items:
                yield item

        replica_rotate.return_value = async_iter([])
        async with p:
            with pytest.raises(ReplicaNotFoundError):
                await p.ping()

    async def test_write_to_replica(self, client):
        p = client.replica_for("mymaster")
        async with p:
            await p.ping()
            with pytest.raises(ReadOnlyError):
                await p.set("fubar", 1)

    @pytest.mark.parametrize("client_arguments", [{"cache": coredis.cache.LRUCache()}])
    async def test_sentinel_cache(self, client: Sentinel, client_arguments, mocker, _s):
        primary = client.primary_for("mymaster")
        async with primary:
            await primary.set("fubar", 1)
            assert await primary.get("fubar") == _s("1")

        new_primary = client.primary_for("mymaster")
        new_replica = client.replica_for("mymaster")

        async with new_primary, new_replica:
            await new_primary.ping()
            await new_replica.ping()

            assert await new_primary.get("fubar") == _s("1")
            create_request_spy = mocker.spy(coredis.BaseConnection, "create_request")
            assert await new_replica.get("fubar") == _s("1")
            assert create_request_spy.call_count == 0

    async def test_replication(self, client: Sentinel):
        primary = client.primary_for("mymaster")
        async with primary:
            with primary.ensure_replication(1):
                await primary.set("fubar", 1)

            with pytest.raises(ReplicationError):
                with primary.ensure_replication(2):
                    await primary.set("fubar", 1)
