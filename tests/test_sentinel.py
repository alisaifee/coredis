from __future__ import annotations

import pytest
from exceptiongroup import ExceptionGroup

import coredis
from coredis.exceptions import (
    PrimaryNotFoundError,
    ReadOnlyError,
    ReplicaNotFoundError,
    ResponseError,
)
from coredis.sentinel import Sentinel, SentinelConnectionPool
from tests.conftest import targets


async def test_init_compose_sentinel(redis_sentinel: Sentinel):
    master = redis_sentinel.primary_for("mymaster")
    async with master:
        await master.ping()


async def test_discover_primary(redis_sentinel: Sentinel):
    address = await redis_sentinel.discover_primary("mymaster")
    assert address == ("127.0.0.1", 6379)


async def test_discover_primary_error(sentinel):
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("xxx")


async def test_discover_primary_sentinel_down(cluster, sentinel: Sentinel):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = await sentinel.discover_primary("mymaster")
    assert address == ("127.0.0.1", 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


async def test_discover_primary_sentinel_timeout(cluster, sentinel: Sentinel):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = await sentinel.discover_primary("mymaster")
    assert address == ("127.0.0.1", 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


async def test_master_min_other_sentinels(cluster):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("mymaster")
    cluster.primary["num-other-sentinels"] = 2
    address = await sentinel.discover_primary("mymaster")
    assert address == ("127.0.0.1", 6379)


async def test_master_odown(cluster, sentinel):
    cluster.primary["is_odown"] = True
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("mymaster")


async def test_master_sdown(cluster, sentinel):
    cluster.primary["is_sdown"] = True
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("mymaster")


async def test_discover_replicas(cluster, sentinel):
    assert await sentinel.discover_replicas("mymaster") == []

    cluster.replicas = [
        {"ip": "replica0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert await sentinel.discover_replicas("mymaster") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]

    # replica0 -> ODOWN
    cluster.replicas[0]["is_odown"] = True
    assert await sentinel.discover_replicas("mymaster") == [("replica1", 1234)]

    # replica1 -> SDOWN
    cluster.replicas[1]["is_sdown"] = True
    assert await sentinel.discover_replicas("mymaster") == []

    cluster.replicas[0]["is_odown"] = False
    cluster.replicas[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert await sentinel.discover_replicas("mymaster") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert await sentinel.discover_replicas("mymaster") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]


async def test_replica_for_slave_not_found_error(cluster, sentinel: Sentinel):
    cluster.primary["is_odown"] = True
    replica = sentinel.replica_for("mymaster", db=9)
    async with replica:
        with pytest.raises(ReplicaNotFoundError):
            await replica.ping()


async def test_replica_round_robin(cluster, sentinel):
    cluster.replicas = [
        {"ip": "replica0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    async for rotator in pool.rotate_replicas():
        assert rotator in {("replica0", 6379), ("replica1", 6379)}


async def test_autodecode(redis_sentinel_server: tuple[str, int]):
    sentinel = Sentinel(sentinels=[redis_sentinel_server], decode_responses=True)
    client = sentinel.primary_for("mymaster")
    async with client:
        assert await client.ping() == "PONG"
    client = sentinel.primary_for("mymaster", decode_responses=False)
    async with client:
        assert await client.ping() == b"PONG"


@targets("redis_sentinel", "redis_sentinel_raw", "redis_sentinel_resp2")
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
        with pytest.raises(ExceptionGroup) as group:
            async with p:
                await p.ping()
        assert isinstance(group._excinfo[1].exceptions[0], ReplicaNotFoundError)

    async def test_write_to_replica(self, client):
        p = client.replica_for("mymaster")
        async with p:
            await p.ping()
            with pytest.raises(ReadOnlyError):
                await p.set("fubar", 1)

    @pytest.mark.parametrize("client_arguments", [{"cache": coredis.cache.NodeTrackingCache()}])
    async def test_sentinel_cache(self, client: Sentinel, client_arguments, mocker, _s):
        primary = client.primary_for("mymaster")
        async with primary:
            await primary.set("fubar", 1)
            assert await primary.get("fubar") == _s("1")

        new_primary = client.primary_for("mymaster")
        new_replica = client.replica_for("mymaster")

        assert new_primary.cache
        assert new_replica.cache

        async with new_primary, new_replica:
            await new_primary.ping()
            await new_replica.ping()

            replica_spy = mocker.spy(coredis.BaseConnection, "create_request")

            assert await new_primary.get("fubar") == _s("1")
            assert await new_replica.get("fubar") == _s("1")

            assert replica_spy.call_count == 0

    @pytest.mark.xfail
    async def test_replication(self, client: Sentinel):
        primary = client.primary_for("mymaster")
        async with primary:
            with primary.ensure_replication(1):
                await primary.set("fubar", 1)

            with primary.ensure_replication(2):
                await primary.set("fubar", 1)

        replica = client.replica_for("mymaster")
        with pytest.raises(ResponseError):
            async with replica:
                with replica.ensure_replication(2):
                    await replica.set("fubar", 1)
