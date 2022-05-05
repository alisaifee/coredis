from __future__ import annotations

import pytest

import coredis
from coredis.exceptions import (
    ConnectionError,
    PrimaryNotFoundError,
    ReplicaNotFoundError,
    TimeoutError,
)
from coredis.sentinel import Sentinel, SentinelConnectionPool

pytest.marks = ("asyncio",)


class SentinelTestClient:
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id

    async def sentinel_masters(self):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)

        return {self.cluster.service_name: self.cluster.primary}

    async def sentinel_replicas(self, primary_name):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)

        if primary_name != self.cluster.service_name:
            return []

        return self.cluster.replicas


class SentinelTestCluster:
    def __init__(
        self, service_name="localhost-redis-sentinel", ip="127.0.0.1", port=6379
    ):
        self.clients = {}
        self.primary = {
            "ip": ip,
            "port": port,
            "is_master": True,
            "is_sdown": False,
            "is_odown": False,
            "num-other-sentinels": 0,
        }
        self.service_name = service_name
        self.replicas = []
        self.nodes_down = set()
        self.nodes_timeout = set()

    def connection_error_if_down(self, node):
        if node.id in self.nodes_down:
            raise ConnectionError

    def timeout_if_down(self, node):
        if node.id in self.nodes_timeout:
            raise TimeoutError

    def client(self, host, port, **kwargs):
        return SentinelTestClient(self, (host, port))


@pytest.fixture()
def cluster(request):
    def teardown():
        coredis.sentinel.Redis = saved_Redis

    cluster = SentinelTestCluster()
    saved_Redis = coredis.sentinel.Redis
    coredis.sentinel.Redis = cluster.client
    request.addfinalizer(teardown)

    return cluster


@pytest.fixture()
def sentinel(request, cluster, event_loop):
    return Sentinel([("foo", 26379), ("bar", 26379)], loop=event_loop)


async def test_discover_primary(sentinel):
    address = await sentinel.discover_primary("localhost-redis-sentinel")
    assert address == ("127.0.0.1", 6379)


async def test_discover_primary_error(sentinel):
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("xxx")


async def test_discover_primary_sentinel_down(cluster, sentinel):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = await sentinel.discover_primary("localhost-redis-sentinel")
    assert address == ("127.0.0.1", 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


async def test_discover_primary_sentinel_timeout(cluster, sentinel):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = await sentinel.discover_primary("localhost-redis-sentinel")
    assert address == ("127.0.0.1", 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


async def test_master_min_other_sentinels(cluster):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("localhost-redis-sentinel")
    cluster.primary["num-other-sentinels"] = 2
    address = await sentinel.discover_primary("localhost-redis-sentinel")
    assert address == ("127.0.0.1", 6379)


async def test_master_odown(cluster, sentinel):
    cluster.primary["is_odown"] = True
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("localhost-redis-sentinel")


async def test_master_sdown(cluster, sentinel):
    cluster.primary["is_sdown"] = True
    with pytest.raises(PrimaryNotFoundError):
        await sentinel.discover_primary("localhost-redis-sentinel")


async def test_discover_replicas(cluster, sentinel):
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == []

    cluster.replicas = [
        {"ip": "replica0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]

    # replica0 -> ODOWN
    cluster.replicas[0]["is_odown"] = True
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == [
        ("replica1", 1234)
    ]

    # replica1 -> SDOWN
    cluster.replicas[1]["is_sdown"] = True
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == []

    cluster.replicas[0]["is_odown"] = False
    cluster.replicas[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert await sentinel.discover_replicas("localhost-redis-sentinel") == [
        ("replica0", 1234),
        ("replica1", 1234),
    ]


async def test_primary_for(redis_sentinel, host_ip):
    primary = redis_sentinel.primary_for("localhost-redis-sentinel")
    assert await primary.ping()
    assert primary.connection_pool.primary_address == (host_ip, 6380)

    # Use internal connection check
    primary = redis_sentinel.primary_for(
        "localhost-redis-sentinel", check_connection=True
    )
    assert await primary.ping()


async def test_replica_for(redis_sentinel):
    replica = redis_sentinel.replica_for("localhost-redis-sentinel")
    assert await replica.ping()


async def test_replica_for_slave_not_found_error(cluster, sentinel):
    cluster.primary["is_odown"] = True
    replica = sentinel.replica_for("localhost-redis-sentinel", db=9)
    with pytest.raises(ReplicaNotFoundError):
        await replica.ping()


async def test_replica_round_robin(cluster, sentinel):
    cluster.replicas = [
        {"ip": "replica0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "replica1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("localhost-redis-sentinel", sentinel)
    rotator = await pool.rotate_replicas()
    assert set(rotator) == {("replica0", 6379), ("replica1", 6379)}


async def test_protocol_version(redis_sentinel_server):
    sentinel = Sentinel(sentinels=[redis_sentinel_server], protocol_version=3)
    assert sentinel.sentinels[0].protocol_version == 3
    assert sentinel.primary_for("localhost-redis-sentinel").protocol_version == 3
    assert sentinel.replica_for("localhost-redis-sentinel").protocol_version == 3


async def test_autodecode(redis_sentinel_server):
    sentinel = Sentinel(sentinels=[redis_sentinel_server], decode_responses=True)
    assert await sentinel.primary_for("localhost-redis-sentinel").ping() == "PONG"
    assert (
        await sentinel.primary_for(
            "localhost-redis-sentinel", decode_responses=False
        ).ping()
        == b"PONG"
    )


async def test_ckquorum(redis_sentinel):
    assert await redis_sentinel.sentinels[0].sentinel_ckquorum(
        "localhost-redis-sentinel"
    )
