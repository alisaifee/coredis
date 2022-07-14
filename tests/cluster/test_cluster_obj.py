# python std lib
from __future__ import annotations

import asyncio

# 3rd party imports
from unittest.mock import patch

import pytest

import coredis
from coredis import RedisCluster

# rediscluster imports
from coredis.exceptions import ClusterDownError
from coredis.pool import ClusterConnectionPool

pytestmark = [pytest.mark.asyncio]


class DummyConnectionPool(ClusterConnectionPool):
    pass


class DummyConnection:
    pass


async def test_multi_node_cluster_down_retry(mocker):
    rc = RedisCluster(host="127.0.0.1", port=7000, decode_responses=True)
    e = mocker.patch.object(rc, "execute_command_on_nodes")

    async def raise_cluster_down(*a, **k):
        raise ClusterDownError(b"")

    e.side_effect = raise_cluster_down

    with pytest.raises(ClusterDownError):
        await rc.delete(["fubar{a}", "fubar{b}"])
    assert e.call_count == 3


async def test_single_node_cluster_down_retry(mocker):
    rc = RedisCluster(host="127.0.0.1", port=7000, decode_responses=True)
    await rc.set("fubar{a}", 1)
    e = mocker.patch.object(coredis.pool.cluster.ClusterConnection, "send_command")

    async def raise_cluster_down(*a, **k):
        raise ClusterDownError(b"")

    e.side_effect = raise_cluster_down

    with pytest.raises(ClusterDownError):
        await rc.get("fubar{a}")
    assert e.call_count == 3


async def test_moved_redirection():
    """
    Test that the client handles MOVED response.

    At first call it should return a MOVED ResponseError that will point
    the client to the next server it should talk to.

    Important thing to verify is that it tries to talk to the second node.
    """
    r0 = RedisCluster(host="127.0.0.1", port=7000, decode_responses=True)
    r2 = RedisCluster(host="127.0.0.1", port=7002, decode_responses=True)

    await r0.flushdb()
    await r2.flushdb()

    assert await r0.set("foo", "bar")
    assert await r2.get("foo") == "bar"


async def test_moved_redirection_pipeline(monkeypatch):
    """
    Test that the server handles MOVED response when used in pipeline.

    At first call it should return a MOVED ResponseError that will point
    the client to the next server it should talk to.

    Important thing to verify is that it tries to talk to the second node.
    """
    r0 = RedisCluster(host="127.0.0.1", port=7000, decode_responses=True)
    r2 = RedisCluster(host="127.0.0.1", port=7002, decode_responses=True)
    await r0.flushdb()
    await r2.flushdb()
    p = await r0.pipeline()
    await p.set("foo", "bar")
    assert await p.execute() == (True,)
    assert await r2.get("foo") == "bar"


async def assert_moved_redirection_on_slave(sr, connection_pool_cls, cluster_obj):
    """ """
    # we assume this key is set on 127.0.0.1:7000(7003)
    await sr.set("foo16706", "foo")
    await asyncio.sleep(1)
    with patch.object(connection_pool_cls, "get_node_by_slot") as return_slave_mock:
        return_slave_mock.return_value = {
            "name": "127.0.0.1:7004",
            "host": "127.0.0.1",
            "port": 7004,
            "server_type": "slave",
        }

        master_value = {
            "host": "127.0.0.1",
            "name": "127.0.0.1:7000",
            "port": 7000,
            "server_type": "master",
        }
        with patch.object(
            connection_pool_cls, "get_primary_node_by_slot"
        ) as return_master_mock:
            return_master_mock.return_value = master_value
            assert await cluster_obj.get("foo16706") == "foo"
            assert return_slave_mock.call_count == 1


async def test_moved_redirection_on_slave_with_default_client(sr):
    """
    Test that the client is redirected normally with default
    (readonly_mode=False) client even when we connect always to slave.
    """
    await assert_moved_redirection_on_slave(
        sr,
        ClusterConnectionPool,
        RedisCluster(
            host="127.0.0.1", port=7000, reinitialize_steps=1, decode_responses=True
        ),
    )


@pytest.mark.max_server_version("6.2.0")
async def test_moved_redirection_on_slave_with_readonly_mode_client(sr):
    """
    Ditto with READONLY mode.
    """
    await assert_moved_redirection_on_slave(
        sr,
        ClusterConnectionPool,
        RedisCluster(
            host="127.0.0.1",
            port=7000,
            readonly=True,
            reinitialize_steps=1,
            decode_responses=True,
        ),
    )


async def test_access_correct_slave_with_readonly_mode_client(sr):
    """
    Test that the client can get value normally with readonly mode
    when we connect to correct slave.
    """

    # we assume this key is set on 127.0.0.1:7000(7003)
    await sr.set("foo16706", "foo")
    await asyncio.sleep(1)

    with patch.object(ClusterConnectionPool, "get_node_by_slot") as return_slave_mock:
        return_slave_mock.return_value = {
            "name": "127.0.0.1:7004",
            "host": "127.0.0.1",
            "port": 7004,
            "server_type": "slave",
        }

        master_value = {
            "host": "127.0.0.1",
            "name": "127.0.0.1:7000",
            "port": 7000,
            "server_type": "master",
        }
        with patch.object(
            ClusterConnectionPool, "get_primary_node_by_slot", return_value=master_value
        ):
            readonly_client = RedisCluster(
                host="127.0.0.1", port=7000, readonly=True, decode_responses=True
            )
            assert "foo" == await readonly_client.get("foo16706")
            readonly_client = RedisCluster.from_url(
                url="redis://127.0.0.1:7000/0", readonly=True, decode_responses=True
            )
            assert "foo" == await readonly_client.get("foo16706")


@pytest.mark.parametrize(
    "cluster_remap_keyslots", [("a{fu}", "b{fu}", "c{bar}", "d{bar}")]
)
async def test_slot_moved_redirection(redis_cluster, cluster_remap_keyslots):
    await redis_cluster.set("a{fu}", 1)
    assert "1" == await redis_cluster.get("a{fu}")
    await redis_cluster.set("c{bar}", 2)
    assert "2" == await redis_cluster.get("c{bar}")
    assert 2 == await redis_cluster.exists(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
    assert 2 == await redis_cluster.delete(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
    assert 0 == await redis_cluster.exists(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])


@pytest.mark.parametrize(
    "cluster_remap_keyslots", [("a{fu}", "b{fu}", "c{bar}", "d{bar}")]
)
async def test_slot_moved_redirection_non_atomic_multi_node(
    redis_cluster, cluster_remap_keyslots
):
    assert 0 == await redis_cluster.exists(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
    assert await redis_cluster.set("a{fu}", 1)
    assert 1 == await redis_cluster.exists(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
    assert await redis_cluster.set("c{bar}", 1)
    assert 2 == await redis_cluster.exists(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
    assert 2 == await redis_cluster.delete(["a{fu}", "b{fu}", "c{bar}", "d{bar}"])
