# python std lib
from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, Mock, patch

# 3rd party imports
import pytest

# rediscluster imports
from coredis.client import Redis
from coredis.credentials import UserPassCredentialProvider
from coredis.exceptions import ConnectionError, RedisClusterException, RedisError
from coredis.pool.nodemanager import HASH_SLOTS, ManagedNode, NodeManager


async def test_init_slots_cache_not_all_slots(s, redis_cluster):
    """
    Test that if not all slots are covered it should raise an exception
    """

    with patch.object(NodeManager, "get_redis_link") as get_redis_link:
        cluster_slots_async = asyncio.Future()
        cluster_slots = {
            (0, 5459): [
                {
                    "host": "127.0.0.1",
                    "port": 7000,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "master",
                },
                {
                    "host": "127.0.0.1",
                    "port": 7003,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "slave",
                },
            ],
            (5461, 10922): [
                {
                    "host": "127.0.0.1",
                    "port": 7001,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "master",
                },
                {
                    "host": "127.0.0.1",
                    "port": 7004,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "slave",
                },
            ],
            (10923, 16383): [
                {
                    "host": "127.0.0.1",
                    "port": 7002,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "master",
                },
                {
                    "host": "127.0.0.1",
                    "port": 7005,
                    "node_id": str(uuid.uuid4()),
                    "server_type": "slave",
                },
            ],
        }
        mock_redis = Mock()
        cluster_slots_async.set_result(cluster_slots)
        mock_redis.cluster_slots.return_value = cluster_slots_async

        config_get_async = asyncio.Future()
        config_get_async.set_result({"cluster-require-full-coverage": "yes"})

        mock_redis.config_get.return_value = config_get_async

        get_redis_link.return_value = mock_redis
        with pytest.raises(RedisClusterException) as ex:
            await s.connection_pool.initialize()

        assert str(ex.value).startswith("Not all slots are covered after query all startup_nodes.")


async def test_init_slots_cache_not_all_slots_not_require_full_coverage(s, redis_cluster):
    """
    Test that if not all slots are covered it should raise an exception
    """
    with patch.object(Redis, "cluster_slots", new_callable=AsyncMock) as mock_cluster_slots:
        with patch.object(Redis, "config_get", new_callable=AsyncMock) as mock_config_get:
            mock_config_get.return_value = {"cluster-require-full-coverage": "no"}
            mock_cluster_slots.return_value = {
                (0, 5459): [
                    {
                        "host": "127.0.0.1",
                        "port": 7000,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "master",
                    },
                    {
                        "host": "127.0.0.1",
                        "port": 7003,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "slave",
                    },
                ],
                (5461, 10922): [
                    {
                        "host": "127.0.0.1",
                        "port": 7001,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "master",
                    },
                    {
                        "host": "127.0.0.1",
                        "port": 7004,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "slave",
                    },
                ],
                (10923, 16383): [
                    {
                        "host": "127.0.0.1",
                        "port": 7002,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "master",
                    },
                    {
                        "host": "127.0.0.1",
                        "port": 7005,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "slave",
                    },
                ],
            }

            await s.connection_pool.nodes.initialize()
            assert 5460 not in s.connection_pool.nodes.slots


async def test_init_slots_cache(s, redis_cluster):
    """
    Test that slots cache can in initialized and all slots are covered
    """
    good_slots_resp = {
        (0, 5460): [
            {
                "host": "127.0.0.1",
                "port": 7000,
                "node_id": str(uuid.uuid4()),
                "server_type": "master",
            },
            {
                "host": "127.0.0.1",
                "port": 7003,
                "node_id": str(uuid.uuid4()),
                "server_type": "slave",
            },
        ],
        (5461, 10922): [
            {
                "host": "127.0.0.1",
                "port": 7001,
                "node_id": str(uuid.uuid4()),
                "server_type": "master",
            },
            {
                "host": "127.0.0.1",
                "port": 7004,
                "node_id": str(uuid.uuid4()),
                "server_type": "slave",
            },
        ],
        (10923, 16383): [
            {
                "host": "127.0.0.1",
                "port": 7002,
                "node_id": str(uuid.uuid4()),
                "server_type": "master",
            },
            {
                "host": "127.0.0.1",
                "port": 7005,
                "node_id": str(uuid.uuid4()),
                "server_type": "slave",
            },
        ],
    }

    with patch.object(Redis, "config_get", new_callable=AsyncMock) as mock_config_get:
        with patch.object(Redis, "cluster_slots", new_callable=AsyncMock) as mock_cluster_slots:
            mock_cluster_slots.return_value = good_slots_resp
            mock_config_get.return_value = {"cluster-require-full-coverage": "yes"}

            await s.connection_pool.nodes.initialize()
            assert len(s.connection_pool.nodes.slots) == HASH_SLOTS

            for slot_info, node_info in good_slots_resp.items():
                all_hosts = ["127.0.0.1", "127.0.0.2"]
                all_ports = [7000, 7001, 7002, 7003, 7004, 7005]
                slot_start = slot_info[0]
                slot_end = slot_info[1]

                for i in range(slot_start, slot_end + 1):
                    assert len(s.connection_pool.nodes.slots[i]) == len(node_info)
                    assert s.connection_pool.nodes.slots[i][0].host in all_hosts
                    assert s.connection_pool.nodes.slots[i][1].host in all_hosts
                    assert s.connection_pool.nodes.slots[i][0].port in all_ports
                    assert s.connection_pool.nodes.slots[i][1].port in all_ports

        assert len(s.connection_pool.nodes.nodes) == 6


async def test_empty_startup_nodes():
    """
    It should not be possible to create a node manager with no nodes specified
    """
    with pytest.raises(RedisClusterException):
        await NodeManager().initialize()

    with pytest.raises(RedisClusterException):
        await NodeManager([]).initialize()


async def test_all_nodes(redis_cluster):
    """
    Set a list of nodes and it should be possible to iterate over all
    """
    n = NodeManager(startup_nodes=[{"host": "127.0.0.1", "port": 7000}])
    await n.initialize()

    nodes = [node for node in n.nodes.values()]

    for i, node in enumerate(n.all_nodes()):
        assert node in nodes


async def test_all_nodes_primaries(redis_cluster):
    """
    Set a list of nodes with random primary/replica config and it shold be possible
    to iterate over all of them.
    """
    n = NodeManager(
        startup_nodes=[
            {"host": "127.0.0.1", "port": 7000},
            {"host": "127.0.0.1", "port": 7001},
        ]
    )
    await n.initialize()

    nodes = [node for node in n.nodes.values() if node.server_type == "primary"]

    for node in n.all_primaries():
        assert node in nodes


async def test_cluster_slots_error(redis_cluster):
    """
    Check that exception is raised if initialize can't execute
    'CLUSTER SLOTS' command.
    """
    with patch.object(Redis, "execute_command") as execute_command_mock:
        execute_command_mock.side_effect = RedisError("foobar")

        n = NodeManager(startup_nodes=[{"host": "6.6.6.6", "port": 1234}])

        with pytest.raises(RedisClusterException):
            await n.initialize()


def test_set_node():
    """
    Test to update data in a slot.
    """
    expected = ManagedNode(host="127.0.0.1", port=7000, server_type="primary")
    n = NodeManager(startup_nodes=[])
    assert len(n.slots) == 0, "no slots should exist"
    res = n.set_node(host="127.0.0.1", port=7000, server_type="primary")
    assert res == expected
    assert n.nodes == {expected.name: expected}


async def test_reset(redis_cluster):
    """
    Test that reset method resets variables back to correct default values.
    """

    n = NodeManager(startup_nodes=[])
    n.initialize = AsyncMock()
    await n.reset()
    assert n.initialize.call_count == 1


async def test_cluster_one_instance(redis_cluster):
    """
    If the cluster exists of only 1 node then there is some hacks that must
    be validated they work.
    """
    with patch.object(Redis, "cluster_slots", new_callable=AsyncMock) as mock_cluster_slots:
        with patch.object(Redis, "config_get", new_callable=AsyncMock) as mock_config_get:
            mock_config_get.return_value = {"cluster-require-full-coverage": "yes"}
            mock_cluster_slots.return_value = {
                (0, 16383): [
                    {
                        "host": "",
                        "port": 7006,
                        "node_id": str(uuid.uuid4()),
                        "server_type": "master",
                    }
                ],
            }

            n = NodeManager(startup_nodes=[{"host": "127.0.0.1", "port": 7006}])
            await n.initialize()

            del n.nodes["127.0.0.1:7006"].node_id
            assert n.nodes == {
                "127.0.0.1:7006": ManagedNode(host="127.0.0.1", port=7006, server_type="primary")
            }
            assert len(n.slots) == 16384

            for i in range(0, 16384):
                assert n.slots[i] == [
                    ManagedNode(
                        host="127.0.0.1",
                        port=7006,
                        server_type="primary",
                    )
                ]


async def test_initialize_follow_cluster(redis_cluster):
    n = NodeManager(
        nodemanager_follow_cluster=True,
        startup_nodes=[{"host": "127.0.0.1", "port": 7000}],
    )
    n.orig_startup_nodes = None
    await n.initialize()


async def test_init_with_down_node(redis_cluster):
    """
    If I can't connect to one of the nodes, everything should still work.
    But if I can't connect to any of the nodes, exception should be thrown.
    """

    def get_redis_link(host, port, decode_responses=False):
        if port == 7000:
            raise ConnectionError("mock connection error for 7000")

        return Redis(host=host, port=port, decode_responses=decode_responses)

    with patch.object(NodeManager, "get_redis_link", side_effect=get_redis_link):
        n = NodeManager(startup_nodes=[{"host": "127.0.0.1", "port": 7000}])
        with pytest.raises(RedisClusterException) as e:
            await n.initialize()
        assert "Redis Cluster cannot be connected" in str(e.value)


async def test_cluster_initialization_fail(redis_cluster_auth, cloner):
    with pytest.raises(RedisClusterException, match="invalid username-password pair"):
        await cloner(redis_cluster_auth, password="wrong")


async def test_cluster_initialization_credential_provider_fail(
    redis_cluster_auth_cred_provider, cloner
):
    with pytest.raises(RedisClusterException, match="invalid username-password pair"):
        await cloner(
            redis_cluster_auth_cred_provider,
            credential_provider=UserPassCredentialProvider(password="wrong"),
        )
