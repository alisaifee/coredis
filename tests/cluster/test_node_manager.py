from __future__ import annotations

import pytest

import coredis
from coredis.connection import TCPLocation
from coredis.exceptions import AuthenticationError, RedisClusterError, ResponseError
from coredis.pool._nodemanager import NodeManager


class TestStartupNodes:
    async def test_initialization(self, redis_cluster_server, mocker):
        startup_nodes = [TCPLocation(*redis_cluster_server)]
        manager = NodeManager(startup_nodes, connect_timeout=0.1, reinitialize_steps=5)
        await manager.initialize()
        initialize_spy = mocker.spy(manager, "initialize")
        [await manager.increment_reinitialize_counter() for _ in range(5)]
        assert initialize_spy.call_count == 1
        [await manager.increment_reinitialize_counter() for _ in range(5)]
        assert initialize_spy.call_count == 2

    @pytest.mark.parametrize("username, password", ([None, None], ["wrong", "password"]))
    async def test_authenticated_cluster_invalid_credentials(
        self, redis_cluster_auth_server, username, password
    ):
        manager = NodeManager(
            [TCPLocation(*redis_cluster_auth_server)],
            username=username,
            password=password,
        )
        with pytest.raises(RedisClusterError) as exc:
            await manager.initialize()
        assert isinstance(exc.value.__cause__, AuthenticationError)

    async def test_authenticated_cluster(
        self,
        redis_cluster_auth_server,
    ):
        manager = NodeManager(
            [TCPLocation(*redis_cluster_auth_server)],
            username=None,
            password="sekret",
        )
        await manager.initialize()
        assert len(list(manager.all_nodes())) > 1

    async def test_partially_down_startup_nodes(self, redis_cluster_server, free_tcp_port_factory):
        startup_nodes = [
            TCPLocation("127.0.0.1", free_tcp_port_factory()),
            TCPLocation(*redis_cluster_server),
        ]
        manager = NodeManager(startup_nodes, connect_timeout=0.1)
        await manager.initialize()
        startup_nodes.pop(-1)
        manager = NodeManager(startup_nodes, connect_timeout=0.1)
        with pytest.raises(RedisClusterError):
            await manager.initialize()

    async def test_partial_slot_coverage(self, redis_cluster_server, mocker):
        startup_nodes = [TCPLocation(*redis_cluster_server)]
        cluster_slots = coredis.Redis.cluster_slots

        async def mocked_cluster_slots(self, *args, **kwargs):
            value = await cluster_slots(self, *args, **kwargs)
            slot_ranges = list(value.keys())
            value[(slot_ranges[0][0], slot_ranges[0][1] - 1)] = value.pop(slot_ranges[0])

            return value

        mocker.patch.object(coredis.Redis, "cluster_slots", new=mocked_cluster_slots)
        manager = NodeManager(
            startup_nodes,
            connect_timeout=0.1,
            skip_full_coverage_check=False,
        )
        with pytest.raises(RedisClusterError, match="Not all slots are covered"):
            await manager.initialize()

    async def test_partial_slot_coverage_allowed(self, redis_cluster_server, mocker):
        startup_nodes = [TCPLocation(*redis_cluster_server)]
        cluster_slots = coredis.Redis.cluster_slots

        async def mocked_cluster_slots(self, *args, **kwargs):
            value = await cluster_slots(self, *args, **kwargs)
            slot_ranges = list(value.keys())
            value[(slot_ranges[0][0], slot_ranges[0][1] - 1)] = value.pop(slot_ranges[0])

            return value

        mocker.patch.object(coredis.Redis, "cluster_slots", new=mocked_cluster_slots)
        manager = NodeManager(
            startup_nodes,
            connect_timeout=0.1,
            skip_full_coverage_check=False,
        )

        async def mocked_config_get(self, *args, **kwargs):
            raise ResponseError()

        mocker.patch.object(coredis.Redis, "config_get", new=mocked_config_get)
        await manager.initialize()
        mocker.resetall()

        manager = NodeManager(
            startup_nodes,
            connect_timeout=0.1,
            skip_full_coverage_check=True,
        )
        await manager.initialize()

    async def test_slot_coverage_disagreement(self, redis_cluster_server, mocker):
        startup_nodes = [
            TCPLocation(*redis_cluster_server),
            TCPLocation(redis_cluster_server[0], redis_cluster_server[1] + 1),
            TCPLocation(redis_cluster_server[0], redis_cluster_server[1] + 2),
        ]
        cluster_slots = coredis.Redis.cluster_slots
        count = 0

        # This is probably unrealistic, but each startup node
        # has a different node for 5 slots and each has one
        # slot completely missing in its map.
        async def mocked_cluster_slots(self, *args, **kwargs):
            nonlocal count
            value = await cluster_slots(self, *args, **kwargs)
            slot_range_to_corrupt = count % len(value)
            slot_ranges = list(sorted(value.keys(), key=lambda k: k[0]))
            corruptable = slot_ranges[slot_range_to_corrupt]
            slots = value.pop(corruptable)
            value[corruptable[0] - 1, corruptable[1] - 5] = slots
            if slot_range_to_corrupt < len(value):
                next_range = slot_ranges[slot_range_to_corrupt + 1]
                slots = value.pop(next_range)
                value[next_range[0] - 5, next_range[1]] = slots
            count += 1
            return value

        mocker.patch.object(coredis.Redis, "cluster_slots", new=mocked_cluster_slots)
        manager = NodeManager(
            startup_nodes,
            connect_timeout=0.1,
            skip_full_coverage_check=False,
        )

        with pytest.raises(RedisClusterError, match="could not agree on a valid slots cache"):
            await manager.initialize()
