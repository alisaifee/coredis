from __future__ import annotations

import pytest
from anyio import create_task_group, sleep

import coredis
from coredis._utils import hash_slot
from coredis.cluster._discovery import DiscoveryService
from coredis.cluster._layout import ClusterLayout
from coredis.connection import TCPLocation
from coredis.exceptions import ConnectionError, MovedError, RedisClusterError


class TestClusterLayout:
    @pytest.fixture
    async def discovery_service(self, redis_cluster_server):
        return DiscoveryService(startup_nodes=[TCPLocation(*redis_cluster_server)])

    async def test_uninitialized_layout(self, redis_cluster_server, discovery_service):
        layout = ClusterLayout(discovery_service)
        assert not list(layout.nodes)
        assert not list(layout.primaries)
        assert not list(layout.replicas)
        with pytest.raises(RedisClusterError, match="cluster layout cache is empty"):
            layout.node_for_request(b"PING", ())
        with pytest.raises(RedisClusterError, match="cluster layout cache is empty"):
            layout.nodes_for_request(b"PING", ())

        await layout.initialize()
        assert len(list(layout.nodes)) > 1
        assert len(list(layout.primaries)) > 1
        assert len(list(layout.replicas)) > 1

    async def test_error_threshold_primary_moved(
        self, redis_cluster_server, discovery_service, mocker
    ):
        cluster_slots = coredis.Redis.cluster_slots

        refresh_count = 0
        broken_slots = []
        original_primaries = []

        async def move_primaries(self, *args, **kwargs):
            nonlocal refresh_count, broken_slots, original_primaries
            refresh_count += 1
            values = await cluster_slots(self, *args, **kwargs)
            if refresh_count == 1:
                # change the primary host for the first slot range
                slot_range, nodes = list(values.items())[0]
                broken_slots.append(slot_range)
                original_primaries.append(dict(nodes[0]))
                nodes[0]["host"] = "bogus"
                # swap the primary with the replica for the second port range
                slot_range, nodes = list(values.items())[1]
                broken_slots.append(slot_range)
                original_primaries.append(dict(nodes[0]))
                nodes[0]["port"], nodes[1]["port"] = nodes[1]["port"], nodes[0]["port"]
            elif refresh_count == 2:
                raise ConnectionError()
            return values

        mocker.patch.object(coredis.Redis, "cluster_slots", new=move_primaries)
        layout = ClusterLayout(discovery_service, error_threshold=2)
        async with create_task_group() as tg:
            await tg.start(layout.monitor)

            await layout.initialize()
            assert len(list(layout.primaries)) > 1
            assert len(list(layout.replicas)) > 1
            primary = layout.node_for_slot(broken_slots[0][0])
            assert primary.host == "bogus"

            layout.report_errors(
                primary,
                MovedError(
                    f"{broken_slots[0][0]} {original_primaries[0]['host']}:{original_primaries[0]['port']}"
                ),
            )
            layout.report_errors(
                primary,
                MovedError(
                    f"{broken_slots[1][0]} {original_primaries[1]['host']}:{original_primaries[1]['port']}"
                ),
            )

            assert (
                layout.node_for_slot(broken_slots[0][0], primary=True).host
                == original_primaries[0]["host"]
            )
            assert layout.node_for_slot(broken_slots[0][1], primary=True).host == "bogus"
            assert (
                layout.node_for_slot(broken_slots[1][0], primary=True).port
                == original_primaries[1]["port"]
            )

            await sleep(0.5)

            assert (
                layout.node_for_slot(broken_slots[0][0], primary=True).host
                == original_primaries[0]["host"]
            )
            assert (
                layout.node_for_slot(broken_slots[0][1], primary=True).host
                == original_primaries[0]["host"]
            )
            assert (
                layout.node_for_slot(broken_slots[1][0], primary=True).port
                == original_primaries[1]["port"]
            )

            tg.cancel_scope.cancel()

    async def test_error_threshold_replica_moved(
        self, redis_cluster_server, discovery_service, mocker
    ):
        cluster_slots = coredis.Redis.cluster_slots

        refresh_count = 0
        broken_slots = []
        original_replicas = []

        async def move_replicas(self, *args, **kwargs):
            nonlocal refresh_count, broken_slots, original_replicas
            refresh_count += 1
            values = await cluster_slots(self, *args, **kwargs)
            if refresh_count == 1:
                # change the host for the first slot range
                slot_range, nodes = list(values.items())[0]
                original_replicas.append(dict(nodes[1]))
                nodes[1]["host"] = "bogus"
                broken_slots.append(slot_range)
                # swap the primary with the replica for the second port range
                slot_range, nodes = list(values.items())[1]
                original_replicas.append(dict(nodes[1]))
                nodes[0]["port"], nodes[1]["port"] = nodes[1]["port"], nodes[0]["port"]
                broken_slots.append(slot_range)
            elif refresh_count == 2:
                raise ConnectionError()
            return values

        mocker.patch.object(coredis.Redis, "cluster_slots", new=move_replicas)
        layout = ClusterLayout(discovery_service, error_threshold=2)
        async with create_task_group() as tg:
            await tg.start(layout.monitor)

            await layout.initialize()
            assert len(list(layout.primaries)) > 1
            assert len(list(layout.replicas)) > 1
            replica_one = layout.node_for_slot(broken_slots[0][0], primary=False)
            assert replica_one.host == "bogus"
            replica_two = layout.node_for_slot(broken_slots[1][0], primary=False)
            assert replica_two.host == original_replicas[0]["host"]

            layout.report_errors(
                replica_one,
                MovedError(
                    f"{broken_slots[0][0]} {original_replicas[0]['host']}:{original_replicas[0]['port']}"
                ),
            )
            layout.report_errors(
                replica_two,
                MovedError(
                    f"{broken_slots[1][0]} {original_replicas[1]['host']}:{original_replicas[1]['port']}"
                ),
            )

            assert (
                layout.node_for_slot(broken_slots[0][0], primary=False).host
                == original_replicas[0]["host"]
            )
            assert layout.node_for_slot(broken_slots[0][1], primary=False).host == "bogus"
            assert (
                layout.node_for_slot(broken_slots[1][0], primary=False).port
                == original_replicas[1]["port"]
            )

            await sleep(0.5)

            assert (
                layout.node_for_slot(broken_slots[0][0], primary=False).host
                == original_replicas[0]["host"]
            )
            assert (
                layout.node_for_slot(broken_slots[0][1], primary=False).host
                == original_replicas[0]["host"]
            )
            assert (
                layout.node_for_slot(broken_slots[1][0], primary=False).port
                == original_replicas[1]["port"]
            )

            tg.cancel_scope.cancel()

    async def test_staleness_threshold_met(self, redis_cluster_server, discovery_service, mocker):
        cluster_slots = coredis.Redis.cluster_slots

        refresh_count = 0

        async def remove_replicas(self, *args, **kwargs):
            nonlocal refresh_count
            refresh_count += 1
            values = await cluster_slots(self, *args, **kwargs)
            if refresh_count == 1:
                return values
            else:
                raise ConnectionError()

        mocker.patch.object(coredis.Redis, "cluster_slots", new=remove_replicas)
        layout = ClusterLayout(discovery_service, error_threshold=1, maximum_staleness=1)
        with pytest.RaisesGroup(pytest.RaisesExc(RedisClusterError)):
            async with create_task_group() as tg:
                await tg.start(layout.monitor)
                await layout.initialize()
                assert len(list(layout.primaries)) > 1
                assert len(list(layout.replicas)) > 1
                layout.report_errors(None, MovedError("1 localhost:6379"))

    async def test_error_burst(self, redis_cluster_server, discovery_service, mocker):
        layout = ClusterLayout(discovery_service, error_threshold=10, maximum_staleness=1)
        await layout.initialize()
        async with create_task_group() as tg:
            await tg.start(layout.monitor)

            refresh = mocker.spy(layout, "_refresh")
            nodes = layout.nodes
            [layout.report_errors(None, ConnectionError()) for _ in range(100)]
            await sleep(0.5)
            assert refresh.call_count == 2
            assert layout.nodes == nodes

            tg.cancel_scope.cancel()

    async def test_node_for_request(self, redis_cluster_server, discovery_service):
        layout = ClusterLayout(discovery_service, error_threshold=1, maximum_staleness=1)
        await layout.initialize()

        with pytest.raises(RedisClusterError, match="single node"):
            layout.node_for_request(b"PING", ())
        with pytest.raises(RedisClusterError, match="don't hash to the same slot"):
            layout.node_for_request(b"MGET", ("a{a}", "a{b}", "a{c}"))
        assert layout.node_for_request(b"MGET", ("a{a}", "b{a}"))
        with pytest.raises(RedisClusterError):
            layout.node_for_request(b"DEL", ("a{a}", "b{b}", "c{c}", "d{d}"))

    async def test_nodes_for_request(self, redis_cluster_server, discovery_service):
        layout = ClusterLayout(discovery_service, error_threshold=1, maximum_staleness=1)
        await layout.initialize()

        assert list(layout.nodes_for_request(b"SAVE", ()).keys()) == list(layout.nodes)
        assert list(layout.nodes_for_request(b"PING", ()).keys()) == list(layout.primaries)
        assert len(list(layout.nodes_for_request(b"ACL WHOAMI", ()).keys())) == 1
        assert len(list(layout.nodes_for_request(b"FCALL", ("test", 0)).keys())) == 1
        assert (
            len(
                list(
                    layout.nodes_for_request(
                        b"CLUSTER DELSLOTS",
                        (0, 16000),
                        execution_parameters={"slot_arguments_range": (0, 1)},
                    ).keys()
                )
            )
            == 1
        )
        assert list(layout.nodes_for_request(b"GET", ("a{a}")).keys()) == [
            layout.node_for_slot(hash_slot(b"a{a}"))
        ]
        with pytest.raises(RedisClusterError):
            layout.nodes_for_request(b"DEL", ("a{a}", "b{b}", "c{c}", "d{d}"))
        layout.nodes_for_request(b"DEL", ("a{a}", "b{b}", "c{c}", "d{d}"), allow_cross_slot=True)

    async def test_node_for_slot(self, redis_cluster_server, discovery_service):
        layout = ClusterLayout(discovery_service, error_threshold=1, maximum_staleness=1)
        await layout.initialize()

        assert layout.node_for_slot(1)
        with pytest.raises(RedisClusterError, match="Unable to map slot 18000 to a primary node"):
            layout.node_for_slot(18000)
        with pytest.raises(RedisClusterError, match="Unable to map slot 18000 to a replica node"):
            layout.node_for_slot(18000, primary=False)

    async def test_replica_nodes(
        self, redis_cluster_server, discovery_service, free_tcp_port_factory, mocker
    ):
        cluster_slots = coredis.Redis.cluster_slots

        async def add_bad_replicas(self, *args, **kwargs):
            values = await cluster_slots(self, *args, **kwargs)
            for slot_range, nodes in values.items():
                bad_replicas = []
                for _ in range(10):
                    bad_replica = dict(nodes[-1])
                    bad_replica["port"] = free_tcp_port_factory()
                    bad_replicas.append(bad_replica)
                values[slot_range] = nodes + tuple(bad_replicas)
            return values

        mocker.patch.object(coredis.Redis, "cluster_slots", new=add_bad_replicas)
        layout = ClusterLayout(discovery_service)
        await layout.initialize()
        replica = layout.node_for_slot(1, primary=False)
        layout.report_errors(replica, ConnectionError())
        assert replica != layout.node_for_slot(1, primary=False)
