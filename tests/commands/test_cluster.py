from __future__ import annotations

import asyncio

import pytest

from coredis import PureToken
from coredis._utils import hash_slot
from coredis.exceptions import MovedError, ResponseError
from coredis.tokens import PrefixToken
from tests.conftest import targets


@targets(
    "redis_cluster", "redis_cluster_raw", "redis_cluster_resp2", "redis_cluster_ssl"
)
@pytest.mark.asyncio()
class TestCluster:
    async def test_addslots(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        client = client.connection_pool.nodes.get_redis_link(node["host"], node["port"])
        with pytest.raises(ResponseError, match="Slot 1 is already busy"):
            await client.cluster_addslots([1])

    @pytest.mark.min_server_version("7.0.0")
    async def test_addslots_range(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        client = client.connection_pool.nodes.get_redis_link(node["host"], node["port"])
        with pytest.raises(ResponseError, match="Slot 1 is already busy"):
            await client.cluster_addslotsrange([(1, 2)])

    async def test_asking(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        assert await client.connection_pool.nodes.get_redis_link(
            node["host"], node["port"]
        ).asking()

    async def test_count_failure_reports(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        assert 0 == await client.cluster_count_failure_reports(node["node_id"])
        with pytest.raises(ResponseError, match="Unknown node"):
            await client.cluster_count_failure_reports("bogus")

    async def test_cluster_delslots(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        assert await client.cluster_delslots([1])
        assert await client.connection_pool.nodes.get_redis_link(
            node["host"], node["port"]
        ).cluster_addslots([1])

    @pytest.mark.min_server_version("7.0.0")
    async def test_cluster_delslots_range(self, client, _s):
        node = client.connection_pool.get_primary_node_by_slot(1)
        assert await client.cluster_delslotsrange([(1, 2)])
        assert await client.connection_pool.nodes.get_redis_link(
            node["host"], node["port"]
        ).cluster_addslots([1, 2])

    async def test_readonly_explicit(self, client, _s):
        await client.set("fubar", 1)
        slot = hash_slot(b"fubar")
        node = client.connection_pool.get_replica_node_by_slot(slot, replica_only=True)
        node_client = client.connection_pool.nodes.get_redis_link(
            node["host"], node["port"]
        )
        with pytest.raises(MovedError):
            await node_client.get("fubar")
        await node_client.readonly()
        await node_client.get("fubar") == _s(1)
        await node_client.readwrite()
        with pytest.raises(MovedError):
            await node_client.get("fubar")

    async def test_cluster_info(self, client, _s):
        info = await client.cluster_info()
        assert info["cluster_state"] == "ok"

        info = await list(client.replicas)[0].cluster_info()
        assert info["cluster_state"] == "ok"

        info = await list(client.primaries)[0].cluster_info()
        assert info["cluster_state"] == "ok"

    async def test_cluster_keyslot(self, client, _s):
        slot = await client.cluster_keyslot("a")
        assert slot is not None
        await client.set("a", "1")
        assert await client.cluster_countkeysinslot(slot) == 1
        assert await client.cluster_getkeysinslot(slot, 1) == (_s("a"),)

    async def test_cluster_nodes(self, client, _s):
        nodes = await client.cluster_nodes()
        assert len(nodes) == 6
        replicas = await client.cluster_replicas(
            [n["id"] for n in nodes if "master" in n["flags"]].pop()
        )
        with pytest.warns(DeprecationWarning):
            replicas_depr = await client.cluster_slaves(
                [n["id"] for n in nodes if "master" in n["flags"]].pop()
            )
        assert len(replicas) == len(replicas_depr) == 1

    @pytest.mark.min_server_version("7.0.0")
    async def test_cluster_links(self, client, _s):
        links = []
        for node in client.primaries:
            links.append(await node.cluster_links())
        for node in client.replicas:
            links.append(await node.cluster_links())
        assert len(links) == 6

    async def test_cluster_meet(self, client, _s):
        node = list(client.primaries)[0]
        other = list(client.primaries)[1].connection_pool.connection_kwargs
        assert await node.cluster_meet(other["host"], other["port"])
        with pytest.raises(ResponseError, match="Invalid node address"):
            await node.cluster_meet("bogus", 6666)

    async def test_cluster_my_id(self, client, _s):
        ids = []
        for node in client.primaries:
            ids.append(node.cluster_myid())
        for node in client.replicas:
            ids.append(node.cluster_myid())
        ids = await asyncio.gather(*ids)
        known_nodes = (
            _s(node["node_id"]) for node in client.connection_pool.nodes.all_nodes()
        )
        assert set(ids) == set(known_nodes)

    @pytest.mark.min_server_version("7.0.0")
    async def test_cluster_shards(self, client, _s):
        await client
        known_nodes = {
            _s(node["node_id"]) for node in client.connection_pool.nodes.all_nodes()
        }
        shards = await client.cluster_shards()

        nodes = []
        [nodes.extend(shard[_s("nodes")]) for shard in shards]
        assert known_nodes == {node[_s("id")] for node in nodes}


async def test_cluster_bumpepoch(fake_redis):
    fake_redis.responses[b"CLUSTER BUMPEPOCH"] = {
        (): b"OK",
    }
    assert await fake_redis.cluster_bumpepoch()


async def test_cluster_failover(fake_redis):
    fake_redis.responses[b"CLUSTER FAILOVER"] = {
        (): b"OK",
        (PureToken.FORCE,): b"OK",
        (PureToken.TAKEOVER,): b"OK",
    }
    assert await fake_redis.cluster_failover()
    assert await fake_redis.cluster_failover(PureToken.FORCE)
    assert await fake_redis.cluster_failover(PureToken.TAKEOVER)


async def test_cluster_flushslots(fake_redis):
    fake_redis.responses[b"CLUSTER FLUSHSLOTS"] = {(): "OK"}
    assert await fake_redis.cluster_flushslots()


async def test_cluster_forget(fake_redis):
    fake_redis.responses[b"CLUSTER FORGET"] = {(b"abcdefg",): "OK"}
    assert await fake_redis.cluster_forget(b"abcdefg")


async def test_cluster_replicate(fake_redis):
    fake_redis.responses[b"CLUSTER REPLICATE"] = {
        (b"abcdefg",): "OK",
    }
    assert await fake_redis.cluster_replicate(b"abcdefg")


async def test_cluster_reset(fake_redis):
    fake_redis.responses[b"CLUSTER RESET"] = {
        (): "OK",
        (PureToken.HARD,): "OK",
        (PureToken.SOFT,): "OK",
    }
    assert await fake_redis.cluster_reset()
    assert await fake_redis.cluster_reset(PureToken.HARD)
    assert await fake_redis.cluster_reset(PureToken.SOFT)


async def test_cluster_saveconfig(fake_redis):
    fake_redis.responses[b"CLUSTER SAVECONFIG"] = {
        (): "OK",
    }
    assert await fake_redis.cluster_saveconfig()


async def test_cluster_set_config_epoch(fake_redis):
    fake_redis.responses[b"CLUSTER SET-CONFIG-EPOCH"] = {
        (1,): "OK",
    }
    assert await fake_redis.cluster_set_config_epoch(1)


async def test_cluster_setslot(fake_redis):
    fake_redis.responses[b"CLUSTER SETSLOT"] = {
        (1,): "OK",
        (1, PrefixToken.IMPORTING, "abcdefg"): "OK",
        (1, PrefixToken.MIGRATING, "hijkl"): "OK",
        (1, PureToken.STABLE): "OK",
        (1, PrefixToken.NODE, "abcdefg"): "OK",
    }
    assert await fake_redis.cluster_setslot(1)
    assert await fake_redis.cluster_setslot(1, importing="abcdefg")
    assert await fake_redis.cluster_setslot(1, migrating="hijkl")
    assert await fake_redis.cluster_setslot(1, stable=True)
    assert await fake_redis.cluster_setslot(1, node="abcdefg")
