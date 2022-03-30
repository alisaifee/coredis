from __future__ import annotations

import asyncio

import pytest

from tests.conftest import targets


@targets("redis_cluster", "redis_cluster_resp3")
@pytest.mark.asyncio()
class TestCluster:
    async def test_cluster_info(self, client):
        info = await client.cluster_info()
        assert info["cluster_state"] == "ok"

        info = await list(client.replicas)[0].cluster_info()
        assert info["cluster_state"] == "ok"

        info = await list(client.primaries)[0].cluster_info()
        assert info["cluster_state"] == "ok"

    async def test_cluster_keyslot(self, client):
        slot = await client.cluster_keyslot("a")
        assert slot is not None
        await client.set("a", "1")
        assert await client.cluster_countkeysinslot(slot) == 1
        assert await client.cluster_getkeysinslot(slot, 1) == ("a",)

    async def test_cluster_nodes(self, client):
        nodes = await client.cluster_nodes()
        assert len(nodes) == 6
        replicas = await client.cluster_replicas(
            [n["id"] for n in nodes if "master" in n["flags"]].pop()
        )
        assert len(replicas) == 1

    @pytest.mark.min_server_version("6.9.0")
    async def test_cluster_links(self, client):
        links = []
        for node in client.primaries:
            links.append(await node.cluster_links())
        for node in client.replicas:
            links.append(await node.cluster_links())
        assert len(links) == 6

    @pytest.mark.min_server_version("6.9.0")
    async def test_cluster_my_id(self, client):
        ids = []
        for node in client.primaries:
            ids.append(node.cluster_myid())
        for node in client.replicas:
            ids.append(node.cluster_myid())
        ids = await asyncio.gather(*ids)
        known_nodes = (
            node["node_id"] for node in client.connection_pool.nodes.all_nodes()
        )
        assert set(ids) == set(known_nodes)
