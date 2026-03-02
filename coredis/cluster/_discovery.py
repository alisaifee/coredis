from __future__ import annotations

import warnings
from collections import defaultdict

from coredis.connection import BaseConnectionParams, TCPLocation
from coredis.exceptions import ConnectionError, RedisClusterError, RedisError, ResponseError
from coredis.typing import Final, Iterable, Unpack

from ._node import ClusterNodeLocation

HASH_SLOTS: Final = 16384
HASH_SLOTS_SET: Final = set(range(HASH_SLOTS))


class DiscoveryService:
    def __init__(
        self,
        startup_nodes: Iterable[TCPLocation] | None = None,
        skip_full_coverage_check: bool = False,
        follow_cluster: bool = True,
        **connection_kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        self._connection_kwargs = connection_kwargs
        self._startup_nodes: list[ClusterNodeLocation] = (
            []
            if startup_nodes is None
            else list(ClusterNodeLocation(n.host, n.port) for n in startup_nodes if n)
        )
        self._startup_nodes_reachable = False
        self._initial_startup_nodes = list(self._startup_nodes)
        self._follow_cluster = follow_cluster
        self._skip_full_coverage_check = skip_full_coverage_check

    async def get_cluster_layout(
        self,
    ) -> tuple[list[ClusterNodeLocation], defaultdict[int, list[ClusterNodeLocation]]]:
        """
        Initializes the slots cache by asking all startup nodes what the
        current cluster configuration is.

        TODO: Currently the last node will have the last say about how the configuration is setup.
        Maybe it should stop to try after it have correctly covered all slots or when one node is
        reached and it could execute CLUSTER SLOTS command.
        """
        self._startup_nodes_reachable = False

        nodes_cache: dict[TCPLocation, ClusterNodeLocation] = {}
        tmp_slots: defaultdict[int, list[ClusterNodeLocation]] = defaultdict(list)

        all_slots_covered = False
        disagreements: list[str] = []
        replicas: set[str] = set()
        startup_node_errors: dict[Exception, list[str]] = {}

        nodes = self._initial_startup_nodes

        # With this option the client will attempt to connect to any of the previous set of nodes
        # instead of the original set of startup nodes
        if self._follow_cluster:
            nodes = self._startup_nodes

        for node in nodes:
            cluster_slots = {}
            if node:
                async with node.as_client(**self._connection_kwargs) as r:
                    try:
                        cluster_slots = await r.cluster_slots()
                        self._startup_nodes_reachable = True
                    except RedisError as err:
                        startup_node_errors.setdefault(err, []).append(node.name)
                        continue
            all_slots_covered = True

            for min_slot, max_slot in cluster_slots:
                _nodes = cluster_slots.get((min_slot, max_slot))
                if _nodes:
                    primary_node = ClusterNodeLocation(
                        host=_nodes[0]["host"],
                        port=_nodes[0]["port"],
                        server_type="primary",
                        node_id=_nodes[0]["node_id"],
                    )
                    replica_nodes = [
                        ClusterNodeLocation(
                            host=n["host"],
                            port=n["port"],
                            server_type="replica",
                            node_id=n["node_id"],
                        )
                        for n in _nodes[1:]
                    ]

                    primary_node.host = primary_node.host or node.host
                    nodes_cache[TCPLocation(primary_node.host, primary_node.port)] = primary_node

                    for i in range(min_slot, max_slot + 1):
                        if i not in tmp_slots:
                            tmp_slots[i].append(primary_node)
                            for replica_node in replica_nodes:
                                nodes_cache[TCPLocation(replica_node.host, replica_node.port)] = (
                                    replica_node
                                )
                                tmp_slots[i].append(replica_node)
                                replicas.add(replica_node.name)
                        else:
                            # Validate that 2 nodes want to use the same slot cache setup
                            if tmp_slots[i][0].name != node.name:
                                disagreements.append(
                                    f"{tmp_slots[i][0].name} vs {node.name} on slot: {i}",
                                )
                                if len(disagreements) > 5:
                                    raise RedisClusterError(
                                        "startup_nodes could not agree on a valid slots cache."
                                        f" {', '.join(disagreements)}"
                                    )
            if not (nodes_cache and tmp_slots):
                all_slots_covered = False
            elif not self._skip_full_coverage_check and (
                await self._cluster_require_full_coverage(nodes_cache)
            ):
                all_slots_covered = set(tmp_slots.keys()) == HASH_SLOTS_SET

            if all_slots_covered:
                break

        if not self._startup_nodes_reachable:
            startup_error = RedisClusterError(
                "Redis Cluster cannot be connected. Please provide at least one reachable node."
            )
            if startup_node_errors:
                startup_error.__cause__ = list(startup_node_errors.keys())[-1]
            raise startup_error
        if not all_slots_covered:
            raise RedisClusterError(
                "Not all slots are covered after attempting to query all startup nodes. "
                f"{len(tmp_slots)} of {HASH_SLOTS} covered."
            )

        self._startup_nodes.clear()
        self._startup_nodes.extend(nodes_cache.values())
        return list(nodes_cache.values()), tmp_slots

    async def _cluster_require_full_coverage(
        self, nodes: dict[TCPLocation, ClusterNodeLocation]
    ) -> bool:
        """
        If exists 'cluster-require-full-coverage no' config on redis servers,
        then even all slots are not covered, cluster still will be able to
        respond
        """

        for node in nodes.values():
            if await self._node_require_full_coverage(node):
                return True
        return False

    async def _node_require_full_coverage(self, node: ClusterNodeLocation) -> bool:
        async with node.as_client(**self._connection_kwargs) as r_node:
            try:
                with r_node.decoding(False):
                    node_config = await r_node.config_get(["cluster-require-full-coverage"])
                    return b"yes" in node_config.values()
            except ResponseError as err:
                warnings.warn(
                    "Unable to determine whether the cluster requires full coverage "
                    f"due to response error from `CONFIG GET`: {err}. To suppress this "
                    "warning use skip_full_coverage=True when initializing the client."
                )
                return False
            except ConnectionError:
                return False
