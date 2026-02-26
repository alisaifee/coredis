from __future__ import annotations

import warnings

from coredis.connection import BaseConnectionParams, TCPLocation
from coredis.exceptions import RedisClusterError, RedisError, ResponseError
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
        self.__connection_kwargs = connection_kwargs
        self.__startup_nodes: list[ClusterNodeLocation] = (
            []
            if startup_nodes is None
            else list(ClusterNodeLocation(n.host, n.port) for n in startup_nodes if n)
        )
        self.__startup_nodes_reachable = False
        self.__initial_startup_nodes = list(self.__startup_nodes)
        self.__follow_cluster = follow_cluster
        self.__skip_full_coverage_check = skip_full_coverage_check

    async def get_cluster_layout(
        self,
    ) -> tuple[list[ClusterNodeLocation], dict[int, list[ClusterNodeLocation]]]:
        """
        Initializes the slots cache by asking all startup nodes what the
        current cluster configuration is.

        TODO: Currently the last node will have the last say about how the configuration is setup.
        Maybe it should stop to try after it have correctly covered all slots or when one node is
        reached and it could execute CLUSTER SLOTS command.
        """
        self.__startup_nodes_reachable = False

        nodes_cache: dict[TCPLocation, ClusterNodeLocation] = {}
        tmp_slots: dict[int, list[ClusterNodeLocation]] = {}

        all_slots_covered = False
        disagreements: list[str] = []
        replicas: set[str] = set()
        startup_node_errors: dict[Exception, list[str]] = {}

        nodes = self.__initial_startup_nodes

        # With this option the client will attempt to connect to any of the previous set of nodes
        # instead of the original set of startup nodes
        if self.__follow_cluster:
            nodes = self.__startup_nodes

        for node in nodes:
            cluster_slots = {}
            if node:
                async with node.as_client(**self.__connection_kwargs) as r:
                    try:
                        cluster_slots = await r.cluster_slots()
                        self.__startup_nodes_reachable = True
                    except RedisError as err:
                        startup_node_errors.setdefault(err, []).append(node.name)
                        continue

            all_slots_covered = True
            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if len(cluster_slots) == 1 and len(self.__startup_nodes) == 1:
                slots = cluster_slots.get((0, HASH_SLOTS - 1))
                assert slots
                single_node_slots = slots[0]
                if len(single_node_slots["host"]) == 0:
                    single_node_slots["host"] = self.__startup_nodes[0].host
                    single_node_slots["server_type"] = "master"

            for min_slot, max_slot in cluster_slots:
                _nodes = cluster_slots.get((min_slot, max_slot))
                assert _nodes
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
                        tmp_slots[i] = [primary_node]
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

            if not self.__skip_full_coverage_check and (
                await self._cluster_require_full_coverage(nodes_cache)
            ):
                all_slots_covered = set(tmp_slots.keys()) == HASH_SLOTS_SET

            if all_slots_covered:
                break

        if not self.__startup_nodes_reachable:
            startup_error = RedisClusterError(
                "Redis Cluster cannot be connected. Please provide at least one reachable node."
            )
            if startup_node_errors:
                startup_error.__cause__ = list(startup_node_errors.keys())[-1]
            raise startup_error
        if not all_slots_covered:
            raise RedisClusterError(
                "Not all slots are covered after query all startup_nodes. "
                f"{len(tmp_slots)} of {HASH_SLOTS} covered..."
            )

        self.__startup_nodes.clear()
        self.__startup_nodes.extend(nodes_cache.values())
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
        async with node.as_client(**self.__connection_kwargs) as r_node:
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
