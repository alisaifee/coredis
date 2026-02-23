from __future__ import annotations

import random

from coredis.cluster._discovery import DiscoveryService
from coredis.cluster._layout import ClusterLayout
from coredis.cluster._node import ClusterNodeLocation
from coredis.commands.constants import NodeFlag
from coredis.connection import BaseConnectionParams, TCPLocation
from coredis.typing import (
    ExecutionParameters,
    Iterable,
    Iterator,
    RedisValueT,
    StringT,
    Unpack,
)


class NodeManager:
    """
    Utility class to manage the topology of a redis cluster
    """

    def __init__(
        self,
        startup_nodes: Iterable[TCPLocation] | None = None,
        reinitialize_steps: int | None = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        **connection_kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        """
        :skip_full_coverage_check:
            Skips the check of cluster-require-full-coverage config, useful for clusters
            without the CONFIG command (like aws)
        :nodemanager_follow_cluster:
            The node manager will during initialization try the last set of nodes that
            it was operating on. This will allow the client to drift along side the cluster
            if the cluster nodes move around a slot.
        """
        self._discovery_service = DiscoveryService(
            startup_nodes=startup_nodes,
            skip_full_coverage_check=skip_full_coverage_check,
            follow_cluster=nodemanager_follow_cluster,
            **connection_kwargs,
        )
        self._cluster_layout: ClusterLayout | None = None

        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps or 25

    async def initialize(self) -> None:
        """
        Initializes the slots cache by asking all startup nodes what the
        current cluster configuration is.

        TODO: Currently the last node will have the last say about how the configuration is setup.
        Maybe it should stop to try after it have correctly covered all slots or when one node is
        reached and it could execute CLUSTER SLOTS command.
        """
        self._cluster_layout = await self._discovery_service.get_cluster_layout()
        self.reinitialize_counter = 0

    @property
    def cluster_layout(self) -> ClusterLayout:
        assert self._cluster_layout

        return self._cluster_layout

    async def increment_reinitialize_counter(self, ct: int = 1) -> None:
        for _ in range(min(ct, self.reinitialize_steps)):
            self.reinitialize_counter += 1

            if self.reinitialize_counter % self.reinitialize_steps == 0:
                await self.initialize()

    def update_primary(
        self,
        slot: int,
        host: StringT,
        port: int,
    ) -> ClusterNodeLocation:
        return self.cluster_layout.update_primary(slot, host, port)

    def determine_slots(
        self,
        command: bytes,
        *args: RedisValueT,
        readonly: bool = False,
        **options: Unpack[ExecutionParameters],
    ) -> set[int]:
        """Determines the slots the command and args would touch"""

        return self.cluster_layout.determine_slots(command, *args, readonly=readonly, **options)

    def determine_node(
        self,
        command: bytes,
        *args: RedisValueT,
        node_flag: NodeFlag | None = None,
        readonly: bool = False,
        **kwargs: Unpack[ExecutionParameters],
    ) -> list[ClusterNodeLocation] | None:
        return self.cluster_layout.determine_node(
            command, *args, node_flag=node_flag, readonly=readonly, **kwargs
        )

    def keys_to_nodes_by_slot(
        self, *keys: RedisValueT
    ) -> dict[ClusterNodeLocation, dict[int, list[RedisValueT]]]:
        return self.cluster_layout.keys_to_nodes_by_slot(*keys)

    def keys_to_slots(self, *keys: RedisValueT) -> set[int]:
        return self.cluster_layout.keys_to_slots(*keys)

    def node_from_location(self, location: TCPLocation) -> ClusterNodeLocation | None:
        return self.cluster_layout.node_from_location(location)

    def node_from_slot(self, slot: int, primary: bool = True) -> ClusterNodeLocation | None:
        return self.cluster_layout.node_from_slot(slot, primary)

    def nodes_from_slots(
        self, *slots: int, primary: bool = True
    ) -> dict[ClusterNodeLocation, list[int]]:
        return self.cluster_layout.nodes_from_slots(*slots, primary=primary)

    def all_nodes(self) -> Iterator[ClusterNodeLocation]:
        return self.cluster_layout.all_nodes()

    def all_primaries(self) -> Iterator[ClusterNodeLocation]:
        return self.cluster_layout.all_primaries()

    def all_replicas(self) -> Iterator[ClusterNodeLocation]:
        return self.cluster_layout.all_replicas()

    def random_node(self, primary: bool = False) -> ClusterNodeLocation:
        return random.choice(
            list(
                self.cluster_layout.all_primaries() if primary else self.cluster_layout.all_nodes()
            )
        )
