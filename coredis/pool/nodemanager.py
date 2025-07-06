from __future__ import annotations

import dataclasses
import random
import warnings
from typing import TYPE_CHECKING, Any

from coredis._utils import b, hash_slot, nativestr
from coredis.exceptions import (
    ConnectionError,
    RedisClusterException,
    RedisError,
    ResponseError,
)
from coredis.typing import (
    Iterable,
    Iterator,
    Literal,
    Node,
    RedisValueT,
    StringT,
)

HASH_SLOTS = 16384
HASH_SLOTS_SET = set(range(HASH_SLOTS))

if TYPE_CHECKING:
    from coredis import Redis


@dataclasses.dataclass
class ManagedNode:
    """
    Represents a cluster node (primary or replica) in a redis cluster
    """

    host: str
    port: int
    server_type: Literal["primary", "replica"] | None = None
    node_id: str | None = None

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"


class NodeManager:
    """
    Utility class to manage the topology of a redis cluster
    """

    def __init__(
        self,
        startup_nodes: Iterable[Node] | None = None,
        reinitialize_steps: int | None = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        decode_responses: bool = False,
        **connection_kwargs: Any | None,
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
        self.connection_kwargs = connection_kwargs
        self.connection_kwargs.update(decode_responses=decode_responses)

        self.nodes: dict[str, ManagedNode] = {}
        self.slots: dict[int, list[ManagedNode]] = {}
        self.startup_nodes: list[ManagedNode] = (
            []
            if startup_nodes is None
            else list(ManagedNode(n["host"], n["port"]) for n in startup_nodes if n)
        )
        self.startup_nodes_reachable = False
        self.orig_startup_nodes = list(self.startup_nodes)
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps or 25
        self._skip_full_coverage_check = skip_full_coverage_check
        self.nodemanager_follow_cluster = nodemanager_follow_cluster
        self.replicas_per_shard = 0

    def keys_to_nodes_by_slot(self, *keys: RedisValueT) -> dict[str, dict[int, list[RedisValueT]]]:
        mapping: dict[str, dict[int, list[RedisValueT]]] = {}
        for k in keys:
            node = self.node_from_slot(hash_slot(b(k)))
            if node:
                mapping.setdefault(node.name, {}).setdefault(hash_slot(b(k)), []).append(k)
        return mapping

    def node_from_slot(self, slot: int) -> ManagedNode | None:
        for node in self.slots[slot]:
            if node.server_type == "primary":
                return node
        return None  # noqa

    def nodes_from_slots(self, *slots: int) -> dict[str, list[int]]:
        mapping: dict[str, list[int]] = {}
        for slot in slots:
            if node := self.node_from_slot(slot):
                mapping.setdefault(node.name, []).append(slot)
        return mapping

    def all_nodes(self) -> Iterator[ManagedNode]:
        yield from self.nodes.values()

    def all_primaries(self) -> Iterator[ManagedNode]:
        for node in self.nodes.values():
            if node.server_type == "primary":
                yield node

    def all_replicas(self) -> Iterator[ManagedNode]:
        for node in self.nodes.values():
            if node.server_type == "replica":
                yield node

    def random_startup_node_iter(self, primary: bool = False) -> Iterator[ManagedNode]:
        """A generator that returns a random startup nodes"""
        options = list(
            self.all_primaries()
            if primary
            else (self.startup_nodes if self.startup_nodes_reachable else [])
        )
        while options:
            choice = random.choice(options)
            options.remove(choice)
            yield choice

    def random_node(self, primary: bool = True) -> ManagedNode:
        if primary:
            return random.choice(list(self.all_primaries()))
        else:
            return random.choice(list(self.nodes.values()))

    def get_redis_link(self, host: str, port: int) -> Redis[Any]:
        from coredis.client import Redis

        allowed_keys = (
            "username",
            "password",
            "credential_provider",
            "encoding",
            "decode_responses",
            "stream_timeout",
            "connect_timeout",
            "ssl_context",
            "parser_class",
            "loop",
            "protocol_version",
        )
        connection_kwargs = {k: v for k, v in self.connection_kwargs.items() if k in allowed_keys}
        return Redis(host=host, port=port, **connection_kwargs)  # type: ignore

    async def initialize(self) -> None:
        """
        Initializes the slots cache by asking all startup nodes what the
        current cluster configuration is.

        TODO: Currently the last node will have the last say about how the configuration is setup.
        Maybe it should stop to try after it have correctly covered all slots or when one node is
        reached and it could execute CLUSTER SLOTS command.
        """
        nodes_cache: dict[str, ManagedNode] = {}
        tmp_slots: dict[int, list[ManagedNode]] = {}

        all_slots_covered = False
        disagreements: list[str] = []
        self.startup_nodes_reachable = False

        nodes = self.orig_startup_nodes
        replicas: set[str] = set()
        startup_node_errors: dict[str, list[str]] = {}

        # With this option the client will attempt to connect to any of the previous set of nodes
        # instead of the original set of startup nodes
        if self.nodemanager_follow_cluster:
            nodes = self.startup_nodes

        for node in nodes:
            cluster_slots = {}
            try:
                if node:
                    r = self.get_redis_link(host=node.host, port=node.port)
                    cluster_slots = await r.cluster_slots()
                    self.startup_nodes_reachable = True
            except RedisError as err:
                startup_node_errors.setdefault(str(err), []).append(node.name)
                continue

            all_slots_covered = True
            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if len(cluster_slots) == 1 and len(self.startup_nodes) == 1:
                slots = cluster_slots.get((0, HASH_SLOTS - 1))
                assert slots
                single_node_slots = slots[0]
                if len(single_node_slots["host"]) == 0:
                    single_node_slots["host"] = self.startup_nodes[0].host
                    single_node_slots["server_type"] = "master"

            for min_slot, max_slot in cluster_slots:
                _nodes = cluster_slots.get((min_slot, max_slot))
                assert _nodes
                primary_node = ManagedNode(
                    host=_nodes[0]["host"],
                    port=_nodes[0]["port"],
                    server_type="primary",
                    node_id=_nodes[0]["node_id"],
                )
                replica_nodes = [
                    ManagedNode(
                        host=n["host"],
                        port=n["port"],
                        server_type="replica",
                        node_id=n["node_id"],
                    )
                    for n in _nodes[1:]
                ]

                primary_node.host = primary_node.host or node.host
                nodes_cache[primary_node.name] = primary_node

                for i in range(min_slot, max_slot + 1):
                    if i not in tmp_slots:
                        tmp_slots[i] = [primary_node]
                        for replica_node in replica_nodes:
                            nodes_cache[replica_node.name] = replica_node
                            tmp_slots[i].append(replica_node)
                            replicas.add(replica_node.name)
                    else:
                        # Validate that 2 nodes want to use the same slot cache setup
                        if tmp_slots[i][0].name != node.name:
                            disagreements.append(
                                f"{tmp_slots[i][0].name} vs {node.name} on slot: {i}",
                            )
                            if len(disagreements) > 5:
                                raise RedisClusterException(
                                    "startup_nodes could not agree on a valid slots cache."
                                    f" {', '.join(disagreements)}"
                                )

                self.refresh_table_asap = False

            if not self._skip_full_coverage_check and (
                await self.cluster_require_full_coverage(nodes_cache)
            ):
                all_slots_covered = set(tmp_slots.keys()) == HASH_SLOTS_SET

            if all_slots_covered:
                break

        if not self.startup_nodes_reachable:
            details = ""
            # collapse any startup nodes by error representation
            if startup_node_errors:
                details = " Underlying errors:\n" + "\n".join(
                    [f"- {err} [{','.join(nodes)}]" for err, nodes in startup_node_errors.items()]
                )
            raise RedisClusterException(
                "Redis Cluster cannot be connected. "
                "Please provide at least one reachable node."
                f"{details}"
            )

        if not all_slots_covered:
            raise RedisClusterException(
                "Not all slots are covered after query all startup_nodes. "
                f"{len(tmp_slots)} of {HASH_SLOTS} covered..."
            )

        # Set the tmp variables to the real variables
        self.slots = tmp_slots
        self.nodes = nodes_cache
        self.replicas_per_shard = int((len(self.nodes) / len(replicas)) - 1 if replicas else 0)
        self.reinitialize_counter = 0
        self.populate_startup_nodes()

    async def increment_reinitialize_counter(self, ct: int = 1) -> None:
        for _ in range(min(ct, self.reinitialize_steps)):
            self.reinitialize_counter += 1
            if self.reinitialize_counter % self.reinitialize_steps == 0:
                await self.initialize()

    async def node_require_full_coverage(self, node: ManagedNode) -> bool:
        try:
            r_node = self.get_redis_link(host=node.host, port=node.port)
            node_config = await r_node.config_get(["cluster-require-full-coverage"])
            return "yes" in node_config.values()
        except ResponseError as err:
            warnings.warn(
                "Unable to determine whether the cluster requires full coverage "
                f"due to response error from `CONFIG GET`: {err}. To suppress this "
                "warning use skip_full_coverage=True when initializing the client."
            )
            return False

    async def cluster_require_full_coverage(self, nodes_cache: dict[str, ManagedNode]) -> bool:
        """
        If exists 'cluster-require-full-coverage no' config on redis servers,
        then even all slots are not covered, cluster still will be able to
        respond
        """
        nodes = nodes_cache or self.nodes

        for node in nodes.values():
            try:
                if await self.node_require_full_coverage(node):
                    return True
            except ConnectionError:
                continue
        return False

    def set_node(
        self,
        host: StringT,
        port: int,
        server_type: Literal["primary", "replica"],
    ) -> ManagedNode:
        """Updates data for a node"""
        node = ManagedNode(
            host=nativestr(host),
            port=port,
            server_type=server_type,
            node_id=None,
        )
        self.nodes[node.name] = node
        return node

    def populate_startup_nodes(self) -> None:
        self.startup_nodes.clear()
        for n in self.nodes.values():
            self.startup_nodes.append(n)

    async def reset(self) -> None:
        await self.initialize()
