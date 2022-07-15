from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any, cast

from coredis._utils import b, hash_slot, nativestr
from coredis.exceptions import ConnectionError, RedisClusterException
from coredis.typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Node,
    Optional,
    StringT,
    ValueT,
)

HASH_SLOTS = 16384
HASH_SLOTS_SET = set(range(HASH_SLOTS))

if TYPE_CHECKING:
    from coredis import Redis


class NodeManager:
    """
    TODO: document
    """

    def __init__(
        self,
        startup_nodes: Optional[Iterable[Node]] = None,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = False,
        decode_responses: bool = False,
        **connection_kwargs: Optional[Any],
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

        self.nodes: Dict[str, Node] = {}
        self.slots: Dict[int, List[Node]] = {}
        self.startup_nodes: List[Node] = (
            [] if startup_nodes is None else list(startup_nodes)
        )
        self.startup_nodes_reachable = False
        self.orig_startup_nodes = list(self.startup_nodes)
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps or 25
        self._skip_full_coverage_check = skip_full_coverage_check
        self.nodemanager_follow_cluster = nodemanager_follow_cluster

        if not self.startup_nodes:
            raise RedisClusterException("No startup nodes provided")

    def keys_to_nodes_by_slot(
        self, *keys: ValueT
    ) -> Dict[str, Dict[int, List[ValueT]]]:
        mapping: Dict[str, Dict[int, List[ValueT]]] = {}
        for k in keys:
            if node := self.node_from_slot(hash_slot(b(k))):
                mapping.setdefault(node["name"], {}).setdefault(
                    hash_slot(b(k)), []
                ).append(k)
        return mapping

    def node_from_slot(self, slot: int) -> Optional[Node]:
        for node in self.slots[slot]:
            if node["server_type"] == "master":
                return node
        return None  # noqa

    def all_nodes(self) -> Iterator[Node]:
        yield from self.nodes.values()

    def all_primaries(self) -> Iterator[Node]:
        for node in self.nodes.values():
            if node["server_type"] == "master":
                yield node

    def all_replicas(self) -> Iterator[Node]:
        for node in self.nodes.values():
            if node["server_type"] == "slave":
                yield node

    def random_startup_node(self) -> Node:
        return random.choice(self.startup_nodes)

    def random_startup_node_iter(self, primary: bool = False) -> Iterator[Node]:
        """A generator that returns a random startup nodes"""
        options = (
            list(self.all_primaries())
            if primary
            else (self.startup_nodes if self.startup_nodes_reachable else [])
        )
        while options:
            choice = random.choice(options)
            options.remove(choice)
            yield choice

    def random_node(self) -> Node:
        return random.choice(list(self.nodes.values()))

    def get_redis_link(self, host: str, port: int) -> Redis[Any]:
        from coredis.client import Redis

        allowed_keys = (
            "username",
            "password",
            "encoding",
            "decode_responses",
            "stream_timeout",
            "connect_timeout",
            "retry_on_timeout",
            "ssl_context",
            "parser_class",
            "reader_read_size",
            "loop",
            "protocol_version",
        )
        connection_kwargs = {
            k: v for k, v in self.connection_kwargs.items() if k in allowed_keys
        }
        return Redis(host=host, port=port, **connection_kwargs)  # type: ignore

    async def initialize(self) -> None:
        """
        Initializes the slots cache by asking all startup nodes what the
        current cluster configuration is.

        TODO: Currently the last node will have the last say about how the configuration is setup.
        Maybe it should stop to try after it have correctly covered all slots or when one node is
        reached and it could execute CLUSTER SLOTS command.
        """
        nodes_cache = {}
        tmp_slots: Dict[int, List[Node]] = {}

        all_slots_covered = False
        disagreements: List[str] = []
        self.startup_nodes_reachable = False

        nodes = self.orig_startup_nodes

        # With this option the client will attempt to connect to any of the previous set of nodes
        # instead of the original set of nodes
        if self.nodemanager_follow_cluster:
            nodes = self.startup_nodes

        for node in nodes:
            cluster_slots = {}
            try:
                if node:
                    r = self.get_redis_link(host=node["host"], port=node["port"])
                    cluster_slots = await r.cluster_slots()
                    self.startup_nodes_reachable = True

            except ConnectionError:
                continue
            all_slots_covered = True

            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if len(cluster_slots) == 1 and len(self.startup_nodes) == 1:
                slots = cluster_slots.get((0, HASH_SLOTS - 1))
                assert slots
                single_node_slots = slots[0]
                if len(single_node_slots["host"]) == 0:
                    single_node_slots["host"] = self.startup_nodes[0]["host"]
                    single_node_slots["server_type"] = "master"

            for min_slot, max_slot in cluster_slots:
                _nodes = cast(List[Node], cluster_slots.get((min_slot, max_slot)))
                assert _nodes
                master_node, slave_nodes = _nodes[0], _nodes[1:]

                master_node["host"] = master_node["host"] or node["host"]
                self.set_node_name(master_node)
                nodes_cache[master_node["name"]] = master_node

                for i in range(min_slot, max_slot + 1):
                    if i not in tmp_slots:
                        tmp_slots[i] = [master_node]
                        for slave_node in slave_nodes:
                            self.set_node_name(slave_node)
                            nodes_cache[slave_node["name"]] = slave_node
                            tmp_slots[i].append(slave_node)
                    else:
                        # Validate that 2 nodes want to use the same slot cache setup
                        if tmp_slots[i][0]["name"] != node["name"]:
                            disagreements.append(
                                "{} vs {} on slot: {}".format(
                                    tmp_slots[i][0]["name"], node["name"], i
                                ),
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
            raise RedisClusterException(
                "Redis Cluster cannot be connected. "
                "Please provide at least one reachable node."
            )

        if not all_slots_covered:
            raise RedisClusterException(
                "Not all slots are covered after query all startup_nodes. "
                "{} of {} covered...".format(len(tmp_slots), HASH_SLOTS)
            )

        # Set the tmp variables to the real variables
        self.slots = tmp_slots
        self.nodes = nodes_cache
        self.reinitialize_counter = 0
        self.populate_startup_nodes()

    async def increment_reinitialize_counter(self, ct: int = 1) -> None:
        for _ in range(min(ct, self.reinitialize_steps)):
            self.reinitialize_counter += 1
            if self.reinitialize_counter % self.reinitialize_steps == 0:
                await self.initialize()

    async def node_require_full_coverage(self, node: Node) -> bool:
        r_node = self.get_redis_link(host=node["host"], port=node["port"])
        node_config = await r_node.config_get(["cluster-require-full-coverage"])
        return "yes" in node_config.values()

    async def cluster_require_full_coverage(self, nodes_cache: Dict[str, Node]) -> bool:
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

    def set_node_name(self, node: Node) -> None:
        """
        Formats the name for the given node object

        # TODO: This shold not be constructed this way. It should update the name of the node in
        the node cache dict
        """
        if not node.get("name"):
            node["name"] = f'{nativestr(node["host"])}:{node["port"]}'

    def set_node(
        self,
        host: StringT,
        port: int,
        server_type: Literal["master", "slave"],
    ) -> Node:
        """Updates data for a node"""
        node_name = f"{nativestr(host)}:{port}"
        node = Node(
            host=nativestr(host),
            port=port,
            name=node_name,
            server_type=server_type,
            node_id=None,
        )
        self.nodes[node_name] = node
        return node

    def populate_startup_nodes(self) -> None:
        self.startup_nodes.clear()
        for n in self.nodes.values():
            self.startup_nodes.append(n)

    async def reset(self) -> None:
        await self.initialize()
