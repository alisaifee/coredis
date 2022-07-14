from __future__ import annotations

import asyncio
import os
import random
import threading
import time
from itertools import chain
from typing import Any, cast

from coredis._utils import b, hash_slot
from coredis.connection import ClusterConnection, Connection
from coredis.exceptions import RedisClusterException
from coredis.pool.basic import ConnectionPool
from coredis.pool.nodemanager import NodeManager
from coredis.typing import (
    Dict,
    Iterable,
    List,
    Node,
    Optional,
    Set,
    StringT,
    Type,
    ValueT,
)


class ClusterConnectionPool(ConnectionPool):
    """
    Custom connection pool for :class:`~coredis.RedisCluster` client
    """

    nodes: NodeManager
    connection_class: Type[ClusterConnection]

    _created_connections_per_node: Dict[str, int]
    _cluster_available_connections: Dict[str, List[Connection]]
    _cluster_in_use_connections: Dict[str, Set[Connection]]

    def __init__(
        self,
        startup_nodes: Optional[Iterable[Node]] = None,
        connection_class: Type[ClusterConnection] = ClusterConnection,
        max_connections: Optional[int] = None,
        max_connections_per_node: bool = False,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = False,
        readonly: bool = False,
        max_idle_time: int = 0,
        idle_check_interval: int = 1,
        **connection_kwargs: Optional[Any],
    ):
        """
        :param skip_full_coverage_check:
            Skips the check of cluster-require-full-coverage config, useful for clusters
            without the CONFIG command (like aws)
        :param nodemanager_follow_cluster:
            The node manager will during initialization try the last set of nodes that
            it was operating on. This will allow the client to drift along side the cluster
            if the cluster nodes move around alot.
        """
        super().__init__(
            connection_class=connection_class, max_connections=max_connections
        )

        # Special case to make from_url method compliant with cluster setting.
        # from_url method will send in the ip and port through a different variable then the
        # regular startup_nodes variable.

        if startup_nodes is None:
            host = connection_kwargs.pop("host", None)
            port = connection_kwargs.pop("port", None)
            if host and port:
                startup_nodes = [
                    Node(
                        host=str(host),
                        port=int(port),
                        name="",
                        server_type=None,
                        node_id=None,
                    )
                ]
        self.max_connections = max_connections or 2**31
        self.max_connections_per_node = max_connections_per_node
        self.nodes = NodeManager(
            startup_nodes,
            reinitialize_steps=reinitialize_steps,
            skip_full_coverage_check=skip_full_coverage_check,
            max_connections=self.max_connections,
            nodemanager_follow_cluster=nodemanager_follow_cluster,
            **connection_kwargs,  # type: ignore
        )
        self.initialized = False

        self.connection_kwargs = connection_kwargs
        self.connection_kwargs["readonly"] = readonly
        self.readonly = readonly
        self.max_idle_time = max_idle_time
        self.idle_check_interval = idle_check_interval
        self.reset()

        if "stream_timeout" not in self.connection_kwargs:
            self.connection_kwargs["stream_timeout"] = None

    def __repr__(self) -> str:
        """
        Returns a string with all unique ip:port combinations that this pool
        is connected to
        """

        return "{}<{}>".format(
            type(self).__name__,
            ", ".join(
                [
                    self.connection_class.describe(dict(node))
                    for node in self.nodes.startup_nodes
                ]
            ),
        )

    async def initialize(self) -> None:
        if not self.initialized:
            await self.nodes.initialize()
            self.initialized = True

    async def disconnect_on_idle_time_exceeded(self, connection: Connection) -> None:
        assert isinstance(connection, ClusterConnection)
        while True:
            if (
                time.time() - connection.last_active_at > self.max_idle_time
                and not connection.awaiting_response
            ):
                connection.disconnect()
                node = connection.node
                connections = self._cluster_available_connections.get(node["name"])

                if connections:
                    connections.remove(connection)
                self._created_connections_per_node[node["name"]] -= 1

                break
            await asyncio.sleep(self.idle_check_interval)

    def reset(self) -> None:
        """Resets the connection pool back to a clean state"""
        self.pid = os.getpid()
        self._created_connections_per_node = {}
        self._cluster_available_connections = {}
        self._cluster_in_use_connections = {}
        self._check_lock = threading.Lock()
        self.initialized = False

    def checkpid(self) -> None:  # noqa
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lockself.

                    return
                self.disconnect()
                self.reset()

    async def get_connection(
        self,
        command_name: Optional[bytes] = None,
        *keys: ValueT,
        **options: Optional[ValueT],
    ) -> Connection:
        # Only pubsub command/connection should be allowed here

        if command_name != b"pubsub":
            raise RedisClusterException(
                "Only 'pubsub' commands can use get_connection()"
            )

        routing_key = options.pop("channel", None)
        node_type = options.pop("node_type", "master")

        if not routing_key:
            return self.get_random_connection()

        slot = hash_slot(b(routing_key))
        if node_type == "replica":
            node = self.get_replica_node_by_slot(slot)
        else:
            node = self.get_primary_node_by_slot(slot)
        self.checkpid()

        try:
            connection = self._cluster_available_connections.get(node["name"], []).pop()
            if connection.needs_handshake:
                await connection.perform_handshake()
        except IndexError:
            connection = self._make_connection(node)

        if node["name"] not in self._cluster_in_use_connections:
            self._cluster_in_use_connections[node["name"]] = set()

        self._cluster_in_use_connections[node["name"]].add(connection)

        return connection

    def _make_connection(
        self, node: Optional[Node] = None, **options: Optional[ValueT]
    ) -> Connection:
        """Creates a new connection"""

        assert node
        if self.count_all_num_connections(node) >= self.max_connections:
            if self.max_connections_per_node:
                raise RedisClusterException(
                    "Too many connection ({}) for node: {}".format(
                        self.count_all_num_connections(node), node["name"]
                    )
                )

            raise RedisClusterException("Too many connections")

        self._created_connections_per_node.setdefault(node["name"], 0)
        self._created_connections_per_node[node["name"]] += 1
        connection = self.connection_class(
            host=node["host"],
            port=node["port"],
            **self.connection_kwargs,  # type: ignore
        )

        # Must store node in the connection to make it eaiser to track
        connection.node = node

        if self.max_idle_time > self.idle_check_interval > 0:
            # do not await the future
            asyncio.ensure_future(self.disconnect_on_idle_time_exceeded(connection))

        return connection

    def release(self, connection: Connection) -> None:
        """Releases the connection back to the pool"""
        assert isinstance(connection, ClusterConnection)

        self.checkpid()

        if connection.pid == self.pid:
            # Remove the current connection from _in_use_connection and add it back to the available
            # pool. There is cases where the connection is to be removed but it will not exist and
            # there must be a safe way to remove
            i_c = self._cluster_in_use_connections.get(connection.node["name"], set())

            if connection in i_c:
                i_c.remove(connection)
            else:
                pass
            # discard connection with unread response

            if connection.awaiting_response:
                connection.disconnect()
                # reduce node connection count in case of too many connection error raised

                if self._created_connections_per_node.get(connection.node["name"]):
                    self._created_connections_per_node[connection.node["name"]] -= 1
            else:
                self._cluster_available_connections.setdefault(
                    connection.node["name"], []
                ).append(connection)

    def disconnect(self) -> None:
        """Closes all connections in the pool"""
        all_conns = chain(
            self._cluster_available_connections.values(),
            self._cluster_in_use_connections.values(),
        )

        for node_connections in all_conns:
            for connection in node_connections:
                connection.disconnect()

    def count_all_num_connections(self, node: Node) -> int:
        if self.max_connections_per_node:
            return self._created_connections_per_node.get(node["name"], 0)

        return sum(i for i in self._created_connections_per_node.values())

    def get_random_connection(self, primary: bool = False) -> ClusterConnection:
        """Opens new connection to random redis server in the cluster"""
        if self._cluster_available_connections:
            filter = []
            if primary:
                filter = [node["name"] for node in self.nodes.all_primaries()]
            node_name = random.choice(
                list(
                    node
                    for node in self._cluster_available_connections.keys()
                    if not filter or node in filter
                )
            )
            conn_list = None
            if node_name:
                conn_list = self._cluster_available_connections[node_name]

            if conn_list:
                return cast(ClusterConnection, conn_list.pop())

        for node in self.nodes.random_startup_node_iter(primary):
            connection = self.get_connection_by_node(node)

            if connection:
                return connection
        raise RedisClusterException("Cant reach a single startup node.")

    def get_connection_by_key(self, key: StringT) -> ClusterConnection:
        if not key:
            raise RedisClusterException(
                "No way to dispatch this command to Redis Cluster."
            )

        return self.get_connection_by_slot(hash_slot(b(key)))

    def get_connection_by_slot(self, slot: int) -> ClusterConnection:
        """
        Determines what server a specific slot belongs to and return a redis
        object that is connected
        """
        self.checkpid()

        try:
            return self.get_connection_by_node(self.get_node_by_slot(slot))
        except KeyError:
            return self.get_random_connection()

    def get_connection_by_node(self, node: Node) -> ClusterConnection:
        """Gets a connection by node"""
        self.checkpid()
        self.nodes.set_node_name(node)

        try:
            # Try to get connection from existing pool
            connection = self._cluster_available_connections.get(node["name"], []).pop()
        except IndexError:
            connection = self._make_connection(node)

        self._cluster_in_use_connections.setdefault(node["name"], set()).add(connection)

        return cast(ClusterConnection, connection)

    def get_primary_node_by_slot(self, slot: int) -> Node:
        return self.nodes.slots[slot][0]

    def get_replica_node_by_slot(self, slot: int, replica_only: bool = False) -> Node:
        if replica_only:
            return random.choice(
                [
                    node
                    for node in self.nodes.slots[slot]
                    if node["server_type"] != "master"
                ]
            )
        else:
            return random.choice(self.nodes.slots[slot])

    def get_node_by_slot(self, slot: int, command: Optional[bytes] = None) -> Node:
        if self.readonly and command in self.READONLY_COMMANDS:
            return self.get_replica_node_by_slot(slot)
        return self.get_primary_node_by_slot(slot)
