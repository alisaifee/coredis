from __future__ import annotations

import asyncio
import os
import random
import threading
import time
import warnings
from typing import Any, cast

from coredis._utils import b, hash_slot
from coredis.connection import ClusterConnection, Connection
from coredis.exceptions import ConnectionError, RedisClusterException
from coredis.pool.basic import ConnectionPool
from coredis.pool.nodemanager import NodeManager
from coredis.typing import Dict, Iterable, Node, Optional, Set, StringT, Type, ValueT


class ClusterConnectionPool(ConnectionPool):
    """
    Custom connection pool for :class:`~coredis.RedisCluster` client
    """

    nodes: NodeManager
    connection_class: Type[ClusterConnection]

    _created_connections_per_node: Dict[str, int]
    _cluster_available_connections: Dict[str, asyncio.Queue[Optional[Connection]]]
    _cluster_in_use_connections: Dict[str, Set[Connection]]

    def __init__(
        self,
        startup_nodes: Optional[Iterable[Node]] = None,
        connection_class: Type[ClusterConnection] = ClusterConnection,
        max_connections: Optional[int] = None,
        max_connections_per_node: bool = False,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        readonly: bool = False,
        read_from_replicas: bool = False,
        max_idle_time: int = 0,
        idle_check_interval: int = 1,
        blocking: bool = False,
        timeout: int = 20,
        **connection_kwargs: Optional[Any],
    ):
        """

        Changes
          - .. versionchanged:: 4.4.0
            - :paramref:`nodemanager_follow_cluster` now defaults to ``True``
          - .. deprecated:: 4.4.0
            - :paramref:`readonly` renamed to :paramref:`read_from_replicas`

        :param max_connections: Maximum number of connections to allow concurrently from this
         client. If the value is ``None`` it will default to 32.
        :param max_connections_per_node: Whether to use the value of :paramref:`max_connections`
         on a per node basis or cluster wide. If ``False`` and :paramref:`blocking` is ``True``
         the per-node connection pools will have a maximum size of :paramref:`max_connections`
         divided by the number of nodes in the cluster.
        :param blocking: If ``True`` the client will block at most :paramref:`timeout` seconds
         if :paramref:`max_connections` is reachd when trying to obtain a connection
        :param timeout: Number of seconds to block if :paramref:`block` is ``True`` when trying to
         obtain a connection.
        :param skip_full_coverage_check:
            Skips the check of cluster-require-full-coverage config, useful for clusters
            without the :rediscommand:`CONFIG` command (For example with AWS Elasticache)
        :param nodemanager_follow_cluster:
            The node manager will during initialization try the last set of nodes that
            it was operating on. This will allow the client to drift along side the cluster
            if the cluster nodes move around alot.
        :param read_from_replicas: If ``True`` the client will route readonly commands to replicas
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
        self.blocking = blocking
        self.blocking_timeout = timeout
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
        self.connection_kwargs["read_from_replicas"] = read_from_replicas
        self.read_from_replicas = read_from_replicas or readonly
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
            if not self.max_connections_per_node and self.max_connections < len(
                self.nodes.nodes
            ):
                warnings.warn(
                    f"The value of max_connections={self.max_connections} "
                    f"should be atleast equal to the number of nodes ({len(self.nodes.nodes)}) "
                    "in the cluster and has been increased by "
                    f"{len(self.nodes.nodes)-self.max_connections} connections."
                )
                self.max_connections = len(self.nodes.nodes)
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
            return await self.get_random_connection()

        slot = hash_slot(b(routing_key))
        if node_type == "replica":
            node = self.get_replica_node_by_slot(slot)
        else:
            node = self.get_primary_node_by_slot(slot)
        self.checkpid()

        try:
            connection = self._cluster_available_connections.get(
                node["name"], self.__default_node_queue()
            ).get_nowait()
        except asyncio.QueueEmpty:
            connection = None
        if not connection:
            connection = self._make_connection(node)
        else:
            if connection.needs_handshake:
                await connection.perform_handshake()

        self._cluster_in_use_connections.setdefault(node["name"], set())
        self._cluster_in_use_connections[node["name"]].add(connection)

        return connection

    def _make_connection(
        self, node: Optional[Node] = None, **options: Optional[ValueT]
    ) -> Connection:
        """Creates a new connection"""

        assert node
        if self.count_all_num_connections(node) >= self.max_connections:
            if self.max_connections_per_node:
                raise ConnectionError(
                    "Too many connection ({}) for node: {}".format(
                        self.count_all_num_connections(node), node["name"]
                    )
                )

            raise ConnectionError("Too many connections")

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

    def __default_node_queue(self) -> asyncio.Queue[Optional[Connection]]:
        q_size = max(
            1,
            int(
                self.max_connections
                if self.max_connections_per_node
                else self.max_connections / len(self.nodes.nodes)
            ),
        )

        q: asyncio.Queue[Optional[Connection]] = asyncio.LifoQueue(q_size)

        # If the queue is non-blocking, we don't need to pre-populate it
        if not self.blocking:
            return q

        if q_size > 2**16:  # noqa
            raise RuntimeError(
                f"Requested unsupported value of max_connections: {q_size} in blocking mode"
            )

        while True:
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                break
        return q

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
                try:
                    self._cluster_available_connections.setdefault(
                        connection.node["name"], self.__default_node_queue()
                    ).put_nowait(connection)
                except asyncio.QueueFull:
                    connection.disconnect()

    def disconnect(self) -> None:
        """Closes all connections in the pool"""
        for node_connections in self._cluster_in_use_connections.values():
            for connection in node_connections:
                connection.disconnect()
        for node, available_connections in list(
            self._cluster_available_connections.items()
        ):
            while True:
                try:
                    if _connection := available_connections.get_nowait():
                        _connection.disconnect()
                except asyncio.QueueEmpty:
                    break
            self._cluster_available_connections.pop(node)

    def count_all_num_connections(self, node: Node) -> int:
        if self.max_connections_per_node:
            return self._created_connections_per_node.get(node["name"], 0)

        return sum(i for i in self._created_connections_per_node.values())

    async def get_random_connection(self, primary: bool = False) -> ClusterConnection:
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
                try:
                    conn = cast(ClusterConnection, conn_list.get_nowait())
                except asyncio.QueueEmpty:
                    conn = None
                if not conn:
                    conn_list.put_nowait(conn)
                else:
                    return conn
        for node in self.nodes.random_startup_node_iter(primary):
            connection = await self.get_connection_by_node(node)

            if connection:
                return connection
        raise RedisClusterException("Cant reach a single startup node.")

    async def get_connection_by_key(self, key: StringT) -> ClusterConnection:
        if not key:
            raise RedisClusterException(
                "No way to dispatch this command to Redis Cluster."
            )

        return await self.get_connection_by_slot(hash_slot(b(key)))

    async def get_connection_by_slot(self, slot: int) -> ClusterConnection:
        """
        Determines what server a specific slot belongs to and return a redis
        object that is connected
        """
        self.checkpid()

        try:
            return await self.get_connection_by_node(self.get_node_by_slot(slot))
        except KeyError:
            return await self.get_random_connection()

    async def get_connection_by_node(self, node: Node) -> ClusterConnection:
        """Gets a connection by node"""
        self.checkpid()
        self.nodes.set_node_name(node)

        if not self.blocking:
            try:
                connection = self._cluster_available_connections.setdefault(
                    node["name"], self.__default_node_queue()
                ).get_nowait()
            except asyncio.QueueEmpty:
                connection = None
        else:
            try:
                connection = await asyncio.wait_for(
                    self._cluster_available_connections.setdefault(
                        node["name"], self.__default_node_queue()
                    ).get(),
                    self.blocking_timeout,
                )
            except asyncio.TimeoutError:
                raise ConnectionError("No connection available.")

        if not connection:
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
        if self.read_from_replicas and command in self.READONLY_COMMANDS:
            return self.get_replica_node_by_slot(slot)
        return self.get_primary_node_by_slot(slot)


class BlockingClusterConnectionPool(ClusterConnectionPool):
    """
    .. versionadded:: 4.3.0

    Blocking connection pool for :class:`~coredis.RedisCluster` client

    .. note:: This is just a convenience subclass of :class:`~coredis.pool.ClusterConnectionPool`
       that sets :paramref:`~coredis.pool.ClusterConnectionPool.blocking` to ``True``
    """

    def __init__(
        self,
        startup_nodes: Optional[Iterable[Node]] = None,
        connection_class: Type[ClusterConnection] = ClusterConnection,
        max_connections: Optional[int] = None,
        max_connections_per_node: bool = False,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        readonly: bool = False,
        read_from_replicas: bool = False,
        max_idle_time: int = 0,
        idle_check_interval: int = 1,
        timeout: int = 20,
        **connection_kwargs: Optional[Any],
    ):
        """

        Changes
          - .. versionchanged:: 4.4.0
            - :paramref:`nodemanager_follow_cluster` now defaults to ``True``
          - .. deprecated:: 4.4.0
            - :paramref:`readonly` renamed to :paramref:`read_from_replicas`

        :param max_connections: Maximum number of connections to allow concurrently from this
         client.
        :param max_connections_per_node: Whether to use the value of :paramref:`max_connections`
         on a per node basis or cluster wide. If ``False`` the per-node connection pools will have
         a maximum size of :paramref:`max_connections` divided by the number of nodes in the
         cluster.
        :param timeout: Number of seconds to block when trying to obtain a connection.
        :param skip_full_coverage_check:
            Skips the check of cluster-require-full-coverage config, useful for clusters
            without the CONFIG command (like aws)
        :param nodemanager_follow_cluster:
            The node manager will during initialization try the last set of nodes that
            it was operating on. This will allow the client to drift along side the cluster
            if the cluster nodes move around alot.
        """
        super().__init__(
            startup_nodes=startup_nodes,
            connection_class=connection_class,
            max_connections=max_connections,
            max_connections_per_node=max_connections_per_node,
            reinitialize_steps=reinitialize_steps,
            skip_full_coverage_check=skip_full_coverage_check,
            nodemanager_follow_cluster=nodemanager_follow_cluster,
            readonly=readonly,
            read_from_replicas=read_from_replicas,
            max_idle_time=max_idle_time,
            idle_check_interval=idle_check_interval,
            timeout=timeout,
            blocking=True,
            **connection_kwargs,
        )
