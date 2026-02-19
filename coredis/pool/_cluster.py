from __future__ import annotations

import random
import warnings
from contextlib import asynccontextmanager
from typing import Any

from anyio import TASK_STATUS_IGNORED, Lock, fail_after
from anyio.abc import TaskStatus

from coredis._concurrency import Queue, QueueFull
from coredis._utils import query_param_to_bool
from coredis.connection import (
    BaseConnectionParams,
    ClusterConnection,
)
from coredis.connection._tcp import TCPLocation
from coredis.exceptions import RedisClusterException, RedisError
from coredis.globals import READONLY_COMMANDS
from coredis.patterns.cache import AbstractCache, ClusterTrackingCache
from coredis.pool._basic import ConnectionPoolParams
from coredis.pool._nodemanager import NodeManager
from coredis.typing import (
    AsyncGenerator,
    Callable,
    ClassVar,
    Iterable,
    KeyT,
    ManagedNode,
    Node,
    NotRequired,
    Self,
    Unpack,
)

from ._base import BaseConnectionPool


class ClusterConnectionPoolParams(ConnectionPoolParams[ClusterConnection]):
    """
    Parameters accepted by :class:`coredis.pool.ClusterConnectionPool`
    """

    #: The initial collection of nodes to use to map the cluster solts to individual primary & replica nodes.
    startup_nodes: NotRequired[Iterable[Node]]
    #: Skips the check of cluster-require-full-coverage config, useful for clusters
    #: without the :rediscommand:`CONFIG` command (For example with AWS Elasticache)
    skip_full_coverage_check: NotRequired[bool]
    #: Whether to use the value of ``max_connections``
    #: on a per node basis or cluster wide. If ``False``  the per-node connection pools will have
    #: a maximum size of :paramref:`max_connections` divided by the number of nodes in the cluster.
    max_connections_per_node: NotRequired[bool]
    #: If ``True`` connections to replicas will be returned for readonly commands
    read_from_replicas: NotRequired[bool]


class ClusterConnectionPool(BaseConnectionPool[ClusterConnection]):
    """
    Custom connection pool for :class:`~coredis.RedisCluster` client
    """

    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        dict[str, Callable[..., int | float | bool | str | None]]
    ] = {
        **BaseConnectionPool.URL_QUERY_ARGUMENT_PARSERS,
        "max_connections_per_node": query_param_to_bool,
        "reinitialize_steps": int,
        "skip_full_coverage_check": query_param_to_bool,
        "read_from_replicas": query_param_to_bool,
    }

    nodes: NodeManager
    connection_class: type[ClusterConnection]

    _cluster_available_connections: dict[TCPLocation, Queue[ClusterConnection]]
    _online_connections: set[ClusterConnection]

    def __init__(
        self,
        startup_nodes: Iterable[Node] | None = None,
        *,
        connection_class: type[ClusterConnection] = ClusterConnection,
        max_connections: int | None = None,
        max_connections_per_node: bool = False,
        reinitialize_steps: int | None = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        readonly: bool = False,
        read_from_replicas: bool = False,
        timeout: float | None = None,
        _cache: AbstractCache | None = None,
        **connection_kwargs: Unpack[BaseConnectionParams],
    ):
        """
        Cluster aware connection pool that tracks and manages sub pools for each node
        in the redis cluster

        Changes
          - .. versionchanged:: 4.4.0

            - :paramref:`nodemanager_follow_cluster` now defaults to ``True``

          - .. deprecated:: 4.4.0

            - :paramref:`readonly` renamed to :paramref:`read_from_replicas`

        :param startup_nodes: The initial collection of nodes to use to map the cluster
         solts to individual primary & replica nodes.
        :param connection_class: The connection class to use when creating new connections
        :param max_connections: Maximum number of connections to allow concurrently from this
         client. If the value is ``None`` it will default to 64.
        :param max_connections_per_node: Whether to use the value of :paramref:`max_connections`
         on a per node basis or cluster wide. If ``False``  the per-node connection pools will have
         a maximum size of :paramref:`max_connections` divided by the number of nodes in the cluster.
        :param timeout: Number of seconds to block when trying to obtain a connection.
        :param skip_full_coverage_check:
         Skips the check of cluster-require-full-coverage config, useful for clusters
         without the :rediscommand:`CONFIG` command (For example with AWS Elasticache)
        :param nodemanager_follow_cluster: The node manager will during initialization try
         the last set of nodes that it was operating on. This will allow the client to drift
         along side the cluster if the cluster nodes move around alot.
        :param read_from_replicas: If ``True`` connections to replicas will be returned for readonly
         commands
        :param connection_kwargs: arguments to pass to the :paramref:`connection_class`
         constructor when creating a new connection
        """
        super().__init__(
            connection_class=connection_class,
            **connection_kwargs,
        )
        self.initialized = False
        self.timeout = timeout
        self.max_connections = max_connections or 64
        self.max_connections_per_node = max_connections_per_node
        self.nodes = NodeManager(
            startup_nodes,
            reinitialize_steps=reinitialize_steps,
            skip_full_coverage_check=skip_full_coverage_check,
            max_connections=self.max_connections,
            nodemanager_follow_cluster=nodemanager_follow_cluster,
            **connection_kwargs,
        )
        self.connection_kwargs = connection_kwargs
        self.read_from_replicas = read_from_replicas or readonly
        self.startup_nodes = startup_nodes
        # TODO: Use the `max_failures` argument of tracking cache
        self.cache = ClusterTrackingCache(self, _cache) if _cache else None
        self._reset()

        if "stream_timeout" not in self.connection_kwargs:
            self.connection_kwargs["stream_timeout"] = None
        self._init_lock = Lock()

    @classmethod
    def from_url(
        cls: type[Self],
        url: str,
        *,
        decode_components: bool = False,
        **kwargs: Unpack[ClusterConnectionPoolParams],
    ) -> Self:
        """
        Returns a cluster connection pool configured from the given URL.
        """
        location, merged_options = cls._parse_url(
            url, decode_components, kwargs, ClusterConnectionPoolParams
        )
        if "startup_nodes" not in merged_options and location:
            assert isinstance(location, TCPLocation)
            merged_options["startup_nodes"] = [Node(host=location.host, port=location.port)]
        merged_options["connection_class"] = ClusterConnection

        return cls(
            **merged_options,
        )

    def __repr__(self) -> str:
        """
        Returns a string with all unique ip:port combinations that this pool
        is connected to
        """

        return "{}<{}>".format(
            type(self).__name__,
            ", ".join([node.name for node in self.nodes.startup_nodes]),
        )

    async def _initialize(self) -> None:
        await self.refresh_cluster_mapping()
        if self.cache:
            # TODO: handle cache failure so that the pool doesn't die
            #  if the cache fails.
            await self._task_group.start(self.cache.run)

    async def refresh_cluster_mapping(self, forced: bool = False) -> None:
        if not self.initialized or forced:
            async with self._init_lock:
                if self.initialized and not forced:
                    return
                await self.nodes.initialize()
                for node in self._cluster_available_connections:
                    if ManagedNode(node.host, node.port).name not in self.nodes.nodes:
                        self._cluster_available_connections.pop(node)
                if not self.max_connections_per_node and self.max_connections < len(
                    self.nodes.nodes
                ):
                    warnings.warn(
                        f"The value of max_connections={self.max_connections} "
                        "should be atleast equal to the number of nodes "
                        f"({len(self.nodes.nodes)}) in the cluster and has been increased by "
                        f"{len(self.nodes.nodes) - self.max_connections} connections."
                    )
                    self.max_connections = len(self.nodes.nodes)
                self.initialized = True

    async def get_connection(
        self, node: ManagedNode | None = None, primary: bool = True, **options: Any
    ) -> ClusterConnection:
        """
        Acquires a connection from the cluster pool. If no node
        is specified a random node is picked. The connection
        must be returned back to the pool using the :meth:`release`
        method.

        :param shared: Whether the connection can be shared with other
         requests or is required for dedicated/blocking use.
        :param node:  The node for which to get a connection from
        :param primary: If False a connection from the replica will be returned
        """
        connection: ClusterConnection
        if node:
            connection = await self.__get_connection_by_node(node)
        else:
            connection = await self.__get_random_connection(primary=primary)
        return connection

    @asynccontextmanager
    async def acquire(
        self,
        node: ManagedNode | None = None,
        primary: bool = True,
        **options: Any,
    ) -> AsyncGenerator[ClusterConnection]:
        """
        Acquires a connection from the cluster pool. If no node
        is specified a random node is picked. The connection
        will be automatically released back to the pool when
        the context manager exits.

        :param node:  The node for which to get a connection from
        :param primary: If False a connection from the replica will be returned
        """
        connection = await self.get_connection(node=node, primary=primary, **options)
        yield connection
        self.release(connection)

    def release(self, connection: ClusterConnection) -> None:
        """Releases the connection back to the pool"""
        assert isinstance(connection, ClusterConnection)
        try:
            if connection.usable:
                self.__node_pool(connection.location).put_nowait(connection)
        except QueueFull:
            pass

    def disconnect(self) -> None:
        for connection in self._online_connections:
            connection.terminate()
        self._online_connections.clear()

    def _reset(self) -> None:
        """Resets the connection pool back to a clean state"""
        self._cluster_available_connections = {}
        self._online_connections = set()
        self.initialized = False

    async def __wrap_connection(
        self,
        connection: ClusterConnection,
        *,
        task_status: TaskStatus[None | Exception] = TASK_STATUS_IGNORED,
    ) -> None:
        try:
            await connection.run(task_status=task_status)
        except RedisError as error:
            # Only coredis.exception.RedisError is explictly caught and returned with the task status
            # As these are clear signals that an error case was handled by the connection
            task_status.started(error)
        finally:
            node_pool = self.__node_pool(connection.location)
            self._online_connections.discard(connection)
            if connection in node_pool:
                node_pool.remove(connection)
                node_pool.append_nowait(None)

    async def __make_node_connection(self, location: TCPLocation) -> ClusterConnection:
        """Creates a new connection to a node"""

        connection = self.connection_class(
            location=location,
            read_from_replicas=self.read_from_replicas,
            **self.connection_kwargs,
        )
        if err := await self._task_group.start(self.__wrap_connection, connection):
            raise err
        self._online_connections.add(connection)
        return connection

    def __node_pool(self, location: TCPLocation) -> Queue[ClusterConnection]:
        if self._cluster_available_connections.get(location) is None:
            self._cluster_available_connections[location] = self.__default_node_queue()
        return self._cluster_available_connections[location]

    def __default_node_queue(
        self,
    ) -> Queue[ClusterConnection]:
        q_size = max(
            1,
            self.max_connections
            if self.max_connections_per_node
            else self.max_connections // len(self.nodes.nodes),
        )
        return Queue[ClusterConnection](q_size)

    async def __get_random_connection(self, primary: bool = False) -> ClusterConnection:
        """Opens new connection to random redis server in the cluster"""

        for node in self.nodes.random_startup_node_iter(primary):
            connection = await self.__get_connection_by_node(node)

            if connection:
                return connection
        raise RedisClusterException("Cant reach a single startup node.")

    async def __get_connection_by_node(self, node: ManagedNode) -> ClusterConnection:
        """Gets a connection by node"""
        location = TCPLocation(node.host, node.port)
        with fail_after(self.timeout):
            connection = await self.__node_pool(location).get()

        if not connection or not connection.usable:
            connection = await self.__make_node_connection(location)

        return connection

    def get_primary_node_by_slots(self, slots: list[int]) -> ManagedNode:
        nodes = {self.nodes.slots[slot][0].node_id for slot in slots}

        if len(nodes) == 1:
            return self.nodes.slots[slots[0]][0]
        else:
            raise RedisClusterException(f"Unable to map slots {slots} to a single node")

    def get_replica_node_by_slots(
        self, slots: list[int], replica_only: bool = False
    ) -> ManagedNode:
        nodes = {self.nodes.slots[slot][0].node_id for slot in slots}

        if len(nodes) == 1:
            slot = slots[0]

            if replica_only:
                return random.choice(
                    [node for node in self.nodes.slots[slot] if node.server_type != "primary"]
                )
            else:
                return random.choice(self.nodes.slots[slot])
        else:
            raise RedisClusterException(f"Unable to map slots {slots} to a single node")

    def get_node_by_slot(self, slot: int, command: bytes | None = None) -> ManagedNode:
        if self.read_from_replicas and command in READONLY_COMMANDS:
            return self.get_replica_node_by_slots([slot])

        return self.get_primary_node_by_slots([slot])

    def get_node_by_slots(self, slots: list[int], command: bytes | None = None) -> ManagedNode:
        if self.read_from_replicas and command in READONLY_COMMANDS:
            return self.get_replica_node_by_slots(slots)

        return self.get_primary_node_by_slots(slots)

    def get_node_by_keys(self, keys: list[KeyT]) -> ManagedNode:
        return self.get_node_by_slots(list(self.nodes.keys_to_slots(*keys)))
