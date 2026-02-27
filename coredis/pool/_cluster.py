from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from typing import Any

from anyio import TASK_STATUS_IGNORED, fail_after, sleep
from anyio.abc import TaskStatus

from coredis._concurrency import Queue, QueueEmpty, QueueFull
from coredis._utils import logger, query_param_to_bool
from coredis.cluster._discovery import DiscoveryService
from coredis.cluster._layout import ClusterLayout
from coredis.cluster._node import ClusterNodeLocation
from coredis.connection import (
    BaseConnectionParams,
    ClusterConnection,
)
from coredis.connection._tcp import TCPLocation
from coredis.exceptions import RedisError
from coredis.patterns.cache import AbstractCache, ClusterTrackingCache
from coredis.pool._basic import ConnectionPoolParams
from coredis.typing import (
    AsyncGenerator,
    Callable,
    ClassVar,
    Iterable,
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
    startup_nodes: NotRequired[Iterable[Node | TCPLocation]]
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

    connection_class: type[ClusterConnection]

    _cluster_available_connections: dict[TCPLocation, Queue[ClusterConnection]]
    _online_connections: set[ClusterConnection]

    def __init__(
        self,
        startup_nodes: Iterable[Node | TCPLocation],
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
          - .. deprecated:: 6.2.0

            - :paramref:`startup_nodes` should not be passed as dictionaries
               and instead migrate to use instances of :class:`~coredis.connection.TCPLocation`

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
        self._initialized = False
        self._cleanup_interval = 60
        self.timeout = timeout
        self.max_connections = max_connections or 64
        self.max_connections_per_node = max_connections_per_node
        # TODO: Remove support for Node
        if any(isinstance(node, dict) for node in startup_nodes):
            warnings.warn(
                "Use coredis.connection.TCPLocation to specify startup nodes",
                DeprecationWarning,
                stacklevel=2,
            )

        self.startup_nodes = [
            node if isinstance(node, TCPLocation) else TCPLocation(node["host"], node["port"])
            for node in startup_nodes
        ]
        self.cluster_layout = ClusterLayout(
            DiscoveryService(
                startup_nodes=self.startup_nodes,
                skip_full_coverage_check=skip_full_coverage_check,
                follow_cluster=nodemanager_follow_cluster,
                **connection_kwargs,
            ),
            error_threshold=reinitialize_steps or 2,
        )
        self.connection_kwargs = connection_kwargs
        self.read_from_replicas = read_from_replicas or readonly
        # TODO: Use the `max_failures` argument of tracking cache
        self.cache = ClusterTrackingCache(self, _cache) if _cache else None
        self._reset()

        if "stream_timeout" not in self.connection_kwargs:
            self.connection_kwargs["stream_timeout"] = None

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
            ", ".join([node.name for node in list(self.cluster_layout.nodes)]),
        )

    async def _initialize(self) -> None:
        total_nodes = len(list(self.cluster_layout.nodes))
        if not self.max_connections_per_node and self.max_connections < total_nodes:
            warnings.warn(
                f"The value of max_connections={self.max_connections} "
                "should be atleast equal to the number of nodes "
                f"({total_nodes}) in the cluster and has been increased by "
                f"{total_nodes - self.max_connections} connections."
            )
            self.max_connections = total_nodes
        await self.cluster_layout.initialize()
        await self._task_group.start(self.cluster_layout.monitor)
        await self._task_group.start(self._cleanup)
        if self.cache:
            # TODO: handle cache failure so that the pool doesn't die
            #  if the cache fails.
            await self._task_group.start(self.cache.run)

    async def get_connection(
        self, node: ClusterNodeLocation | None = None, primary: bool = True, **options: Any
    ) -> ClusterConnection:
        """
        Acquires a connection from the cluster pool. If no node
        is specified a random node is picked. The connection
        must be returned back to the pool using the :meth:`release`
        method.

        :param node:  The node for which to get a connection from
        :param primary: If False a connection from the replica will be returned
        """
        connection: ClusterConnection
        if node:
            connection = await self._get_connection_by_node(node)
        else:
            connection = await self._get_random_connection(primary=primary)
        return connection

    @asynccontextmanager
    async def acquire(
        self,
        node: ClusterNodeLocation | None = None,
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
            if connection.reusable:
                self._node_pool(connection.location).put_nowait(connection)
            else:
                connection.invalidate()
        except QueueFull:
            pass

    def disconnect(self) -> None:
        for connection in self._online_connections:
            connection.invalidate()
        self._online_connections.clear()

    def _reset(self) -> None:
        """Resets the connection pool back to a clean state"""
        self._cluster_available_connections = {}
        self._online_connections = set()
        self.initialized = False

    async def _wrap_connection(
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
            node_pool = self._node_pool(connection.location)
            self._online_connections.discard(connection)
            if connection in node_pool:
                node_pool.remove(connection)
                node_pool.append_nowait(None)

    async def _make_node_connection(self, node: ClusterNodeLocation) -> ClusterConnection:
        """Creates a new connection to a node"""

        location = TCPLocation(node.host, node.port)
        connection = self.connection_class(
            location=location,
            read_from_replicas=self.read_from_replicas and node.server_type == "replica",
            **self.connection_kwargs,
        )
        if err := await self._task_group.start(self._wrap_connection, connection):
            raise err
        self._online_connections.add(connection)
        return connection

    def _node_pool(self, location: TCPLocation) -> Queue[ClusterConnection]:
        if self._cluster_available_connections.get(location) is None:
            self._cluster_available_connections[location] = self._default_node_queue()
        return self._cluster_available_connections[location]

    def _default_node_queue(
        self,
    ) -> Queue[ClusterConnection]:
        q_size = max(
            1,
            self.max_connections
            if self.max_connections_per_node
            else self.max_connections // len(list(self.cluster_layout.nodes)),
        )
        return Queue[ClusterConnection](q_size)

    async def _get_random_connection(self, primary: bool = False) -> ClusterConnection:
        return await self._get_connection_by_node(self.cluster_layout.random_node(primary))

    async def _get_connection_by_node(self, node: ClusterNodeLocation) -> ClusterConnection:
        location = TCPLocation(node.host, node.port)
        with fail_after(self.timeout):
            connection = await self._node_pool(location).get()

        if not connection or not connection.usable:
            try:
                connection = await self._make_node_connection(node)
            except Exception:
                self._node_pool(location).append_nowait(None)
                raise

        return connection

    async def _cleanup(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        task_status.started()
        while True:
            for location, pool in list(self._cluster_available_connections.items()):
                if list(self.cluster_layout.nodes):
                    if not self.cluster_layout.node_for_location(location):
                        dead_queue = self._cluster_available_connections.pop(location)
                        connections = []
                        try:
                            if c := dead_queue.get_nowait():
                                connections.append(c)
                        except QueueEmpty:
                            break
                        logger.info(
                            f"Node for {location} is no longer in cluster layout, releasing from connection pool (connections: {len(connections)}"
                        )
                        for connection in connections:
                            connection.invalidate()
            await sleep(self._cleanup_interval)
