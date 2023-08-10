from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import textwrap
from abc import ABCMeta
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, cast, overload

from deprecated.sphinx import versionadded

from coredis._utils import b, hash_slot
from coredis.cache import AbstractCache, SupportsClientTracking
from coredis.client.basic import Client, Redis
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.commands.pubsub import ClusterPubSub, ShardedPubSub
from coredis.connection import RedisSSLContext
from coredis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    MovedError,
    RedisClusterException,
    TimeoutError,
    TryAgainError,
    WatchError,
)
from coredis.globals import MODULE_GROUPS, READONLY_COMMANDS
from coredis.pool import ClusterConnectionPool
from coredis.pool.nodemanager import ManagedNode
from coredis.response._callbacks import AsyncPreProcessingCallback, NoopCallback
from coredis.retry import CompositeRetryPolicy, ConstantRetryPolicy, RetryPolicy
from coredis.typing import (
    AnyStr,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Coroutine,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Node,
    Optional,
    Parameters,
    ParamSpec,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Type,
    TypeVar,
    ValueT,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.pipeline


class ClusterMeta(ABCMeta):
    ROUTING_FLAGS: Dict[bytes, NodeFlag]
    SPLIT_FLAGS: Dict[bytes, NodeFlag]
    RESULT_CALLBACKS: Dict[bytes, Callable[..., ResponseType]]
    NODE_FLAG_DOC_MAPPING = {
        NodeFlag.PRIMARIES: "all primaries",
        NodeFlag.REPLICAS: "all replicas",
        NodeFlag.RANDOM: "a random node",
        NodeFlag.ALL: "all nodes",
        NodeFlag.SLOT_ID: "one or more nodes based on the slots provided",
    }

    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> ClusterMeta:
        kls = super().__new__(cls, name, bases, namespace)
        methods = dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))
        for module in MODULE_GROUPS:
            methods.update(
                dict(
                    (f"{module.MODULE}.{k[0]}", k[1])
                    for k in inspect.getmembers(module)
                    if inspect.isfunction(k[1])
                )
            )
        for method_name, method in methods.items():
            doc_addition = ""
            cmd = getattr(method, "__coredis_command", None)
            if cmd:
                if cmd.cluster.route:
                    kls.ROUTING_FLAGS[cmd.command] = cmd.cluster.route
                    aggregate_note = ""
                    if cmd.cluster.multi_node:
                        if cmd.cluster.combine:
                            aggregate_note = (
                                f"and return {cmd.cluster.combine.response_policy}"
                            )
                        else:
                            aggregate_note = (
                                "and a mapping of nodes to results will be returned"
                            )
                    doc_addition = f"""
.. admonition:: Cluster note

   The command will be run on **{cls.NODE_FLAG_DOC_MAPPING[cmd.cluster.route]}** {aggregate_note}
                    """
                elif cmd.cluster.split and cmd.cluster.combine:
                    kls.SPLIT_FLAGS[cmd.command] = cmd.cluster.split
                    doc_addition = f"""
.. admonition:: Cluster note

   The command will be run on **{cls.NODE_FLAG_DOC_MAPPING[cmd.cluster.split]}**
   by distributing the keys to the appropriate nodes and return
   {cmd.cluster.combine.response_policy}.

   To disable this behavior set :paramref:`RedisCluster.non_atomic_cross_slot` to ``False``
                """
                if cmd.cluster.multi_node:
                    kls.RESULT_CALLBACKS[cmd.command] = cmd.cluster.combine
            if doc_addition and not hasattr(method, "__cluster_docs"):
                if not getattr(method, "__coredis_module", None):

                    def __w(
                        func: Callable[P, Awaitable[R]]
                    ) -> Callable[P, Awaitable[R]]:
                        @functools.wraps(func)
                        async def _w(*a: P.args, **k: P.kwargs) -> R:
                            return await func(*a, **k)

                        _w.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                    """
                        return _w

                    wrapped = __w(method)
                    setattr(wrapped, "__cluster_docs", doc_addition)
                    setattr(kls, method_name, wrapped)
                else:
                    method.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                    """
                    setattr(method, "__cluster_docs", doc_addition)
        return kls


RedisClusterT = TypeVar("RedisClusterT", bound="RedisCluster[Any]")
RedisClusterStringT = TypeVar("RedisClusterStringT", bound="RedisCluster[str]")
RedisClusterBytesT = TypeVar("RedisClusterBytesT", bound="RedisCluster[bytes]")


class RedisCluster(
    Client[AnyStr],
    metaclass=ClusterMeta,
):
    MAX_RETRIES = 16
    ROUTING_FLAGS: Dict[bytes, NodeFlag] = {}
    SPLIT_FLAGS: Dict[bytes, NodeFlag] = {}
    RESULT_CALLBACKS: Dict[bytes, Callable[..., Any]] = {}

    connection_pool: ClusterConnectionPool

    @overload
    def __init__(
        self: RedisCluster[bytes],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        *,
        startup_nodes: Optional[Iterable[Node]] = ...,
        stream_timeout: Optional[float] = ...,
        connect_timeout: Optional[float] = ...,
        ssl: bool = ...,
        ssl_context: Optional[SSLContext] = ...,
        ssl_keyfile: Optional[str] = ...,
        ssl_certfile: Optional[str] = ...,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = ...,
        ssl_check_hostname: Optional[bool] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        read_from_replicas: bool = ...,
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[False] = ...,
        connection_pool: Optional[ClusterConnectionPool] = ...,
        connection_pool_cls: Type[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self: RedisCluster[str],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        *,
        startup_nodes: Optional[Iterable[Node]] = ...,
        stream_timeout: Optional[float] = ...,
        connect_timeout: Optional[float] = ...,
        ssl: bool = ...,
        ssl_context: Optional[SSLContext] = ...,
        ssl_keyfile: Optional[str] = ...,
        ssl_certfile: Optional[str] = ...,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = ...,
        ssl_check_hostname: Optional[bool] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        read_from_replicas: bool = ...,
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[True],
        connection_pool: Optional[ClusterConnectionPool] = ...,
        connection_pool_cls: Type[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        **kwargs: Any,
    ) -> None:
        ...

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *,
        startup_nodes: Optional[Iterable[Node]] = None,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        ssl: bool = False,
        ssl_context: Optional[SSLContext] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = None,
        ssl_check_hostname: Optional[bool] = None,
        ssl_ca_certs: Optional[str] = None,
        max_connections: int = 32,
        max_connections_per_node: bool = False,
        readonly: bool = False,
        read_from_replicas: bool = False,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        decode_responses: bool = False,
        connection_pool: Optional[ClusterConnectionPool] = None,
        connection_pool_cls: Type[ClusterConnectionPool] = ClusterConnectionPool,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        non_atomic_cross_slot: bool = True,
        cache: Optional[AbstractCache] = None,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        retry_policy: RetryPolicy = CompositeRetryPolicy(
            ConstantRetryPolicy((ClusterDownError,), 2, 0.1),
            ConstantRetryPolicy(
                (
                    ConnectionError,
                    TimeoutError,
                ),
                2,
                0.1,
            ),
        ),
        **kwargs: Any,
    ) -> None:
        """

        Changes
          - .. versionadded:: 4.12.0

            - :paramref:`retry_policy`
            - :paramref:`noevict`
            - :paramref:`notouch`
            - :meth:`RedisCluster.ensure_persistence` context manager
            - Redis Module support

              - RedisJSON: :attr:`RedisCluster.json`
              - RedisBloom:

                - BloomFilter: :attr:`RedisCluster.bf`
                - CuckooFilter: :attr:`RedisCluster.cf`
                - CountMinSketch: :attr:`RedisCluster.cms`
                - TopK: :attr:`RedisCluster.topk`
                - TDigest: :attr:`RedisCluster.tdigest`
              - RedisTimeSeries: :attr:`RedisCluster.timeseries`
              - RedisGraph: :attr:`RedisCluster.graph`
              - RediSearch:

                - Search & Aggregation: :attr:`RedisCluster.search`
                - Autocomplete: Added :attr:`RedisCluster.autocomplete`

          - .. versionchanged:: 4.4.0

            - :paramref:`nodemanager_follow_cluster` now defaults to ``True``

          - .. deprecated:: 4.4.0

            - The :paramref:`readonly` argument is deprecated in favour of
              :paramref:`read_from_replicas`

          - .. versionadded:: 4.3.0

            - Added :paramref:`connection_pool_cls`

          - .. versionchanged:: 4.0.0

            - :paramref:`non_atomic_cross_slot` defaults to ``True``
            - :paramref:`protocol_version`` defaults to ``3``

          - .. versionadded:: 3.11.0

            - Added :paramref:`noreply`

          - .. versionadded:: 3.10.0

            - Synchronized ssl constructor parameters with :class:`coredis.Redis`

          - .. versionadded:: 3.9.0

            - If :paramref:`cache` is provided the client will check & populate
              the cache for read only commands and invalidate it for commands
              that could change the key(s) in the request.

          - .. versionadded:: 3.6.0

            - The :paramref:`non_atomic_cross_slot` parameter was added

          - .. versionchanged:: 3.5.0

            - The :paramref:`verify_version` parameter now defaults to ``True``

          - .. versionadded:: 3.1.0

            - The :paramref:`protocol_version` and :paramref:`verify_version`
              parameters were added

        :param host: Can be used to point to a startup node
        :param port: Can be used to point to a startup node
        :param startup_nodes: List of nodes that initial bootstrapping can be done
         from
        :param stream_timeout: Timeout (seconds) when reading responses from the server
        :param connect_timeout: Timeout (seconds) for establishing a connection to the server
        :param ssl: Whether to use an SSL connection
        :param ssl_context: If provided the :class:`ssl.SSLContext` will be used when
         establishing the connection. Otherwise either the default context (if no other
         ssl related parameters are provided) or a custom context based on the other
         ``ssl_*`` parameters will be used.
        :param ssl_keyfile: Path of the private key to use
        :param ssl_certfile: Path to the certificate corresponding to :paramref:`ssl_keyfile`
        :param ssl_cert_reqs: Whether to try to verify the server's certificates and
         how to behave if verification fails (See :attr:`ssl.SSLContext.verify_mode`).
        :param ssl_check_hostname: Whether to enable hostname checking when establishing
         an ssl connection.
        :param ssl_ca_certs: Path to a concatenated certificate authority file or a directory
         containing several CA certifcates to use  for validating the server's certificates
         when :paramref:`ssl_cert_reqs` is not ``"none"``
         (See :meth:`ssl.SSLContext.load_verify_locations`).
        :param max_connections: Maximum number of connections that should be kept open at one time
        :param max_connections_per_node:
        :param read_from_replicas: If ``True`` the client will route readonly commands to replicas
        :param reinitialize_steps: Number of moved errors that result in a cluster
         topology refresh using the startup nodes provided
        :param skip_full_coverage_check: Skips the check of cluster-require-full-coverage config,
         useful for clusters without the CONFIG command (like aws)
        :param nodemanager_follow_cluster: The node manager will during initialization try the
         last set of nodes that it was operating on. This will allow the client to drift along
         side the cluster if the cluster nodes move around alot.
        :param decode_responses: If ``True`` string responses from the server
         will be decoded using :paramref:`encoding` before being returned.
         (See :ref:`handbook/encoding:encoding/decoding`)
        :param connection_pool: The connection pool instance to use. If not provided
         a new pool will be assigned to this client.
        :param connection_pool_cls: The connection pool class to use when constructing
         a connection pool for this instance.
        :param protocol_version: Whether to use the RESP (``2``) or RESP3 (``3``)
         protocol for parsing responses from the server (Default ``3``).
         (See :ref:`handbook/response:redis response`)
        :param verify_version: Validate redis server version against the documented
         version introduced before executing a command and raises a
         :exc:`CommandNotSupportedError` error if the required version is higher than
         the reported server version
        :param non_atomic_cross_slot: If ``True`` certain commands that can operate
         on multiple keys (cross slot) will be split across the relevant nodes by
         mapping the keys to the appropriate slot and the result merged before being
         returned.
        :param cache: If provided the cache will be used to avoid requests for read only
         commands if the client has already requested the data and it hasn't been invalidated.
         The cache is responsible for any mutations to the keys that happen outside of this client
        :param noreply: If ``True`` the client will not request a response for any
         commands sent to the server.
        :param noevict: Ensures that connections from the client will be excluded from the
         client eviction process even if we're above the configured client eviction threshold.
        :param notouch: Ensures that commands sent by the client will not alter the LRU/LFU
         of the keys they access.
        :param retry_policy: The retry policy to use when interacting with the cluster
        """

        if "db" in kwargs:  # noqa
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        if connection_pool:
            pool = connection_pool
        else:
            startup_nodes = [] if startup_nodes is None else list(startup_nodes)

            # Support host/port as argument

            if host:
                startup_nodes.append(
                    Node(
                        host=host,
                        port=port if port else 7000,
                    )
                )
            if ssl_context is not None:
                kwargs["ssl_context"] = ssl_context
            elif ssl:
                ssl_context = RedisSSLContext(
                    ssl_keyfile,
                    ssl_certfile,
                    ssl_cert_reqs,
                    ssl_ca_certs,
                    ssl_check_hostname,
                ).get()
                kwargs["ssl_context"] = ssl_context

            pool = connection_pool_cls(
                startup_nodes=startup_nodes,
                max_connections=max_connections,
                reinitialize_steps=reinitialize_steps,
                max_connections_per_node=max_connections_per_node,
                skip_full_coverage_check=skip_full_coverage_check,
                nodemanager_follow_cluster=nodemanager_follow_cluster,
                read_from_replicas=readonly or read_from_replicas,
                decode_responses=decode_responses,
                protocol_version=protocol_version,
                noreply=noreply,
                noevict=noevict,
                notouch=notouch,
                stream_timeout=stream_timeout,
                connect_timeout=connect_timeout,
                **kwargs,
            )

        super().__init__(
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            connection_pool=pool,
            connection_pool_cls=connection_pool_cls,
            decode_responses=decode_responses,
            verify_version=verify_version,
            protocol_version=protocol_version,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
            retry_policy=retry_policy,
            **kwargs,
        )

        self.refresh_table_asap: bool = False
        self.route_flags: Dict[bytes, NodeFlag] = self.__class__.ROUTING_FLAGS.copy()
        self.split_flags: Dict[bytes, NodeFlag] = self.__class__.SPLIT_FLAGS.copy()
        self.result_callbacks: Dict[
            bytes, Callable[..., Any]
        ] = self.__class__.RESULT_CALLBACKS.copy()
        self.non_atomic_cross_slot = non_atomic_cross_slot
        self.cache = cache
        self._decodecontext: contextvars.ContextVar[
            Optional[bool],
        ] = contextvars.ContextVar("decode", default=None)
        self._encodingcontext: contextvars.ContextVar[
            Optional[str],
        ] = contextvars.ContextVar("decode", default=None)

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisClusterBytesT],
        url: str,
        *,
        db: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[False] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        **kwargs: Any,
    ) -> RedisClusterBytesT:
        ...

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisClusterStringT],
        url: str,
        *,
        db: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[True],
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        **kwargs: Any,
    ) -> RedisClusterStringT:
        ...

    @classmethod
    def from_url(
        cls: Type[RedisClusterT],
        url: str,
        *,
        db: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        decode_responses: bool = False,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        retry_policy: RetryPolicy = CompositeRetryPolicy(
            ConstantRetryPolicy((ClusterDownError,), 2, 0.1),
            ConstantRetryPolicy(
                (
                    ConnectionError,
                    TimeoutError,
                ),
                2,
                0.1,
            ),
        ),
        **kwargs: Any,
    ) -> RedisClusterT:
        """
        Return a Cluster client object configured from the startup node in URL,
        which must use either the ``redis://`` scheme
        `<http://www.iana.org/assignments/uri-schemes/prov/redis>`_

        For example:

            - ``redis://[:password]@localhost:6379``
            - ``rediss://[:password]@localhost:6379``

        :paramref:`url` and :paramref:`kwargs` are passed as is to
        the :func:`coredis.ConnectionPool.from_url`.
        """
        if decode_responses:
            return cls(
                decode_responses=True,
                protocol_version=protocol_version,
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                protocol_version=protocol_version,
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    **kwargs,
                ),
            )

    async def initialize(self) -> RedisCluster[AnyStr]:
        if self.refresh_table_asap:
            self.connection_pool.initialized = False
        await super().initialize()
        if self.cache:
            self.cache = await self.cache.initialize(self)
        self.refresh_table_asap = False
        return self

    def __repr__(self) -> str:
        servers = list(
            {
                "{}:{}".format(info.host, info.port)
                for info in self.connection_pool.nodes.startup_nodes
            }
        )
        servers.sort()

        return "{}<{}>".format(type(self).__name__, ", ".join(servers))

    @property
    def all_nodes(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for node in self.connection_pool.nodes.all_nodes():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(node.host, node.port),
            )

    @property
    def primaries(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for primary in self.connection_pool.nodes.all_primaries():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(primary.host, primary.port),
            )

    @property
    def replicas(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for replica in self.connection_pool.nodes.all_replicas():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(replica.host, replica.port),
            )

    @property
    def num_replicas_per_shard(self) -> int:
        """
        Number of replicas per shard of the cluster determined by
        initial cluster topology discovery
        """
        return self.connection_pool.nodes.replicas_per_shard

    async def _ensure_initialized(self) -> None:
        if not self.connection_pool.initialized or self.refresh_table_asap:
            await self

    def _determine_slots(
        self, command: bytes, *args: ValueT, **options: Optional[ValueT]
    ) -> Set[int]:
        """Determines the slots the command and args would touch"""
        keys = cast(Tuple[ValueT, ...], options.get("keys")) or KeySpec.extract_keys(
            command, *args, readonly_command=self.connection_pool.read_from_replicas
        )
        if (
            command
            in {
                CommandName.EVAL,
                CommandName.EVAL_RO,
                CommandName.EVALSHA,
                CommandName.EVALSHA_RO,
                CommandName.FCALL,
                CommandName.FCALL_RO,
                CommandName.PUBLISH,
            }
            and not keys
        ):
            return set()

        return {hash_slot(b(key)) for key in keys}

    def _merge_result(
        self,
        command: bytes,
        res: Dict[str, R],
        **kwargs: Optional[ValueT],
    ) -> R:
        assert command in self.result_callbacks
        return cast(
            R,
            self.result_callbacks[command](
                res, version=self.protocol_version, **kwargs
            ),
        )

    def determine_node(
        self, command: bytes, **kwargs: Optional[ValueT]
    ) -> Optional[List[ManagedNode]]:
        node_flag = self.route_flags.get(command)
        if command in self.split_flags and self.non_atomic_cross_slot:
            node_flag = self.split_flags[command]

        if node_flag == NodeFlag.RANDOM:
            return [self.connection_pool.nodes.random_node(primary=True)]
        elif node_flag == NodeFlag.PRIMARIES:
            return list(self.connection_pool.nodes.all_primaries())
        elif node_flag == NodeFlag.ALL:
            return list(self.connection_pool.nodes.all_nodes())
        elif node_flag == NodeFlag.SLOT_ID:
            slot_id: Optional[ValueT] = kwargs.get("slot_id")
            node_from_slot = (
                self.connection_pool.nodes.node_from_slot(int(slot_id))
                if slot_id is not None
                else None
            )
            if node_from_slot:
                return [node_from_slot]
        return None

    async def on_connection_error(self, _: BaseException) -> None:
        self.connection_pool.disconnect()
        self.connection_pool.reset()
        self.refresh_table_asap = True

    async def on_cluster_down_error(self, _: BaseException) -> None:
        self.connection_pool.disconnect()
        self.connection_pool.reset()
        self.refresh_table_asap = True

    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Optional[ValueT],
    ) -> R:
        """
        Sends a command to one or many nodes in the cluster
        with retries based on :paramref:`RedisCluster.retry_policy`
        """

        return await self.retry_policy.call_with_retries(
            lambda: self._execute_command(command, *args, callback=callback, **kwargs),
            failure_hook={
                ConnectionError: self.on_connection_error,
                ClusterDownError: self.on_cluster_down_error,
            },
            before_hook=self._ensure_initialized,
        )

    async def _execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Optional[ValueT],
    ) -> R:
        """
        Sends a command to one or many nodes in the cluster
        """
        nodes = self.determine_node(command, **kwargs)
        if nodes and len(nodes) > 1:
            tasks: Dict[str, Coroutine[Any, Any, R]] = {}
            node_arg_mapping = self._split_args_over_nodes(nodes, command, *args)
            node_name_map = {n.name: n for n in nodes}
            for node_name in node_arg_mapping:
                for portion, pargs in enumerate(node_arg_mapping[node_name]):
                    tasks[
                        f"{node_name}:{portion}"
                    ] = self._execute_command_on_single_node(
                        command,
                        *pargs,
                        callback=callback,
                        node=node_name_map[node_name],
                        slots=None,
                        **kwargs,
                    )

            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            if self.noreply:
                return None  # type: ignore
            return cast(
                R,
                self._merge_result(command, dict(zip(tasks.keys(), results)), **kwargs),
            )
        else:
            node = None
            slots = None
            if not nodes:
                slots = list(self._determine_slots(command, *args, **kwargs))
            else:
                node = nodes.pop()
            return await self._execute_command_on_single_node(
                command, *args, callback=callback, node=node, slots=slots, **kwargs
            )

    def _split_args_over_nodes(
        self,
        nodes: List[ManagedNode],
        command: bytes,
        *args: ValueT,
    ) -> Dict[str, List[Tuple[ValueT, ...]]]:
        if command in self.split_flags and self.non_atomic_cross_slot:
            keys = KeySpec.extract_keys(command, *args)
            node_arg_mapping: Dict[str, List[Tuple[ValueT, ...]]] = {}
            if keys:
                key_start: int = args.index(keys[0])
                key_end: int = args.index(keys[-1])
                assert (
                    args[key_start : 1 + key_end] == keys
                ), f"Unable to map {command.decode('latin-1')} by keys {keys}"

                for (
                    node_name,
                    key_groups,
                ) in self.connection_pool.nodes.keys_to_nodes_by_slot(*keys).items():
                    for _, node_keys in key_groups.items():
                        node_arg_mapping.setdefault(node_name, []).append(
                            (
                                *args[:key_start],
                                *node_keys,  # type: ignore
                                *args[1 + key_end :],
                            )
                        )
            if self.cache and command not in READONLY_COMMANDS:
                self.cache.invalidate(*keys)
            return node_arg_mapping
        else:
            # This command is not meant to be split across nodes and each node
            # should be called with the same arguments
            return {node.name: [args] for node in nodes}

    async def _execute_command_on_single_node(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        node: Optional[ManagedNode] = None,
        slots: Optional[List[int]] = None,
        **kwargs: Optional[ValueT],
    ) -> R:
        redirect_addr = None

        asking = False

        if not node and not slots:
            try_random_node = True
            try_random_type = NodeFlag.PRIMARIES
        else:
            try_random_node = False
            try_random_type = NodeFlag.ALL
        remaining_attempts = int(self.MAX_RETRIES)

        while remaining_attempts > 0:
            remaining_attempts -= 1
            if self.refresh_table_asap and not slots:
                await self
            if asking and redirect_addr:
                node = self.connection_pool.nodes.nodes[redirect_addr]
                r = await self.connection_pool.get_connection_by_node(node)
            elif try_random_node:
                r = await self.connection_pool.get_random_connection(
                    primary=try_random_type == NodeFlag.PRIMARIES
                )
                if slots:
                    try_random_node = False
            elif node:
                r = await self.connection_pool.get_connection_by_node(node)
            elif slots:
                if self.refresh_table_asap:
                    # MOVED
                    node = self.connection_pool.get_primary_node_by_slots(slots)
                else:
                    node = self.connection_pool.get_node_by_slots(slots)
                r = await self.connection_pool.get_connection_by_node(node)
            else:
                continue
            quick_release = self.should_quick_release(command)
            released = False
            try:
                if asking:
                    request = await r.create_request(
                        CommandName.ASKING, noreply=self.noreply, decode=False
                    )
                    await request
                    asking = False

                if (
                    isinstance(self.cache, AbstractCache)
                    and isinstance(self.cache, SupportsClientTracking)
                    and r.tracking_client_id != self.cache.get_client_id(r)
                ):
                    self.cache.reset()
                    await r.update_tracking_client(True, self.cache.get_client_id(r))
                if self.cache and command not in READONLY_COMMANDS:
                    self.cache.invalidate(*KeySpec.extract_keys(command, *args))
                request = await r.create_request(
                    command,
                    *args,
                    noreply=self.noreply,
                    decode=kwargs.get("decode", self._decodecontext.get()),
                    encoding=self._encodingcontext.get(),
                )
                if quick_release and not (self.requires_wait or self.requires_waitaof):
                    released = True
                    self.connection_pool.release(r)

                reply = await request
                response = None
                maybe_wait = [
                    await self._ensure_wait(command, r),
                    await self._ensure_persistence(command, r),
                ]
                if not self.noreply:
                    if isinstance(callback, AsyncPreProcessingCallback):
                        await callback.pre_process(
                            self, reply, version=self.protocol_version, **kwargs
                        )  # pyright: reportGeneralTypeIssues=false
                    response = callback(
                        reply,
                        version=self.protocol_version,
                        **kwargs,
                    )
                await asyncio.gather(*maybe_wait)
                return response  # type: ignore
            except (RedisClusterException, BusyLoadingError, asyncio.CancelledError):
                raise
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                self.refresh_table_asap = True
                await self.connection_pool.nodes.increment_reinitialize_counter()

                node = self.connection_pool.nodes.set_node(
                    e.host, e.port, server_type="primary"
                )
                try_random_node = False
                self.connection_pool.nodes.slots[e.slot_id][0] = node
            except TryAgainError:
                if remaining_attempts < self.MAX_RETRIES / 2:
                    await asyncio.sleep(0.05)
            except AskError as e:
                redirect_addr, asking = f"{e.host}:{e.port}", True
            finally:
                self._ensure_server_version(r.server_version)
                if not released:
                    self.connection_pool.release(r)

        raise ClusterError("Maximum retries exhausted.")

    @overload
    def decoding(
        self, mode: Literal[False], encoding: Optional[str] = None
    ) -> ContextManager[RedisCluster[bytes]]:
        ...

    @overload
    def decoding(
        self, mode: Literal[True], encoding: Optional[str] = None
    ) -> ContextManager[RedisCluster[str]]:
        ...

    @contextlib.contextmanager
    @versionadded(version="4.8.0")
    def decoding(
        self, mode: bool, encoding: Optional[str] = None
    ) -> Iterator[RedisCluster[Any]]:
        """
        Context manager to temporarily change the decoding behavior
        of the client

        :param mode: Whether to decode or not
        :param encoding: Optional encoding to use if decoding. If not provided
         the :paramref:`~coredis.RedisCluster.encoding` parameter provided to the client will
         be used.

        Example::

            client = coredis.RedisCluster(decode_responses=True)
            await client.set("fubar", "baz")
            assert await client.get("fubar") == "baz"
            with client.decoding(False):
                assert await client.get("fubar") == b"baz"
                with client.decoding(True):
                    assert await client.get("fubar") == "baz"

        """
        prev_decode = self._decodecontext.get()
        prev_encoding = self._encodingcontext.get()
        self._decodecontext.set(mode)
        self._encodingcontext.set(encoding)
        try:
            yield self
        finally:
            self._decodecontext.set(prev_decode)
            self._encodingcontext.set(prev_encoding)

    def pubsub(
        self,
        ignore_subscribe_messages: bool = False,
        retry_policy: Optional[RetryPolicy] = None,
        **kwargs: Any,
    ) -> ClusterPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels or
        patterns in a redis cluster and receive messages that get published to them.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param retry_policy: An explicit retry policy to use in the subscriber.
        """
        return ClusterPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            retry_policy=retry_policy,
            **kwargs,
        )

    @versionadded(version="3.6.0")
    def sharded_pubsub(
        self,
        ignore_subscribe_messages: bool = False,
        read_from_replicas: bool = False,
        retry_policy: Optional[RetryPolicy] = None,
        **kwargs: Any,
    ) -> ShardedPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels
        in a redis cluster and receive messages that get published to them. The
        implementation returned differs from that returned by :meth:`pubsub`
        as it uses the Sharded Pub/Sub implementation which routes messages
        to cluster nodes using the same algorithm used to assign keys to slots.
        This effectively restricts the propagation of messages to be within the
        shard of a cluster hence affording horizontally scaling the use of Pub/Sub
        with the cluster itself.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param read_from_replicas: Whether to read messages from replica nodes
        :param retry_policy: An explicit retry policy to use in the subscriber.

        New in :redis-version:`7.0.0`
        """

        return ShardedPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            read_from_replicas=read_from_replicas,
            retry_policy=retry_policy,
            **kwargs,
        )

    async def pipeline(
        self,
        transaction: Optional[bool] = None,
        watches: Optional[Parameters[StringT]] = None,
        timeout: Optional[float] = None,
    ) -> "coredis.pipeline.ClusterPipeline[AnyStr]":
        """
        Returns a new pipeline object that can queue multiple commands for
        batch execution. Pipelines in cluster mode only provide a subset of the
        functionality of pipelines in standalone mode.

        Specifically:

        - Each command in the pipeline should only access keys on the same node
        - Transactions are disabled by default and are only supported if all
          watched keys route to the same node as where the commands in the multi/exec
          part of the pipeline.

        :param transaction: indicates whether all commands should be executed atomically.
        :param watches: If :paramref:`transaction` is True these keys are watched for external
         changes during the transaction.
        :param timeout: If specified this value will take precedence over
         :paramref:`RedisCluster.stream_timeout`

        """
        await self.connection_pool.initialize()

        from coredis.pipeline import ClusterPipeline

        return ClusterPipeline[AnyStr].proxy(
            client=self,
            transaction=transaction,
            watches=watches,
            timeout=timeout,
        )

    async def transaction(
        self,
        func: Callable[
            ["coredis.pipeline.ClusterPipeline[AnyStr]"],
            Coroutine[Any, Any, Any],
        ],
        *watches: StringT,
        value_from_callable: bool = False,
        watch_delay: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Convenience method for executing the callable :paramref:`func` as a
        transaction while watching all keys specified in :paramref:`watches`.

        :param func: callable should expect a single argument which is a
         :class:`coredis.pipeline.ClusterPipeline` object retrieved by calling
         :meth:`~coredis.RedisCluster.pipeline`.
        :param watches: The keys to watch during the transaction. The keys should route
         to the same node as the keys touched by the commands in :paramref:`func`
        :param value_from_callable: Whether to return the result of transaction or the value
         returned from :paramref:`func`

        .. warning:: Cluster transactions can only be run with commands that
           route to the same slot.

        .. versionchanged:: 4.9.0

           When the transaction is started with :paramref:`watches` the
           :class:`~coredis.pipeline.ClusterPipeline` instance passed to :paramref:`func`
           will not start queuing commands until a call to
           :meth:`~coredis.pipeline.ClusterPipeline.multi` is made. This makes the cluster
           implementation consistent with :meth:`coredis.Redis.transaction`
        """
        async with await self.pipeline(True) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = await func(pipe)
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue

    async def scan_iter(
        self,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
        type_: Optional[StringT] = None,
    ) -> AsyncIterator[AnyStr]:
        for node in self.primaries:
            cursor = None
            while cursor != 0:
                cursor, data = await node.scan(cursor or 0, match, count, type_)
                for item in data:
                    yield item
