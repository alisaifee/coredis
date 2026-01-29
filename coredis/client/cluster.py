from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import random
import textwrap
from abc import ABCMeta
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, cast, overload

from anyio import get_cancelled_exc_class, sleep
from deprecated.sphinx import versionadded, versionchanged

from coredis._concurrency import gather
from coredis._utils import b, hash_slot
from coredis.cache import AbstractCache
from coredis.client.basic import Client, Redis
from coredis.commands._key_spec import KeySpec
from coredis.commands._validators import mutually_inclusive_parameters
from coredis.commands.constants import CommandName, NodeFlag
from coredis.commands.pubsub import ClusterPubSub, ShardedPubSub, SubscriptionCallback
from coredis.connection import RedisSSLContext
from coredis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    MovedError,
    RedisClusterException,
    TryAgainError,
)
from coredis.globals import CACHEABLE_COMMANDS, MODULE_GROUPS, READONLY_COMMANDS
from coredis.pool import ClusterConnectionPool
from coredis.pool.nodemanager import ManagedNode
from coredis.response._callbacks import AsyncPreProcessingCallback, NoopCallback
from coredis.retry import CompositeRetryPolicy, ConstantRetryPolicy, RetryPolicy
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    ExecutionParameters,
    Iterable,
    Iterator,
    KeyT,
    Literal,
    Mapping,
    Node,
    Parameters,
    ParamSpec,
    RedisCommand,
    RedisCommandP,
    RedisValueT,
    ResponseType,
    Self,
    StringT,
    TypeAdapter,
    TypeVar,
    Unpack,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.pipeline
    from coredis.lock import Lock
    from coredis.stream import Consumer, GroupConsumer, StreamParameters


class ClusterMeta(ABCMeta):
    ROUTING_FLAGS: dict[bytes, NodeFlag]
    SPLIT_FLAGS: dict[bytes, NodeFlag]
    RESULT_CALLBACKS: dict[bytes, Callable[..., ResponseType]]
    NODE_FLAG_DOC_MAPPING = {
        NodeFlag.PRIMARIES: "all primaries",
        NodeFlag.REPLICAS: "all replicas",
        NodeFlag.RANDOM: "a random node",
        NodeFlag.ALL: "all nodes",
        NodeFlag.SLOT_ID: "one or more nodes based on the slots provided",
    }

    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object]
    ) -> ClusterMeta:
        kls = super().__new__(cls, name, bases, namespace)
        methods = dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))
        for module in MODULE_GROUPS:
            methods.update(
                {
                    f"{module.MODULE}.{k[0]}": k[1]
                    for k in inspect.getmembers(module)
                    if inspect.isfunction(k[1])
                }
            )
        for method_name, method in methods.items():
            doc_addition = ""
            cmd = getattr(method, "__coredis_command", None)
            if cmd:
                if not cmd.cluster.enabled:
                    doc_addition = """
.. warning:: Not supported in cluster mode
                    """
                else:
                    if cmd.cluster.route:
                        kls.ROUTING_FLAGS[cmd.command] = cmd.cluster.route
                        aggregate_note = ""
                        if cmd.cluster.multi_node:
                            if cmd.cluster.combine:
                                aggregate_note = f"and return {cmd.cluster.combine.response_policy}"
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
                        func: Callable[P, Awaitable[R]], enabled: bool
                    ) -> Callable[P, Awaitable[R]]:
                        @functools.wraps(func)
                        async def _w(*a: P.args, **k: P.kwargs) -> R:
                            if not enabled:
                                raise NotImplementedError(
                                    f"{func.__name__} is disabled for cluster client"
                                )
                            return await func(*a, **k)

                        _w.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                    """
                        return _w

                    wrapped = __w(method, cmd.cluster.enabled if cmd else True)
                    setattr(wrapped, "__cluster_docs", doc_addition)
                    setattr(kls, method_name, wrapped)
                else:
                    method.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                    """
                    setattr(method, "__cluster_docs", doc_addition)
        return kls


RedisClusterT = TypeVar("RedisClusterT", bound="RedisCluster[Any]")


class RedisCluster(
    Client[AnyStr],
    metaclass=ClusterMeta,
):
    MAX_RETRIES = 16
    ROUTING_FLAGS: dict[bytes, NodeFlag] = {}
    SPLIT_FLAGS: dict[bytes, NodeFlag] = {}
    RESULT_CALLBACKS: dict[bytes, Callable[..., Any]] = {}

    connection_pool: ClusterConnectionPool

    @overload
    def __init__(
        self: RedisCluster[bytes],
        host: str | None = ...,
        port: int | None = ...,
        *,
        startup_nodes: Iterable[Node] | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        ssl: bool = ...,
        ssl_context: SSLContext | None = ...,
        ssl_keyfile: str | None = ...,
        ssl_certfile: str | None = ...,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = ...,
        ssl_check_hostname: bool | None = ...,
        ssl_ca_certs: str | None = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        read_from_replicas: bool = ...,
        reinitialize_steps: int | None = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        encoding: str = ...,
        decode_responses: Literal[False] = ...,
        connection_pool: ClusterConnectionPool | None = ...,
        connection_pool_cls: type[ClusterConnectionPool] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: AbstractCache | None = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self: RedisCluster[str],
        host: str | None = ...,
        port: int | None = ...,
        *,
        startup_nodes: Iterable[Node] | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        ssl: bool = ...,
        ssl_context: SSLContext | None = ...,
        ssl_keyfile: str | None = ...,
        ssl_certfile: str | None = ...,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = ...,
        ssl_check_hostname: bool | None = ...,
        ssl_ca_certs: str | None = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        read_from_replicas: bool = ...,
        reinitialize_steps: int | None = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        encoding: str = ...,
        decode_responses: Literal[True] = ...,
        connection_pool: ClusterConnectionPool | None = ...,
        connection_pool_cls: type[ClusterConnectionPool] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: AbstractCache | None = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        **kwargs: Any,
    ) -> None: ...

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        startup_nodes: Iterable[Node] | None = None,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        ssl: bool = False,
        ssl_context: SSLContext | None = None,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = None,
        ssl_check_hostname: bool | None = None,
        ssl_ca_certs: str | None = None,
        max_connections: int = 32,
        max_connections_per_node: bool = False,
        readonly: bool = False,
        read_from_replicas: bool = False,
        reinitialize_steps: int | None = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = True,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        connection_pool: ClusterConnectionPool | None = None,
        connection_pool_cls: type[ClusterConnectionPool] = ClusterConnectionPool,
        verify_version: bool = True,
        non_atomic_cross_slot: bool = True,
        cache: AbstractCache | None = None,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        retry_policy: RetryPolicy = CompositeRetryPolicy(
            ConstantRetryPolicy((ClusterDownError,), retries=2, delay=0.1),
            ConstantRetryPolicy(
                (
                    ConnectionError,
                    TimeoutError,
                ),
                retries=2,
                delay=0.1,
            ),
        ),
        type_adapter: TypeAdapter | None = None,
        **kwargs: Any,
    ) -> None:
        """

        Changes
          - .. versionremoved:: 6.0.0
            - :paramref:`protocol_version` removed (and therefore support for RESP2)

          - .. versionadded:: 6.0.0
            -
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
        :param encoding: The codec to use to encode strings transmitted to redis
         and decode responses with. (See :ref:`handbook/encoding:encoding/decoding`)
        :param decode_responses: If ``True`` string responses from the server
         will be decoded using :paramref:`encoding` before being returned.
         (See :ref:`handbook/encoding:encoding/decoding`)
        :param connection_pool: The connection pool instance to use. If not provided
         a new pool will be assigned to this client.
        :param connection_pool_cls: The connection pool class to use when constructing
         a connection pool for this instance.
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
        :param type_adapter: The adapter to use for serializing / deserializing customs types
         when interacting with redis commands.
        """

        if "db" in kwargs:  # noqa
            raise RedisClusterException("Argument 'db' is not possible to use in cluster mode")
        if connection_pool and cache:
            raise RuntimeError("Parameters 'cache' and 'connection_pool' are mutually exclusive!")

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
                encoding=encoding,
                decode_responses=decode_responses,
                noreply=noreply,
                noevict=noevict,
                notouch=notouch,
                stream_timeout=stream_timeout,
                connect_timeout=connect_timeout,
                _cache=cache,
                **kwargs,
            )

        super().__init__(
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            connection_pool=pool,
            connection_pool_cls=connection_pool_cls,
            encoding=encoding,
            decode_responses=decode_responses,
            verify_version=verify_version,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
            retry_policy=retry_policy,
            type_adapter=type_adapter,
            **kwargs,
        )

        self.refresh_table_asap: bool = False
        self.route_flags: dict[bytes, NodeFlag] = self.__class__.ROUTING_FLAGS.copy()
        self.split_flags: dict[bytes, NodeFlag] = self.__class__.SPLIT_FLAGS.copy()
        self.result_callbacks: dict[bytes, Callable[..., Any]] = (
            self.__class__.RESULT_CALLBACKS.copy()
        )
        self.non_atomic_cross_slot = non_atomic_cross_slot
        self._decodecontext: contextvars.ContextVar[bool | None,] = contextvars.ContextVar(
            "decode", default=None
        )
        self._encodingcontext: contextvars.ContextVar[str | None,] = contextvars.ContextVar(
            "decode", default=None
        )

    @classmethod
    @overload
    def from_url(
        cls: type[RedisCluster[bytes]],
        url: str,
        *,
        db: int | None = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[False] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        cache: AbstractCache | None = ...,
        **kwargs: Any,
    ) -> RedisCluster[bytes]: ...

    @classmethod
    @overload
    def from_url(
        cls: type[RedisCluster[str]],
        url: str,
        *,
        db: int | None = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[True],
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        cache: AbstractCache | None = ...,
        **kwargs: Any,
    ) -> RedisCluster[str]: ...

    @classmethod
    def from_url(
        cls: type[RedisClusterT],
        url: str,
        *,
        db: int | None = None,
        skip_full_coverage_check: bool = False,
        decode_responses: bool = False,
        verify_version: bool = True,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        cache: AbstractCache | None = None,
        retry_policy: RetryPolicy = CompositeRetryPolicy(
            ConstantRetryPolicy((ClusterDownError,), retries=2, delay=0.1),
            ConstantRetryPolicy(
                (
                    ConnectionError,
                    TimeoutError,
                ),
                retries=2,
                delay=0.1,
            ),
        ),
        type_adapter: TypeAdapter | None = None,
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
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                type_adapter=type_adapter,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    _cache=cache,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                type_adapter=type_adapter,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    _cache=cache,
                    **kwargs,
                ),
            )

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        if self.refresh_table_asap:
            self.connection_pool.initialized = False
        async with self.connection_pool:
            self.refresh_table_asap = False
            yield self

    def __repr__(self) -> str:
        servers = list(
            {f"{info.host}:{info.port}" for info in self.connection_pool.nodes.startup_nodes}
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
            await self.connection_pool.initialize()

    def _determine_slots(
        self, command: bytes, *args: RedisValueT, **options: Unpack[ExecutionParameters]
    ) -> set[int]:
        """Determines the slots the command and args would touch"""
        keys = cast(tuple[RedisValueT, ...], options.get("keys")) or KeySpec.extract_keys(
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
        res: dict[str, R],
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        assert command in self.result_callbacks
        return cast(
            R,
            self.result_callbacks[command](res, **kwargs),
        )

    def determine_node(
        self, command: bytes, *args: RedisValueT, **kwargs: Unpack[ExecutionParameters]
    ) -> list[ManagedNode] | None:
        node_flag = self.route_flags.get(command)
        if command in self.split_flags and self.non_atomic_cross_slot:
            node_flag = self.split_flags[command]

        if node_flag == NodeFlag.RANDOM:
            return [self.connection_pool.nodes.random_node(primary=True)]
        elif node_flag == NodeFlag.PRIMARIES:
            return list(self.connection_pool.nodes.all_primaries())
        elif node_flag == NodeFlag.ALL:
            return list(self.connection_pool.nodes.all_nodes())
        elif node_flag == NodeFlag.SLOT_ID and (
            slot_arguments_range := kwargs.get("slot_arguments_range", None)
        ):
            slot_start, slot_end = slot_arguments_range
            nodes = list(
                self.connection_pool.nodes.nodes_from_slots(
                    *cast(tuple[int, ...], args[slot_start:slot_end])
                ).keys()
            )
            return [self.connection_pool.nodes.nodes[k] for k in nodes]
        return None

    async def on_connection_error(self, _: BaseException) -> None:
        self.connection_pool.reset()
        self.refresh_table_asap = True

    async def on_cluster_down_error(self, _: BaseException) -> None:
        self.connection_pool.reset()
        self.refresh_table_asap = True

    async def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        """
        Sends a command to one or many nodes in the cluster
        with retries based on :paramref:`RedisCluster.retry_policy`
        """

        return await self.retry_policy.call_with_retries(
            lambda: self._execute_command(command, callback=callback, **kwargs),
            failure_hook={
                ConnectionError: self.on_connection_error,
                ClusterDownError: self.on_cluster_down_error,
            },
            before_hook=self._ensure_initialized,
        )

    async def _execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        """
        Sends a command to one or many nodes in the cluster
        """
        nodes = self.determine_node(command.name, *command.arguments, **kwargs)
        if nodes and len(nodes) > 1:
            tasks: dict[str, Coroutine[Any, Any, R]] = {}
            node_arg_mapping = self._split_args_over_nodes(
                nodes,
                command.name,
                *command.arguments,
                slot_arguments_range=kwargs.get("slot_arguments_range", None),
            )
            node_name_map = {n.name: n for n in nodes}
            for node_name in node_arg_mapping:
                for portion, pargs in enumerate(node_arg_mapping[node_name]):
                    tasks[f"{node_name}:{portion}"] = self._execute_command_on_single_node(
                        RedisCommand(command.name, pargs),
                        callback=callback,
                        node=node_name_map[node_name],
                        slots=None,
                        **kwargs,
                    )

            results = await gather(*tasks.values(), return_exceptions=True)
            if self.noreply:
                return None  # type: ignore
            return self._merge_result(command.name, dict(zip(tasks.keys(), results)))
        else:
            node = None
            slots = None
            if not nodes:
                slots = list(self._determine_slots(command.name, *command.arguments, **kwargs))
            else:
                node = nodes.pop()
            return await self._execute_command_on_single_node(
                command,
                callback=callback,
                node=node,
                slots=slots,
                **kwargs,
            )

    def _split_args_over_nodes(
        self,
        nodes: list[ManagedNode],
        command: bytes,
        *args: RedisValueT,
        slot_arguments_range: tuple[int, int] | None = None,
    ) -> dict[str, list[tuple[RedisValueT, ...]]]:
        node_flag = self.route_flags.get(command)
        node_arg_mapping: dict[str, list[tuple[RedisValueT, ...]]] = {}
        if command in self.split_flags and self.non_atomic_cross_slot:
            keys = KeySpec.extract_keys(command, *args)
            if keys:
                key_start: int = args.index(keys[0])
                key_end: int = args.index(keys[-1])
                assert args[key_start : 1 + key_end] == keys, (
                    f"Unable to map {command.decode('latin-1')} by keys {keys}"
                )

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
            if self.connection_pool.cache and command not in READONLY_COMMANDS:
                self.connection_pool.cache.invalidate(*keys)
        elif node_flag == NodeFlag.SLOT_ID and slot_arguments_range:
            # TODO: fix this nonsense put in place just to support a few cluster commands
            # related to slot management in cluster client which really no one needs to be calling
            # through the cluster client.
            slot_start, slot_end = slot_arguments_range
            all_slots = [int(k) for k in args[slot_start:slot_end] if k is not None]
            for node, slots in self.connection_pool.nodes.nodes_from_slots(*all_slots).items():
                node_arg_mapping[node] = [(*slots, *args[slot_end:])]  # type: ignore
        else:
            # This command is not meant to be split across nodes and each node
            # should be called with the same arguments
            node_arg_mapping = {node.name: [args] for node in nodes}
        return node_arg_mapping

    async def _execute_command_on_single_node(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        node: ManagedNode | None = None,
        slots: list[int] | None = None,
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        redirect_addr = None
        pool = self.connection_pool
        asking = False

        if not node and not slots:
            try_random_node = True
            try_random_type = NodeFlag.PRIMARIES
        else:
            try_random_node = False
            try_random_type = NodeFlag.ALL
        remaining_attempts = int(self.MAX_RETRIES)
        quick_release = self.should_quick_release(command)
        should_block = not quick_release or self.requires_wait or self.requires_waitaof

        while remaining_attempts > 0:
            remaining_attempts -= 1
            released = False
            if self.refresh_table_asap and not slots:
                # await self
                pass
            _node = None
            if asking and redirect_addr:
                _node = pool.nodes.nodes[redirect_addr]
            elif try_random_node:
                _node = None
                if slots:
                    try_random_node = False
            elif node:
                _node = node
            elif slots:
                if self.refresh_table_asap:
                    # MOVED
                    _node = pool.get_primary_node_by_slots(slots)
                else:
                    _node = pool.get_node_by_slots(slots, command=command.name)
            else:
                continue
            r = await pool.get_connection(
                _node, primary=not node and try_random_type == NodeFlag.PRIMARIES
            )
            try:
                if asking:
                    await r.create_request(CommandName.ASKING, noreply=self.noreply, decode=False)
                    asking = False
                keys = KeySpec.extract_keys(command.name, *command.arguments)
                cacheable = (
                    pool.cache
                    and command.name in CACHEABLE_COMMANDS
                    and len(keys) == 1
                    and not self.noreply
                    and self._decodecontext.get() is None
                )
                cache_hit = False
                cached_reply = None
                use_cached = False
                reply = None
                if pool.cache and pool.cache.healthy:
                    if r.tracking_client_id != pool.cache.get_client_id(r):
                        pool.cache.reset()
                        await r.update_tracking_client(True, pool.cache.get_client_id(r))
                    if command.name not in READONLY_COMMANDS:
                        pool.cache.invalidate(*keys)
                    elif cacheable:
                        try:
                            cached_reply = cast(
                                R,
                                pool.cache.get(
                                    command.name,
                                    keys[0],
                                    *command.arguments,
                                ),
                            )
                            use_cached = random.random() * 100.0 < min(100.0, pool.cache.confidence)
                            cache_hit = True
                        except KeyError:
                            pass

                if not (use_cached and cached_reply):
                    request = r.create_request(
                        command.name,
                        *command.arguments,
                        noreply=self.noreply,
                        decode=kwargs.get("decode", self._decodecontext.get()),
                        encoding=self._encodingcontext.get(),
                        disconnect_on_cancellation=should_block,
                    )
                    # TODO: Fix this! using both the release & should_block
                    #  flags to decide release logic is fragile. We should be
                    #  releasing early even in the cached response flow.
                    if not should_block:
                        released = True
                        pool.release(r)

                    reply = await request
                    await self._ensure_wait_and_persist(command, r)
                if self.noreply:
                    return  # type: ignore
                else:
                    if isinstance(callback, AsyncPreProcessingCallback):
                        await callback.pre_process(
                            self,
                            reply,
                        )
                    response = callback(
                        cached_reply if cache_hit else reply,
                    )
                    if pool.cache and cacheable:
                        if cache_hit and not use_cached:
                            pool.cache.feedback(
                                command.name,
                                keys[0],
                                *command.arguments,
                                match=cached_reply == reply,
                            )
                        if not cache_hit:
                            pool.cache.put(
                                command.name,
                                keys[0],
                                *command.arguments,
                                value=reply,
                            )
                    return response
            except (RedisClusterException, BusyLoadingError, get_cancelled_exc_class()):
                raise
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                self.refresh_table_asap = True
                await pool.nodes.increment_reinitialize_counter()

                node = pool.nodes.set_node(e.host, e.port, server_type="primary")
                try_random_node = False
                pool.nodes.slots[e.slot_id][0] = node
            except TryAgainError:
                if remaining_attempts < self.MAX_RETRIES / 2:
                    await sleep(0.05)
            except AskError as e:
                redirect_addr, asking = f"{e.host}:{e.port}", True
            finally:
                if r and not released:
                    pool.release(r)
                self._ensure_server_version(r.server_version)

        raise ClusterError("Maximum retries exhausted.")

    @overload
    def decoding(
        self, mode: Literal[False], encoding: str | None = None
    ) -> contextlib.AbstractContextManager[RedisCluster[bytes]]: ...

    @overload
    def decoding(
        self, mode: Literal[True], encoding: str | None = None
    ) -> contextlib.AbstractContextManager[RedisCluster[str]]: ...

    @contextlib.contextmanager
    @versionadded(version="4.8.0")
    def decoding(self, mode: bool, encoding: str | None = None) -> Iterator[RedisCluster[Any]]:
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

    @versionchanged(version="6.0.0", reason="All arguments are now keyword only")
    def pubsub(
        self,
        *,
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        patterns: Parameters[StringT] | None = None,
        pattern_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = None,
        subscription_timeout: float = 1,
        **kwargs: Any,
    ) -> ClusterPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to consume messages that get
        published to the subscribed channels or patterns.

        :param channels: channels that the constructed Pubsub instance should
         automatically subscribe to
        :param channel_handlers: Mapping of channels to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on the specific channel.
        :param patterns: patterns that the constructed Pubsub instance should
         automatically subscribe to
        :param pattern_handlers: Mapping of patterns to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on channel matching the pattern.
        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param retry_policy: An explicit retry policy to use in the subscriber.
        :param subscription_timeout: Maximum amount of time in seconds to wait for
         acknowledgement of subscriptions.
        """
        return ClusterPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            retry_policy=retry_policy,
            channels=channels,
            channel_handlers=channel_handlers,
            patterns=patterns,
            pattern_handlers=pattern_handlers,
            subscription_timeout=subscription_timeout,
            **kwargs,
        )

    @versionadded(version="3.6.0")
    @versionchanged(version="6.0.0", reason="All arguments are now keyword only")
    def sharded_pubsub(
        self,
        *,
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        ignore_subscribe_messages: bool = False,
        read_from_replicas: bool = False,
        retry_policy: RetryPolicy | None = None,
        subscription_timeout: float = 1,
        **kwargs: Any,
    ) -> ShardedPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to consume messages from
        the subscribed channels in a redis cluster.

        The implementation returned differs from that returned by :meth:`pubsub`
        as it uses the Sharded Pub/Sub implementation which routes messages
        to cluster nodes using the same algorithm used to assign keys to slots.
        This effectively restricts the propagation of messages to be within the
        shard of a cluster hence affording horizontally scaling the use of Pub/Sub
        with the cluster itself.

        :param channels: channels that the constructed Pubsub instance should
         automatically subscribe to
        :param channel_handlers: Mapping of channels to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on the specific channel.
        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param read_from_replicas: Whether to read messages from replica nodes
        :param retry_policy: An explicit retry policy to use in the subscriber.
        :param subscription_timeout: Maximum amount of time in seconds to wait for
         acknowledgement of subscriptions.

        New in :redis-version:`7.0.0`
        """

        return ShardedPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            read_from_replicas=read_from_replicas,
            retry_policy=retry_policy,
            channels=channels,
            channel_handlers=channel_handlers,
            subscription_timeout=subscription_timeout,
            **kwargs,
        )

    def pipeline(
        self,
        transaction: bool = False,
        *,
        raise_on_error: bool = True,
        timeout: float | None = None,
    ) -> coredis.pipeline.ClusterPipeline[AnyStr]:
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
        :param raise_on_error: Whether to raise errors upon executing the pipeline.
         If set to `False` errors will be accumulated and retrievable from the individual
         commands that had errors.
        :param watches: If :paramref:`transaction` is True these keys are watched for external
         changes during the transaction.
        :param timeout: If specified this value will take precedence over
         :paramref:`RedisCluster.stream_timeout`

        """

        from coredis.pipeline import ClusterPipeline

        return ClusterPipeline[AnyStr](
            client=self,
            raise_on_error=raise_on_error,
            transaction=transaction,
            timeout=timeout,
        )

    def lock(
        self,
        name: StringT,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> Lock[AnyStr]:
        """
        Return a lock instance which can be used to guard resource access across
        multiple clients.

        :param name: key for the lock
        :param timeout: indicates a maximum life for the lock.
         By default, it will remain locked until :meth:`~coredis.lock.Lock.release`
         is called.

        :param sleep: indicates the amount of time to sleep per loop iteration
         when the lock is in blocking mode and another client is currently
         holding the lock.

        :param blocking: indicates whether calling :meth:`~coredis.lock.Lock.acquire` should block until
         the lock has been acquired or to fail immediately, causing :meth:`acquire`
         to return ``False`` and the lock not being acquired. Defaults to ``True``.

        :param blocking_timeout: indicates the maximum amount of time in seconds to
         spend trying to acquire the lock. A value of ``None`` indicates
         continue trying forever.
        """
        from coredis.lock import Lock

        return Lock(self, name, timeout, sleep, blocking, blocking_timeout)

    @overload
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = ...,
        timeout: int | None = ...,
        group: Literal[None] = ...,
        consumer: Literal[None] = ...,
        auto_create: bool = ...,
        auto_acknowledge: bool = ...,
        start_from_backlog: bool = ...,
        **stream_parameters: StreamParameters,
    ) -> Consumer[AnyStr]: ...
    @overload
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = ...,
        timeout: int | None = ...,
        group: StringT = ...,
        consumer: StringT = ...,
        auto_create: bool = ...,
        auto_acknowledge: bool = ...,
        start_from_backlog: bool = ...,
        **stream_parameters: StreamParameters,
    ) -> GroupConsumer[AnyStr]: ...
    @versionadded(version="6.0.0")
    @mutually_inclusive_parameters("group", "consumer")
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = 0,
        timeout: int | None = None,
        group: StringT | None = None,
        consumer: StringT | None = None,
        auto_create: bool = True,
        auto_acknowledge: bool = False,
        start_from_backlog: bool = False,
        **stream_parameters: StreamParameters,
    ) -> Consumer[AnyStr] | GroupConsumer[AnyStr]:
        """
        Create a stream consumer for one or more Redis streams.

        Depending on whether ``group`` and ``consumer`` are provided, this method
        creates either a standalone stream consumer or a member of a stream
        consumer group.

        If ``group`` and ``consumer`` are not provided, a standalone stream
        consumer is created that starts reading from the latest entry of each
        stream provided in :paramref:`streams`.

        The latest entry is determined by calling
        :meth:`coredis.Redis.xinfo_stream` and using the :data:`last-entry`
        attribute at the point of initializing the consumer instance or on first
        fetch (whichever comes first). If the stream(s) do not exist at the time
        of consumer creation, the consumer will simply start from the minimum
        identifier (``0-0``).

        If ``group`` and ``consumer`` are provided, a member of a stream consumer
        group is created. The consumer has an identical interface as
        :class:`coredis.stream.Consumer`.

        :param streams: the stream identifiers to consume from
        :param buffer_size: Size of buffer (per stream) to maintain. This
         translates to the maximum number of stream entries that are fetched
         on each request to redis.
        :param timeout: Maximum amount of time in milliseconds to block for new
         entries to appear on the streams the consumer is reading from.
        :param group: The name of the group this consumer is part of
        :param consumer: The unique name (within :paramref:`group`) of the consumer
        :param auto_create: If True the group will be created upon initialization
         or first fetch if it doesn't already exist.
        :param auto_acknowledge: If ``True`` the stream entries fetched will be fetched
         without needing to be acknowledged with :meth:`coredis.Redis.xack` to remove
         them from the pending entries list.
        :param start_from_backlog: If ``True`` the consumer will start by fetching any pending
         entries from the pending entry list before considering any new messages
         not seen by any other consumer in the :paramref:`group`
        :param stream_parameters: Mapping of optional parameters to use
         by stream for the streams provided in :paramref:`streams`.

         .. warning:: Providing an ``identifier`` in ``stream_parameters`` has a different
            meaning for a group consumer. If the value is any valid identifier other than ``>``
            the consumer will only access the history of pending messages. That is, the set of
            messages that were delivered to this consumer (identified by :paramref:`consumer`)
            and never acknowledged.
        """
        from coredis.stream import Consumer, GroupConsumer

        if group is not None and consumer is not None:
            return GroupConsumer(
                self,
                streams,
                group=group,
                consumer=consumer,
                buffer_size=buffer_size,
                auto_create=auto_create,
                auto_acknowledge=auto_acknowledge,
                start_from_backlog=start_from_backlog,
                timeout=timeout,
                **stream_parameters,
            )
        else:
            return Consumer(
                self, streams, buffer_size=buffer_size, timeout=timeout, **stream_parameters
            )

    async def scan_iter(
        self,
        match: StringT | None = None,
        count: int | None = None,
        type_: StringT | None = None,
    ) -> AsyncIterator[AnyStr]:
        await self._ensure_initialized()
        for node in self.primaries:
            async with node:
                cursor = None
                while cursor != 0:
                    cursor, data = await node.scan(cursor or 0, match, count, type_)
                    for item in data:
                        yield item
