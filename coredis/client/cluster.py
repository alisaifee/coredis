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
from coredis.client.basic import Client, Redis
from coredis.cluster._node import ClusterNodeLocation
from coredis.commands._key_spec import KeySpec
from coredis.commands._validators import mutually_inclusive_parameters
from coredis.commands.constants import CommandName, NodeFlag
from coredis.connection._base import RedisSSLContext
from coredis.connection._tcp import TCPLocation
from coredis.credentials import AbstractCredentialProvider
from coredis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    MovedError,
    RedisClusterError,
    TryAgainError,
)
from coredis.globals import (
    CACHEABLE_COMMANDS,
    MERGE_CALLBACKS,
    MODULE_GROUPS,
    READONLY_COMMANDS,
)
from coredis.patterns.cache import AbstractCache
from coredis.patterns.pubsub import ClusterPubSub, ShardedPubSub, SubscriptionCallback
from coredis.pool import ClusterConnectionPool
from coredis.response._callbacks import NoopCallback
from coredis.retry import (
    CompositeRetryPolicy,
    ConstantRetryPolicy,
    ExponentialBackoffRetryPolicy,
    RetryPolicy,
)
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
    Self,
    StringT,
    TypeAdapter,
    TypeVar,
    Unpack,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.patterns.pipeline
    from coredis.patterns.lock import Lock
    from coredis.patterns.streams import Consumer, GroupConsumer, StreamParameters


class ClusterMeta(ABCMeta):
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
                        doc_addition = f"""
.. admonition:: Cluster note

   The command will be run on **{cls.NODE_FLAG_DOC_MAPPING[cmd.cluster.split]}**
   by distributing the keys to the appropriate nodes and return
   {cmd.cluster.combine.response_policy}.

   To disable this behavior set :paramref:`RedisCluster.non_atomic_cross_slot` to ``False``
                    """
            if doc_addition and not hasattr(method, "__cluster_docs") and cmd:
                if not getattr(method, "__coredis_module", None):
                    if not cmd.cluster.enabled:
                        def __w(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
                            @functools.wraps(func)
                            def _w(*a: P.args, **k: P.kwargs) -> Awaitable[R]:
                                raise NotImplementedError(
                                    f"{func.__name__} is disabled for cluster client"
                                )

                            _w.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
    {doc_addition}
                            """
                            return _w

                        wrapped = __w(method)
                        setattr(wrapped, "__cluster_docs", doc_addition)
                        setattr(kls, method_name, wrapped)
                    else:
                        setattr(method, "__cluster_docs", doc_addition)
                        method.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
    {doc_addition}
                        """
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

    connection_pool: ClusterConnectionPool

    @overload
    def __init__(
        self: RedisCluster[bytes],
        host: str | None = ...,
        port: int | None = ...,
        *,
        startup_nodes: Iterable[Node] | None = ...,
        username: str | None = ...,
        password: str | None = ...,
        credential_provider: AbstractCredentialProvider | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        pool_timeout: float | None = ...,
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
    ) -> None: ...

    @overload
    def __init__(
        self: RedisCluster[str],
        host: str | None = ...,
        port: int | None = ...,
        *,
        startup_nodes: Iterable[Node | TCPLocation] | None = ...,
        username: str | None = ...,
        password: str | None = ...,
        credential_provider: AbstractCredentialProvider | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        pool_timeout: float | None = ...,
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
    ) -> None: ...
    @overload
    def __init__(
        self: Self,
        host: str | None = ...,
        port: int | None = ...,
        *,
        startup_nodes: Iterable[Node | TCPLocation] | None = ...,
        username: str | None = ...,
        password: str | None = ...,
        credential_provider: AbstractCredentialProvider | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        pool_timeout: float | None = ...,
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
        decode_responses: bool = ...,
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
    ) -> None: ...

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        startup_nodes: Iterable[Node | TCPLocation] | None = None,
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        pool_timeout: float | None = None,
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
    ) -> None:
        """

        Changes
          - .. versionremoved:: 6.0.0

            - :paramref:`protocol_version` removed (and therefore support for RESP2)

          - .. versionchanged:: 6.0.0

            -  The cluster client is now an async context manager and must always be used as such.

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
        :param pool_timeout: Timeout (seconds) for acquiring a connection from the
         connection pool
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

        if connection_pool and cache:
            raise RuntimeError("Parameters 'cache' and 'connection_pool' are mutually exclusive!")
        if connection_pool:
            pool = connection_pool
        else:
            startup_nodes = list(
                node if isinstance(node, TCPLocation) else TCPLocation(node["host"], node["port"])
                for node in startup_nodes or []
            )

            # Support host/port as argument
            if host and not startup_nodes:
                startup_nodes.append(
                    TCPLocation(
                        host=host,
                        port=port if port else 7000,
                    )
                )
            if ssl_context is None and ssl:
                ssl_context = RedisSSLContext(
                    ssl_keyfile,
                    ssl_certfile,
                    ssl_cert_reqs,
                    ssl_ca_certs,
                    ssl_check_hostname,
                ).get()

            pool = connection_pool_cls(
                startup_nodes=startup_nodes,
                username=username,
                password=password,
                credential_provider=credential_provider,
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
                timeout=pool_timeout,
                ssl_context=ssl_context,
                _cache=cache,
            )

        super().__init__(
            connection_pool=pool,
            verify_version=verify_version,
            noreply=noreply,
            retry_policy=retry_policy,
            type_adapter=type_adapter,
        )

        self.refresh_table_asap: bool = True
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
        cls,
        url: str,
        *,
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
        cls,
        url: str,
        *,
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
        the :func:`coredis.pool.ClusterConnectionPool.from_url`.
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
        async with self.connection_pool:
            self.refresh_table_asap = False
            yield self

    def __repr__(self) -> str:
        servers = list(node.name for node in self.connection_pool.cluster_layout.nodes)
        servers.sort()

        return "{}<{}>".format(type(self).__name__, ", ".join(servers))

    @property
    def all_nodes(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for node in self.connection_pool.cluster_layout.nodes:
            yield node.as_client(**self.connection_pool.connection_kwargs)

    @property
    def primaries(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for primary in self.connection_pool.cluster_layout.primaries:
            yield primary.as_client(**self.connection_pool.connection_kwargs)

    @property
    def replicas(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for replica in self.connection_pool.cluster_layout.replicas:
            yield replica.as_client(**self.connection_pool.connection_kwargs)

    @property
    def num_replicas_per_shard(self) -> int:
        """
        Number of replicas per shard of the cluster determined by
        initial cluster topology discovery
        """
        if replicas := list(self.replicas):
            return int((len(list(self.all_nodes)) / len(replicas)) - 1)
        return 0

    async def _ensure_initialized(self) -> None:
        if not self.connection_pool.initialized or self.refresh_table_asap:
            await self.connection_pool.refresh_cluster_mapping(forced=True)
            self.refresh_table_asap = False

    def _merge_result(
        self,
        command: bytes,
        res: dict[str, R],
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        assert command in MERGE_CALLBACKS
        return cast(
            R,
            MERGE_CALLBACKS[command](res, **kwargs),
        )

    async def on_connection_error(self, _: BaseException) -> None:
        self.refresh_table_asap = True

    async def on_cluster_down_error(self, _: BaseException) -> None:
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
        prefer_replica = (
            command.name in READONLY_COMMANDS and self.connection_pool.read_from_replicas
        )
        nodes = self.connection_pool.cluster_layout.nodes_for_request(
            command.name,
            command.arguments,
            prefer_replica=prefer_replica,
            allow_cross_slot=self.non_atomic_cross_slot,
            execution_parameters=kwargs,
        )
        if nodes and len(nodes) > 1:
            tasks: dict[str, Coroutine[Any, Any, R]] = {}
            for node in nodes:
                for portion, pargs in enumerate(nodes[node]):
                    tasks[f"{node.name}:{portion}"] = self._execute_command_on_single_node(
                        node,
                        RedisCommand(command.name, pargs),
                        callback=callback,
                        **kwargs,
                    )

            results = await gather(*tasks.values(), return_exceptions=True)
            if self.noreply:
                return None  # type: ignore
            return self._merge_result(command.name, dict(zip(tasks.keys(), results)))
        else:
            assert len(nodes) == 1, (nodes, command.name, command.arguments)
            node = list(nodes.keys()).pop()
            return await self._execute_command_on_single_node(
                node,
                command,
                callback=callback,
                **kwargs,
            )

    async def _execute_command_on_single_node(
        self,
        node: ClusterNodeLocation,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        redirect_location = None
        asking = False

        try_random_node = False
        try_random_type = NodeFlag.ALL
        remaining_attempts = int(self.MAX_RETRIES)
        quick_release = self.should_quick_release(command)
        should_block = not quick_release or self.requires_wait or self.requires_waitaof

        while remaining_attempts > 0:
            remaining_attempts -= 1
            released = False
            if self.refresh_table_asap:
                await self.connection_pool.refresh_cluster_mapping(forced=True)
                self.refresh_table_asap = False
            _node = None
            if asking and redirect_location:
                _node = self.connection_pool.cluster_layout.node_for_location(redirect_location)
            elif try_random_node:
                _node = None
            elif node:
                _node = node
            else:
                raise
            r = await self.connection_pool.get_connection(
                _node, primary=not node and try_random_type == NodeFlag.PRIMARIES
            )
            try:
                if asking:
                    await r.create_request(CommandName.ASKING, noreply=self.noreply, decode=False)
                    asking = False
                keys = KeySpec.extract_keys(command.name, *command.arguments)
                cacheable = (
                    self.connection_pool.cache
                    and command.name in CACHEABLE_COMMANDS
                    and len(keys) == 1
                    and not self.noreply
                    and self._decodecontext.get() is None
                )
                cache_hit = False
                cached_reply = None
                use_cached = False
                reply = None
                if self.connection_pool.cache and self.connection_pool.cache.healthy:
                    if r.tracking_client_id != self.connection_pool.cache.get_client_id(r):
                        self.connection_pool.cache.reset()
                        await r.update_tracking_client(
                            True, self.connection_pool.cache.get_client_id(r)
                        )
                    if command.name not in READONLY_COMMANDS:
                        self.connection_pool.cache.invalidate(*keys)
                    elif cacheable:
                        try:
                            cached_reply = cast(
                                R,
                                self.connection_pool.cache.get(
                                    command.name,
                                    keys[0],
                                    *command.arguments,
                                ),
                            )
                            use_cached = random.random() * 100.0 < min(
                                100.0, self.connection_pool.cache.confidence
                            )
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
                        self.connection_pool.release(r)

                    reply = await request
                    await self._ensure_wait_and_persist(command, r)
                if self.noreply:
                    return  # type: ignore
                else:
                    response = callback(
                        cached_reply if cache_hit else reply,
                    )
                    if self.connection_pool.cache and cacheable:
                        if cache_hit and not use_cached:
                            self.connection_pool.cache.feedback(
                                command.name,
                                keys[0],
                                *command.arguments,
                                match=cached_reply == reply,
                            )
                        if not cache_hit:
                            self.connection_pool.cache.put(
                                command.name,
                                keys[0],
                                *command.arguments,
                                value=reply,
                            )
                    return response
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                self.refresh_table_asap = True
                self.connection_pool.cluster_layout.register_errors(e)
                node = self.connection_pool.cluster_layout.update_primary(e.slot_id, e.host, e.port)
                try_random_node = False
            except TryAgainError:
                if remaining_attempts < self.MAX_RETRIES / 2:
                    await sleep(0.05)
            except AskError as e:
                redirect_location, asking = TCPLocation(e.host, e.port), True
            except (RedisClusterError, BusyLoadingError, get_cancelled_exc_class()):
                raise
            finally:
                if r and not released:
                    self.connection_pool.release(r)
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
        retry_policy: RetryPolicy | None = CompositeRetryPolicy(
            ExponentialBackoffRetryPolicy(
                (ConnectionError,), retries=None, base_delay=0.1, max_delay=16, jitter=True
            ),
            ConstantRetryPolicy((TimeoutError,), retries=2, delay=0.1),
        ),
        subscription_timeout: float = 1,
        max_idle_seconds: float = 15,
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
        :param max_idle_seconds: Maximum duration (in seconds) to tolerate no
         messages from the cluster before performing a keepalive check with a
         ``PING``.
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
            max_idle_seconds=max_idle_seconds,
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
        retry_policy: RetryPolicy | None = CompositeRetryPolicy(
            ExponentialBackoffRetryPolicy(
                (ConnectionError,), retries=None, base_delay=0.1, max_delay=16, jitter=True
            ),
            ConstantRetryPolicy((TimeoutError,), retries=2, delay=0.1),
        ),
        subscription_timeout: float = 1,
        max_idle_seconds: float = 15,
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
        :param max_idle_seconds: Maximum duration (in seconds) to tolerate no
         messages from the cluster before performing a keepalive check with a
         ``PING``.

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
            max_idle_seconds=max_idle_seconds,
        )

    def pipeline(
        self,
        transaction: bool = False,
        *,
        raise_on_error: bool = True,
        timeout: float | None = None,
    ) -> coredis.patterns.pipeline.ClusterPipeline[AnyStr]:
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

        from coredis.patterns.pipeline import ClusterPipeline

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
         By default, it will remain locked until :meth:`~coredis.patterns.lock.Lock.release`
         is called.

        :param sleep: indicates the amount of time to sleep per loop iteration
         when the lock is in blocking mode and another client is currently
         holding the lock.

        :param blocking: indicates whether calling :meth:`~coredis.patterns.lock.Lock.acquire` should block until
         the lock has been acquired or to fail immediately, causing :meth:`acquire`
         to return ``False`` and the lock not being acquired. Defaults to ``True``.

        :param blocking_timeout: indicates the maximum amount of time in seconds to
         spend trying to acquire the lock. A value of ``None`` indicates
         continue trying forever.
        """
        from coredis.patterns.lock import Lock

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
        :meth:`~coredis.RedisCluster.xinfo_stream` and using the :data:`last-entry`
        attribute at the point of initializing the consumer instance or on first
        fetch (whichever comes first). If the stream(s) do not exist at the time
        of consumer creation, the consumer will simply start from the minimum
        identifier (``0-0``).

        If ``group`` and ``consumer`` are provided, a member of a stream consumer
        group is created. The consumer has an identical interface as
        :class:`coredis.patterns.streams.Consumer`.

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
         without needing to be acknowledged with :meth:`~coredis.RedisCluster.xack` to remove
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
        from coredis.patterns.streams import Consumer, GroupConsumer

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
