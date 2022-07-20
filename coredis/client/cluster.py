from __future__ import annotations

import asyncio
import functools
import inspect
import textwrap
from abc import ABCMeta
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, List, cast, overload

from deprecated.sphinx import versionadded

from coredis._utils import b, clusterdown_wrapper, hash_slot
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
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    AnyStr,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Node,
    Optional,
    Parameters,
    ParamSpec,
    ResponseType,
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
    RESULT_CALLBACKS: Dict[str, Callable[..., ResponseType]]
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

        for name, method in methods.items():
            doc_addition = ""
            if cmd := getattr(method, "__coredis_command", None):
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
                if cmd.readonly:
                    ConnectionPool.READONLY_COMMANDS.add(cmd.command)
            if doc_addition and not hasattr(method, "__cluster_docs"):

                def __w(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
                    @functools.wraps(func)
                    async def _w(*a: P.args, **k: P.kwargs) -> R:
                        return await func(*a, **k)

                    _w.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                """
                    return _w

                wrapped = __w(method)
                setattr(wrapped, "__cluster_docs", doc_addition)
                setattr(kls, name, wrapped)
        return kls


RedisClusterT = TypeVar("RedisClusterT", bound="RedisCluster[Any]")
RedisClusterStringT = TypeVar("RedisClusterStringT", bound="RedisCluster[str]")
RedisClusterBytesT = TypeVar("RedisClusterBytesT", bound="RedisCluster[bytes]")


class RedisCluster(
    Client[AnyStr],
    metaclass=ClusterMeta,
):
    RedisClusterRequestTTL = 16
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
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[False] = ...,
        connection_pool: Optional[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        **kwargs: Any,
    ):
        ...

    @overload
    def __init__(
        self: RedisCluster[str],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        *,
        startup_nodes: Optional[Iterable[Node]] = ...,
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
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[True],
        connection_pool: Optional[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        **kwargs: Any,
    ):
        ...

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *,
        startup_nodes: Optional[Iterable[Node]] = None,
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
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = False,
        decode_responses: bool = False,
        connection_pool: Optional[ClusterConnectionPool] = None,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        non_atomic_cross_slot: bool = True,
        cache: Optional[AbstractCache] = None,
        noreply: bool = False,
        **kwargs: Any,
    ):
        """

        Changes
          - .. versionchanged:: 4.0.0
            :paramref:`non_atomic_cross_slot` defaults to ``True``
            :paramref:`protocol_version`` defaults to ``3``
          - .. versionadded:: 3.11.0
             Added :paramref:`noreply`
          - .. versionadded:: 3.10.0
             Synchronized ssl constructor parameters with :class:`coredis.Redis`
          - .. versionadded:: 3.9.0
             If :paramref:`cache` is provided the client will check & populate
             the cache for read only commands and invalidate it for commands
             that could change the key(s) in the request.
          - .. versionadded:: 3.6.0
             The :paramref:`non_atomic_cross_slot` parameter was added
          - .. versionchanged:: 3.5.0
             The :paramref:`verify_version` parameter now defaults to ``True``

          - .. versionadded:: 3.1.0
             The :paramref:`protocol_version` and :paramref:`verify_version`
             parameters were added

        :param host: Can be used to point to a startup node
        :param port: Can be used to point to a startup node
        :param startup_nodes: List of nodes that initial bootstrapping can be done
         from
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
        :param readonly: enable READONLY mode. You can read possibly stale data from slave.
        :param reinitialize_steps:
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
                        name="",
                        server_type=None,
                        node_id=None,
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

            pool = ClusterConnectionPool(
                startup_nodes=startup_nodes,
                max_connections=max_connections,
                reinitialize_steps=reinitialize_steps,
                max_connections_per_node=max_connections_per_node,
                skip_full_coverage_check=skip_full_coverage_check,
                nodemanager_follow_cluster=nodemanager_follow_cluster,
                readonly=readonly,
                decode_responses=decode_responses,
                protocol_version=protocol_version,
                noreply=noreply,
                **kwargs,
            )

        super().__init__(
            connection_pool=pool,
            connection_pool_cls=ClusterConnectionPool,
            decode_responses=decode_responses,
            verify_version=verify_version,
            protocol_version=protocol_version,
            noreply=noreply,
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
        **kwargs: Any,
    ) -> RedisClusterT:
        """
        Return a Cluster client object configured from the startup node in URL,
        which must use either the ``redis://`` scheme
        `<http://www.iana.org/assignments/uri-schemes/prov/redis>`_

        For example:

            - ``redis://[:password]@localhost:6379``
            - ``rediss://[:password]@localhost:6379``

        Any additional querystring arguments and keyword arguments will be
        passed along to the :class:`ClusterConnectionPool` class's initializer.
        In the case of conflicting arguments, querystring arguments always win.
        """
        if decode_responses:
            return cls(
                decode_responses=True,
                protocol_version=protocol_version,
                verify_version=verify_version,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    noreply=noreply,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                protocol_version=protocol_version,
                verify_version=verify_version,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    noreply=noreply,
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
                "{}:{}".format(info["host"], info["port"])
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
                self.connection_pool.nodes.get_redis_link(node["host"], node["port"]),
            )

    @property
    def primaries(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for master in self.connection_pool.nodes.all_primaries():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(
                    master["host"], master["port"]
                ),
            )

    @property
    def replicas(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for replica in self.connection_pool.nodes.all_replicas():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(
                    replica["host"], replica["port"]
                ),
            )

    def _determine_slot(self, command: bytes, *args: ValueT) -> Optional[int]:
        """Figures out what slot based on command and args"""
        keys = KeySpec.extract_keys((command,) + args, self.connection_pool.readonly)
        if (
            command
            in {
                CommandName.EVAL,
                CommandName.EVAL_RO,
                CommandName.EVALSHA,
                CommandName.EVALSHA_RO,
                CommandName.FCALL,
                CommandName.FCALL_RO,
            }
            and not keys
        ):
            return None

        slots = {hash_slot(b(key)) for key in keys}
        if len(slots) != 1:
            raise RedisClusterException(
                f"{str(command)} - all keys must map to the same key slot"
            )
        return slots.pop()

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
    ) -> Optional[List[Node]]:
        node_flag = self.route_flags.get(command)

        if command in self.split_flags and self.non_atomic_cross_slot:
            node_flag = self.split_flags[command]

        if node_flag == NodeFlag.RANDOM:
            return [self.connection_pool.nodes.random_node()]
        elif node_flag == NodeFlag.PRIMARIES:
            return list(self.connection_pool.nodes.all_primaries())
        elif node_flag == NodeFlag.ALL:
            return list(self.connection_pool.nodes.all_nodes())
        elif node_flag == NodeFlag.SLOT_ID:
            slot_id: Optional[ValueT] = kwargs.get("slot_id")
            if node_from_slot := (
                self.connection_pool.nodes.node_from_slot(int(slot_id))
                if slot_id is not None
                else None
            ):
                return [node_from_slot]
        return None

    @clusterdown_wrapper
    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Optional[ValueT],
    ) -> R:
        """
        Sends a command to a node in the cluster
        """
        if not self.connection_pool.initialized or self.refresh_table_asap:
            await self

        nodes = self.determine_node(command, **kwargs)
        if nodes and len(nodes) > 1:
            try:
                return await self.execute_command_on_nodes(
                    nodes,
                    command,
                    *args,
                    callback=callback,
                    **kwargs,
                )
            except ClusterDownError:
                self.connection_pool.disconnect()
                self.connection_pool.reset()
                self.refresh_table_asap = True
                raise

        redirect_addr = None
        asking = False

        try_random_node = False
        try_random_type = NodeFlag.ALL
        node = None
        slot = None
        if not nodes:
            slot = self._determine_slot(command, *args)
            if not slot:
                try_random_node = True
                try_random_type = NodeFlag.PRIMARIES
        else:
            node = nodes.pop()
        ttl = int(self.RedisClusterRequestTTL)

        while ttl > 0:
            ttl -= 1

            if asking and redirect_addr:
                node = self.connection_pool.nodes.nodes[redirect_addr]
                r = self.connection_pool.get_connection_by_node(node)
            elif try_random_node:
                r = self.connection_pool.get_random_connection(
                    primary=try_random_type == NodeFlag.PRIMARIES
                )
                try_random_node = False
            elif slot is not None:
                if self.refresh_table_asap:
                    # MOVED
                    node = self.connection_pool.get_primary_node_by_slot(slot)
                else:
                    node = self.connection_pool.get_node_by_slot(slot, command)
                r = self.connection_pool.get_connection_by_node(node)
            elif node:
                r = self.connection_pool.get_connection_by_node(node)
            else:
                continue

            try:
                if asking:
                    await r.send_command(CommandName.ASKING)
                    if not self.noreply:
                        await r.read_response(decode=kwargs.get("decode"))
                    asking = False

                if (
                    self.cache
                    and isinstance(self.cache, SupportsClientTracking)
                    and r.tracking_client_id != self.cache.get_client_id(r)
                ):
                    self.cache.reset()
                    await r.update_tracking_client(True, self.cache.get_client_id(r))
                if self.cache and command not in self.connection_pool.READONLY_COMMANDS:
                    self.cache.invalidate(*KeySpec.extract_keys((command,) + args))
                await r.send_command(command, *args)

                if self.noreply:
                    response = None
                else:
                    response = callback(
                        await r.read_response(decode=kwargs.get("decode")),
                        version=self.protocol_version,
                        **kwargs,
                    )
                await self._ensure_wait(command, r)
                return response  # type: ignore
            except (RedisClusterException, BusyLoadingError, asyncio.CancelledError):
                raise
            except (ConnectionError, TimeoutError):
                try_random_node = True

                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.1)
            except ClusterDownError as e:
                self.connection_pool.disconnect()
                self.connection_pool.reset()
                self.refresh_table_asap = True

                raise e
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                self.refresh_table_asap = True
                await self.connection_pool.nodes.increment_reinitialize_counter()

                node = self.connection_pool.nodes.set_node(
                    e.host, e.port, server_type="master"
                )
                self.connection_pool.nodes.slots[e.slot_id][0] = node
            except TryAgainError:
                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.05)
            except AskError as e:
                redirect_addr, asking = f"{e.host}:{e.port}", True
            finally:
                self._ensure_server_version(r.server_version)
                self.connection_pool.release(r)

        raise ClusterError("TTL exhausted.")

    async def execute_command_on_nodes(
        self,
        nodes: Iterable[Node],
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R],
        **options: Optional[ValueT],
    ) -> R:
        res: Dict[str, R] = {}
        node_arg_mapping: Dict[str, List[Tuple[ValueT, ...]]] = {}
        _nodes = list(nodes)

        if command in self.split_flags and self.non_atomic_cross_slot:
            keys = KeySpec.extract_keys((command,) + args)
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
            if self.cache and command not in self.connection_pool.READONLY_COMMANDS:
                self.cache.invalidate(*keys)
        while _nodes:
            cur = _nodes[0]
            connection = self.connection_pool.get_connection_by_node(cur)
            if (
                self.cache
                and isinstance(self.cache, SupportsClientTracking)
                and connection.tracking_client_id
                != self.cache.get_client_id(connection)
            ):
                self.cache.reset()
                await connection.update_tracking_client(
                    True, self.cache.get_client_id(connection)
                )
            try:
                if cur["name"] in node_arg_mapping:
                    for i, args in enumerate(node_arg_mapping[cur["name"]]):
                        await connection.send_command(command, *args)
                        if self.noreply:
                            continue
                        try:
                            res[f'{cur["name"]}:{i}'] = callback(
                                await connection.read_response(
                                    decode=options.get("decode")
                                ),
                                version=self.protocol_version,
                                **options,
                            )
                            await self._ensure_wait(command, connection)
                        except MovedError as err:
                            target = f"{err.node_addr[0]}:{err.node_addr[1]}"
                            target_node = self.connection_pool.nodes.nodes[target]
                            node_arg_mapping.setdefault(target, []).append(args)
                            if target_node not in _nodes:
                                _nodes.append(target_node)
                            self.refresh_table_asap = True
                            await self.connection_pool.nodes.increment_reinitialize_counter()
                elif node_arg_mapping:
                    continue
                else:
                    await connection.send_command(command, *args)
                    if not self.noreply:
                        res[cur["name"]] = callback(
                            await connection.read_response(
                                decode=options.get("decode"), raise_exceptions=False
                            ),
                            version=self.protocol_version,
                            **options,
                        )
                    await self._ensure_wait(command, connection)
            except asyncio.CancelledError:
                # do not retry when coroutine is cancelled
                connection.disconnect()
                raise
            except (ConnectionError, TimeoutError) as e:
                connection.disconnect()

                if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                    raise
                try:
                    await connection.send_command(command, *args)
                except ConnectionError as err:
                    # if a retry attempt results in a connection error assume cluster error
                    raise ClusterDownError(str(err))
                if not self.noreply:
                    res[cur["name"]] = callback(
                        await connection.read_response(
                            decode=options.get("decode"), raise_exceptions=False
                        ),
                        version=self.protocol_version,
                        **options,
                    )
                await self._ensure_wait(command, connection)
            finally:
                _nodes.pop(0)
                self._ensure_server_version(connection.server_version)
                self.connection_pool.release(connection)

        if self.noreply:
            return None  # type: ignore
        return self._merge_result(command, res, **options)

    def pubsub(
        self, ignore_subscribe_messages: bool = False, **kwargs: Any
    ) -> ClusterPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels or
        patterns in a redis cluster and receive messages that get published to them.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        """
        return ClusterPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            **kwargs,
        )

    @versionadded(version="3.6.0")
    def sharded_pubsub(
        self,
        ignore_subscribe_messages: bool = False,
        read_from_replicas: bool = False,
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

        New in :redis-version:`7.0.0`
        """

        return ShardedPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            read_from_replicas=read_from_replicas,
            **kwargs,
        )

    async def pipeline(
        self,
        transaction: Optional[bool] = None,
        watches: Optional[Parameters[StringT]] = None,
    ) -> "coredis.pipeline.ClusterPipeline[AnyStr]":
        """
        Pipelines in cluster mode only provide a subset of the functionality
        of pipelines in standalone mode.

        Specifically:

        - Transactions are only supported if all commands in the pipeline
          only access keys which route to the same node.
        - Each command in the pipeline should only access keys on the same node
        - Transactions with :paramref:`watch` are not supported.
        """
        await self.connection_pool.initialize()

        from coredis.pipeline import ClusterPipeline

        return ClusterPipeline[AnyStr].proxy(
            connection_pool=self.connection_pool,
            startup_nodes=self.connection_pool.nodes.startup_nodes,
            result_callbacks=self.result_callbacks,
            transaction=transaction,
            watches=watches,
        )

    async def transaction(
        self,
        func: Callable[
            ["coredis.pipeline.ClusterPipeline[AnyStr]"],
            Coroutine[Any, Any, Any],
        ],
        *watches: StringT,
        **kwargs: Any,
    ) -> Any:
        """
        Convenience method for executing the callable :paramref:`func` as a
        transaction while watching all keys specified in :paramref:`watches`.
        The :paramref:`func` callable should expect a single argument which is a
        :class:`~coredis.pipeline.ClusterPipeline` instance retrieved
        by calling :meth:`~coredis.RedisCluster.pipeline`

        .. warning:: Cluster transactions can only be run with commands that
           route to the same node.
        """
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        async with await self.pipeline(True, watches=watches) as pipe:
            while True:
                try:
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
