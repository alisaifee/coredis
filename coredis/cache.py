from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pympler import asizeof

from coredis._utils import b
from coredis.commands import PubSub
from coredis.connection import BaseConnection
from coredis.sidecar import Sidecar
from coredis.typing import (
    Dict,
    Generic,
    Hashable,
    List,
    Literal,
    Optional,
    OrderedDict,
    ResponseType,
    Tuple,
    TypeVar,
    ValueT,
)

if TYPE_CHECKING:
    import coredis.client


class AbstractCache(ABC):
    """
    Abstract class representing a local cache that can be used by
    :class:`coredis.Redis`
    """

    client_id: Optional[int]

    @abstractmethod
    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        """
        Fetch the cached response for command/key/args combination
        """
        ...

    @abstractmethod
    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        """
        Cache the response for command/key/args combination
        """
        ...

    @abstractmethod
    def reset(self, *keys: ValueT) -> None:
        """
        Clear the cache (optionally, only for the keys provided as arguments)
        """
        ...

    @abstractmethod
    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> AbstractCache:
        ...

    @staticmethod
    def hashable_args(*args: Any) -> Tuple[Hashable, ...]:
        return tuple(
            (
                k
                if isinstance(k, Hashable)
                else tuple(k)
                if isinstance(k, list)
                else tuple(k.items())
                if isinstance(k, dict)
                else None
                for k in args
            )
        )

    @abstractmethod
    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        ...


ET = TypeVar("ET")


class LruCache(Generic[ET]):
    def __init__(self, max_keys: int = -1, max_bytes: int = -1):
        self.max_keys = max_keys
        self.max_bytes = max_bytes
        self.__cache: OrderedDict[Hashable, ET] = OrderedDict()

    def get(self, key: Hashable) -> ET:
        if key not in self.__cache:
            raise KeyError(key)
        self.__cache.move_to_end(key)
        return self.__cache[key]

    def insert(self, key: Hashable, value: ET) -> None:
        self.__check_capacity()
        self.__cache[key] = value
        self.__cache.move_to_end(key)

    def setdefault(self, key: Hashable, value: ET) -> ET:
        try:
            self.__check_capacity()
            return self.get(key)
        except KeyError:
            self.insert(key, value)
            return self.get(key)

    def remove(self, key: Hashable) -> None:
        if key in self.__cache:
            self.__cache.pop(key)

    def __check_capacity(self) -> None:
        if len(self.__cache) == self.max_keys or (
            self.max_bytes > 0 and asizeof.asizeof(self) > self.max_bytes
        ):
            self.__cache.popitem(last=False)

    def clear(self) -> None:
        self.__cache.clear()


class InvalidatingCache(AbstractCache, Sidecar):
    """
    An LRU cache that uses server assisted client caching
    to ensure local cache entries are invalidated if any
    operations are performed on the keys by another client.
    """

    def __init__(
        self,
        max_keys: int = 2 * 16,
        max_size_bytes: int = 64 * 1024 * 1024,
        cache: Optional[LruCache[LruCache[LruCache[ResponseType]]]] = None,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__invalidation_task: Optional[asyncio.Task[None]] = None
        self.__cache: LruCache[LruCache[LruCache[ResponseType]]] = cache or LruCache(
            max_keys, max_size_bytes
        )
        super().__init__({b"invalidate"})

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        return self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LruCache()).setdefault(
            command, LruCache()
        ).insert(self.hashable_args(*args), value)

    def reset(self, *keys: ValueT) -> None:
        if keys is not None:
            for k in keys:
                self.__cache.remove(b(k))
        else:
            self.__cache.clear()

    def process_message(self, message: ResponseType) -> Tuple[ResponseType, ...]:
        assert isinstance(message, list)
        if self.__protocol_version == 2:
            assert isinstance(message[0], bytes)

            if b(message[0]) in PubSub.SUBUNSUB_MESSAGE_TYPES:
                return ()
            elif message[2] is not None:
                assert isinstance(message[2], list)
                return tuple(k for k in message[2])
        elif message[1] is not None:
            assert isinstance(message[1], list)
            return tuple(k for k in message[1])
        return ()

    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> "InvalidatingCache":
        self.__protocol_version = client.protocol_version
        await super().start(client)
        if not self.__invalidation_task or self.__invalidation_task.done():
            self.__invalidation_task = asyncio.create_task(self.__invalidate())
        return self

    async def on_reconnect(self, connection: BaseConnection) -> None:
        self.__cache.clear()
        await super().on_reconnect(connection)
        if self.__protocol_version == 2 and self.connection:
            await self.connection.send_command(b"SUBSCRIBE", b"__redis__:invalidate")

    def shutdown(self) -> None:
        if self.__invalidation_task:
            self.__invalidation_task.cancel()
        super().shutdown()

    def get_client_id(self, client: BaseConnection) -> Optional[int]:
        return self.client_id

    async def __invalidate(self) -> None:
        while True:
            try:
                key = b(await self.messages.get())
                self.__cache.remove(key)
                self.messages.task_done()
            except asyncio.CancelledError:
                break


class ClusterInvalidatingCache(AbstractCache):
    """
    An LRU cache for redis cluster that uses server assisted client caching
    to ensure local cache entries are invalidated if any operations are performed on the
    keys by another client.

    The cache maintains an additional connection to each node in the cluster to listen
    to invalidation events
    """

    def __init__(
        self,
        max_keys: int = 2 * 16,
        max_size_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__cache: LruCache[LruCache[LruCache[ResponseType]]] = LruCache(
            max_keys, max_size_bytes
        )
        self.node_caches: Dict[str, InvalidatingCache] = {}
        self.__nodes: List["coredis.client.RedisConnection"] = []

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        return self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LruCache()).setdefault(
            command, LruCache()
        ).insert(self.hashable_args(*args), value)

    def reset(self, *keys: ValueT) -> None:
        if keys is not None:
            for k in keys:
                self.__cache.remove(b(k))
        else:
            self.__cache.clear()

    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        return self.node_caches[connection.location].client_id

    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> ClusterInvalidatingCache:
        import coredis.client

        assert isinstance(client, coredis.client.RedisCluster)

        self.__cache.clear()
        for sidecar in self.node_caches.values():
            sidecar.shutdown()
        self.node_caches.clear()
        self.__nodes = list(client.all_nodes)

        for node in self.__nodes:
            node_cache = InvalidatingCache(cache=self.__cache)
            await node_cache.initialize(node)
            assert node_cache.connection
            self.node_caches[node_cache.connection.location] = node_cache
        return self

    def shutdown(self) -> None:
        if self.node_caches:
            for sidecar in self.node_caches.values():
                sidecar.shutdown()
            self.node_caches.clear()
            self.__nodes.clear()

    def __del__(self) -> None:
        self.shutdown()
