from __future__ import annotations

import asyncio
import time
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
    :class:`coredis.Redis` or :class:`coredis.RedisCluster`
    """

    @abstractmethod
    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> AbstractCache:
        ...

    @property
    @abstractmethod
    def healthy(self) -> bool:
        """
        Whether the cache is healthy and should be taken seriously
        """
        ...

    @abstractmethod
    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        """
        If the cache supports receiving invalidation events from the server
        return the ``client_id`` that the :paramref:`connection` should send
        redirects to.
        """
        ...

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
    def shutdown(self) -> None:
        """
        Explicitly shutdown the cache
        """
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


ET = TypeVar("ET")


class LRUCache(Generic[ET]):
    def __init__(self, max_items: int = -1, max_bytes: int = -1):
        self.max_items = max_items
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

    def clear(self) -> None:
        self.__cache.clear()

    def popitem(self) -> bool:
        """
        Recursively remove the oldest entry. If
        the oldest entry is another LRUCache trigger
        the removal of it's oldest entry and if that
        turns out to be an empty LRUCache, remove that.
        """
        try:
            oldest = next(iter(self.__cache))
            item = self.__cache[oldest]
        except StopIteration:
            return False

        if isinstance(item, LRUCache):
            if item.popitem():
                return True
        self.__cache.popitem(last=False)
        return True

    def shrink(self) -> None:
        """
        Remove old entries until the size of the cache
        is less than :paramref:`LRUCache.max_bytes` or if
        there is nothing left to remove.
        """
        if self.max_bytes > 0:
            while asizeof.asizeof(self) > self.max_bytes:
                if not self.popitem():
                    # nothing left to remove
                    return

    def __repr__(self) -> str:
        return (
            f"LruCache<max_items={self.max_items}, "
            f"max_bytes={self.max_bytes}, "
            f"current_size={asizeof.asizeof(self)}>"
        )

    def __check_capacity(self) -> None:
        if len(self.__cache) == self.max_items:
            self.__cache.popitem(last=False)


class NodeTrackingCache(AbstractCache, Sidecar):
    """
    An LRU cache that uses server assisted client caching
    to ensure local cache entries are invalidated if any
    operations are performed on the keys by another client.
    """

    def __init__(
        self,
        max_keys: int = 2**12,
        max_size_bytes: int = 64 * 1024 * 1024,
        max_idle_seconds: int = 5,
        cache: Optional[LRUCache[LRUCache[LRUCache[ResponseType]]]] = None,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__invalidation_task: Optional[asyncio.Task[None]] = None
        self.__watchdog_task: Optional[asyncio.Task[None]] = None
        self.__cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self.__max_idle_seconds = max_idle_seconds
        super().__init__({b"invalidate"}, max(1, max_idle_seconds - 1))

    @property
    def healthy(self) -> bool:
        return bool(
            self.connection
            and time.monotonic() - self.last_checkin < self.__max_idle_seconds
        )

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        return self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LRUCache()).setdefault(
            command, LRUCache()
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
    ) -> NodeTrackingCache:
        self.__protocol_version = client.protocol_version
        await super().start(client)
        if not self.__invalidation_task or self.__invalidation_task.done():
            self.__invalidation_task = asyncio.create_task(self.__invalidate())
        if not self.__watchdog_task or self.__watchdog_task.done():
            self.__watchdog_task = asyncio.create_task(self.__watchdog())
        return self

    async def on_reconnect(self, connection: BaseConnection) -> None:
        self.__cache.clear()
        await super().on_reconnect(connection)
        if self.__protocol_version == 2 and self.connection:
            await self.connection.send_command(b"SUBSCRIBE", b"__redis__:invalidate")

    def shutdown(self) -> None:
        try:
            asyncio.get_running_loop()
            if self.__invalidation_task:
                self.__invalidation_task.cancel()
            if self.__watchdog_task:
                self.__watchdog_task.cancel()
            super().stop()
        except RuntimeError:
            pass

    def get_client_id(self, client: BaseConnection) -> Optional[int]:
        return self.client_id

    async def __watchdog(self) -> None:
        while True:
            try:
                if not self.healthy:
                    if self.connection:
                        self.connection.disconnect()
                    self.__cache.clear()
                else:
                    self.__cache.shrink()
                await asyncio.sleep(max(1, self.__max_idle_seconds - 1))
            except asyncio.CancelledError:
                break

    async def __invalidate(self) -> None:
        while True:
            try:
                key = b(await self.messages.get())
                self.__cache.remove(key)
                self.messages.task_done()
            except asyncio.CancelledError:
                break
            except RuntimeError:
                break


class ClusterTrackingCache(AbstractCache):
    """
    An LRU cache for redis cluster that uses server assisted client caching
    to ensure local cache entries are invalidated if any operations are performed
    on the keys by another client.

    The cache maintains an additional connection per node (including replicas)
    in the cluster to listen to invalidation events
    """

    def __init__(
        self,
        max_keys: int = 2**12,
        max_size_bytes: int = 64 * 1024 * 1024,
        max_idle_seconds: int = 5,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = LRUCache(
            max_keys, max_size_bytes
        )
        self.node_caches: Dict[str, NodeTrackingCache] = {}
        self.__nodes: List["coredis.client.RedisConnection"] = []
        self.__max_idle_seconds = max_idle_seconds

    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> ClusterTrackingCache:
        import coredis.client

        assert isinstance(client, coredis.client.RedisCluster)

        self.__cache.clear()
        for sidecar in self.node_caches.values():
            sidecar.shutdown()
        self.node_caches.clear()
        self.__nodes = list(client.all_nodes)

        for node in self.__nodes:
            node_cache = NodeTrackingCache(
                cache=self.__cache, max_idle_seconds=self.__max_idle_seconds
            )
            await node_cache.initialize(node)
            assert node_cache.connection
            self.node_caches[node_cache.connection.location] = node_cache
        return self

    @property
    def healthy(self) -> bool:
        return bool(
            self.node_caches
            and all(cache.healthy for cache in self.node_caches.values())
        )

    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        return self.node_caches[connection.location].client_id

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        return self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LRUCache()).setdefault(
            command, LRUCache()
        ).insert(self.hashable_args(*args), value)

    def reset(self, *keys: ValueT) -> None:
        if keys is not None:
            for k in keys:
                self.__cache.remove(b(k))
        else:
            self.__cache.clear()

    def shutdown(self) -> None:
        if self.node_caches:
            for sidecar in self.node_caches.values():
                sidecar.shutdown()
            self.node_caches.clear()
            self.__nodes.clear()

    def __del__(self) -> None:
        self.shutdown()


class TrackingCache(AbstractCache):
    """
    An LRU cache that uses server assisted client caching to ensure local cache entries
    are invalidated if any operations are performed on the keys by another client.

    This class proxies to either :class:`~coredis.cache.NodeTrackingCache`
    or :class:`~coredis.cache.ClusterTrackingCache` depending on which type of client
    it is passed into.
    """

    def __init__(
        self,
        max_keys: int = 2**12,
        max_size_bytes: int = 64 * 1024 * 1024,
        max_idle_seconds: int = 5,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        """
        self.max_keys = max_keys
        self.max_size_bytes = max_size_bytes
        self.max_idle_seconds = max_idle_seconds
        self.instance: Optional[AbstractCache] = None

    async def initialize(
        self, client: "coredis.client.RedisConnection"
    ) -> TrackingCache:
        import coredis.client

        if not self.instance:
            if isinstance(client, coredis.client.RedisCluster):
                self.instance = ClusterTrackingCache(
                    self.max_keys, self.max_size_bytes, self.max_idle_seconds
                )
            else:
                self.instance = NodeTrackingCache(
                    self.max_keys, self.max_size_bytes, self.max_idle_seconds
                )
        await self.instance.initialize(client)
        return self

    @property
    def healthy(self) -> bool:
        return bool(self.instance and self.instance.healthy)

    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        if self.instance:
            return self.instance.get_client_id(connection)
        return None

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        assert self.instance
        return self.instance.get(command, key, *args)

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        if self.instance:
            self.instance.put(command, key, *args, value=value)

    def reset(self, *keys: ValueT) -> None:
        if self.instance:
            self.instance.reset(*keys)

    def shutdown(self) -> None:
        if self.instance:
            self.instance.shutdown()

    def __del__(self) -> None:
        self.shutdown()
