from __future__ import annotations

import asyncio
import dataclasses
import time
import weakref
from abc import ABC, abstractmethod
from collections import Counter
from typing import TYPE_CHECKING, Any, runtime_checkable

from pympler import asizeof

from coredis._sidecar import Sidecar
from coredis._utils import b
from coredis.commands import PubSub
from coredis.connection import BaseConnection
from coredis.typing import (
    Dict,
    Generic,
    Hashable,
    List,
    Literal,
    Optional,
    OrderedDict,
    Protocol,
    ResponseType,
    Tuple,
    TypeVar,
    Union,
    ValueT,
)

if TYPE_CHECKING:
    import coredis.client


@dataclasses.dataclass
class CacheStats:
    """
    Summary of statics to be used by instances of :class:`coredis.cache.AbstractCache`
    The individual counters exposed are not guaranteed to retain fine grained per key
    metrics but the totals (returned by :attr:`coredis.cache.CacheStats.summary`) will be maintained
    aggregated.
    """

    #: summary of hits by key (for all commands)
    hits: Counter[bytes] = dataclasses.field(default_factory=Counter)
    #: summary of misses by key (for all commands)
    misses: Counter[bytes] = dataclasses.field(default_factory=Counter)
    #: number of invalidations including server side and local invalidations
    invalidations: Counter[bytes] = dataclasses.field(default_factory=Counter)
    #: counter of keys which returned dirty results based on confidence testing
    dirty: Counter[bytes] = dataclasses.field(default_factory=Counter)

    def clear(self) -> None:
        self.hits.clear()
        self.misses.clear()
        self.invalidations.clear()
        self.dirty.clear()

    def compact(self) -> None:
        """
        Collapse totals into a single key to avoid unbounded growth of stats

        :meta private:
        """
        for counter in [self.hits, self.misses, self.invalidations, self.dirty]:
            total = sum(counter.values())
            counter.clear()
            counter[b"__coredis__internal__stats__total"] = total

    def hit(self, key: ValueT) -> None:
        self.hits[b(key)] += 1

    def miss(self, key: ValueT) -> None:
        self.misses[b(key)] += 1

    def invalidate(self, key: ValueT) -> None:
        self.invalidations[b(key)] += 1

    def mark_dirty(self, key: ValueT) -> None:
        self.dirty[b(key)] += 1

    @property
    def summary(self) -> Dict[str, int]:
        """
        Aggregated totals of ``hits``, ``misses``, ``dirty_hits``
        and ``invalidations``
        """
        return {
            "hits": sum(self.hits.values()),
            "misses": sum(self.misses.values()),
            "dirty_hits": sum(self.dirty.values()),
            "invalidations": sum(self.invalidations.values()),
        }

    def __repr__(self) -> str:
        summary = self.summary
        return (
            f"CacheStats<hits={summary['hits']}, "
            f"misses={summary['misses']}, "
            f"dirty_hits={summary['dirty_hits']}, "
            f"invalidations={summary['invalidations']}>"
        )


class AbstractCache(ABC):
    """
    Abstract class representing a local cache that can be used by
    :class:`coredis.Redis` or :class:`coredis.RedisCluster`
    """

    @abstractmethod
    async def initialize(
        self,
        client: Union["coredis.client.Redis[Any]", "coredis.client.RedisCluster[Any]"],
    ) -> AbstractCache:
        """
        Associate and initialize this cache with the provided client
        """
        ...

    @property
    @abstractmethod
    def healthy(self) -> bool:
        """
        Whether the cache is healthy and should be taken seriously
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
    def invalidate(self, *keys: ValueT) -> None:
        """
        Invalidate any cached entries for the provided keys
        """
        ...

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the cache
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


@runtime_checkable
class SupportsStats(Protocol):
    """
    Protocol of a cache that provides cache statistics
    """

    @property
    @abstractmethod
    def stats(self) -> CacheStats:
        """
        Returns the current stats for the cache
        """
        ...


@runtime_checkable
class SupportsSampling(Protocol):
    """
    If a cache implements :class:`SupportsSampling`, methods that support
    caching will sample the response from the cache and test it against an uncached
    response from the server based on the confidence returned by :attr:`confidence`.
    The outcome of the validation will be fed back to the cache using :meth:`feedback`
    and in the case that there was no match, the uncached response will be returned.
    """

    @property
    @abstractmethod
    def confidence(self) -> float:
        """
        Confidence in cached values between 0 - 100. Lower values
        will result in the client discarding and / or validating the
        cached responses
        """
        ...

    @abstractmethod
    def feedback(self, command: bytes, key: bytes, *args: ValueT, match: bool) -> None:
        """
        Provide feedback about a key as having either a match or drift from the actual
        server side value
        """
        ...


@runtime_checkable
class SupportsClientTracking(Protocol):
    """
    If a cache implements :class:`SupportsClientTracking`, the :class:`~coredis.Redis`
    and :class:`~coredis.RedisCluster` clients will ensure that the client
    returned by :meth:`get_client_id` is set using the :meth:`~coredis.Redis.client_tracking`
    command on any connection returned by the clients.
    """

    @abstractmethod
    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        """
        If the cache supports receiving invalidation events from the server
        return the ``client_id`` that the :paramref:`connection` should send
        redirects to.
        """
        ...


ET = TypeVar("ET")


class LRUCache(Generic[ET]):
    def __init__(self, max_items: int = -1, max_bytes: int = -1):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.__cache: OrderedDict[Hashable, ET] = OrderedDict()
        if self.max_bytes > 0:
            self.max_bytes += asizeof.asizeof(self.__cache)

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
        the removal of its oldest entry and if that
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
            while asizeof.asizeof(self.__cache) > self.max_bytes:
                if not self.popitem():
                    # nothing left to remove
                    return

    def __repr__(self) -> str:
        return (
            f"LruCache<max_items={self.max_items}, "
            f"current_items={len(self.__cache)}, "
            f"max_bytes={self.max_bytes}, "
            f"current_size_bytes={asizeof.asizeof(self)}>"
        )

    def __check_capacity(self) -> None:
        if len(self.__cache) == self.max_items:
            self.__cache.popitem(last=False)


class NodeTrackingCache(
    Sidecar,
    AbstractCache,
    SupportsStats,
    SupportsSampling,
    SupportsClientTracking,
):
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
        confidence: float = 100,
        dynamic_confidence: bool = False,
        cache: Optional[LRUCache[LRUCache[LRUCache[ResponseType]]]] = None,
        stats: Optional[CacheStats] = None,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        :param confidence: 0 - 100. Lower values will result in the client
         discarding and / or validating the cached responses
        :param dynamic_confidence: Whether to adjust the confidence based on
         sampled validations. Tainted values drop the confidence by 0.1% and
         confirmations of correct cached values will increase the confidence by 0.01%
         upto 100.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__invalidation_task: Optional[asyncio.Task[None]] = None
        self.__compact_task: Optional[asyncio.Task[None]] = None
        self.__cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self.__max_idle_seconds = max_idle_seconds
        self.__confidence = self.__original_confidence = confidence
        self.__dynamic_confidence = dynamic_confidence
        self.__stats = stats or CacheStats()
        super().__init__({b"invalidate"}, max(1, max_idle_seconds - 1))

    @property
    def healthy(self) -> bool:
        return bool(
            self.connection
            and self.connection.is_connected
            and time.monotonic() - self.last_checkin < self.__max_idle_seconds
        )

    @property
    def confidence(self) -> float:
        return self.__confidence

    @property
    def stats(self) -> CacheStats:
        return self.__stats

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        try:
            cached = (
                self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))
            )
            self.__stats.hit(key)
            return cached
        except KeyError:
            self.__stats.miss(key)
            raise

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LRUCache()).setdefault(
            command, LRUCache()
        ).insert(self.hashable_args(*args), value)

    def invalidate(self, *keys: ValueT) -> None:
        for key in keys:
            self.__stats.invalidate(key)
            self.__cache.remove(b(key))

    def feedback(self, command: bytes, key: bytes, *args: ValueT, match: bool) -> None:
        if not match:
            self.__stats.mark_dirty(key)
            self.invalidate(key)
        if self.__dynamic_confidence:
            self.__confidence = min(
                100.0,
                max(0.0, self.__confidence * (1.0001 if match else 0.999)),
            )

    def reset(self) -> None:
        self.__cache.clear()
        self.__stats.compact()
        self.__confidence = self.__original_confidence

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
        return ()  # noqa

    async def initialize(
        self,
        client: Union["coredis.client.Redis[Any]", "coredis.client.RedisCluster[Any]"],
    ) -> NodeTrackingCache:
        self.__protocol_version = client.protocol_version
        await super().start(client)
        if not self.__invalidation_task or self.__invalidation_task.done():
            self.__invalidation_task = asyncio.create_task(self.__invalidate())
        if not self.__compact_task or self.__compact_task.done():
            self.__compact_task = asyncio.create_task(self.__compact())
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
            if self.__compact_task:
                self.__compact_task.cancel()
            super().stop()
        except RuntimeError:
            pass

    def get_client_id(self, client: BaseConnection) -> Optional[int]:
        if self.connection and self.connection.is_connected:
            return self.client_id
        return None

    async def __compact(self) -> None:
        while True:
            try:
                self.__cache.shrink()
                self.__stats.compact()
                await asyncio.sleep(max(1, self.__max_idle_seconds - 1))
            except asyncio.CancelledError:
                break

    async def __invalidate(self) -> None:
        while True:
            try:
                key = b(await self.messages.get())
                self.invalidate(key)
                self.messages.task_done()
            except asyncio.CancelledError:
                break
            except RuntimeError:  # noqa
                break


class ClusterTrackingCache(
    AbstractCache, SupportsStats, SupportsSampling, SupportsClientTracking
):
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
        confidence: float = 100,
        dynamic_confidence: bool = False,
        cache: Optional[LRUCache[LRUCache[LRUCache[ResponseType]]]] = None,
        stats: Optional[CacheStats] = None,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        :param confidence: 0 - 100. Lower values will result in the client
         discarding and / or validating the cached responses
        :param dynamic_confidence: Whether to adjust the confidence based on
         sampled validations. Tainted values drop the confidence by 0.1% and
         confirmations of correct cached values will increase the confidence by 0.01%
         upto 100.
        """
        self.__protocol_version: Optional[Literal[2, 3]] = None
        self.__cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self.node_caches: Dict[str, NodeTrackingCache] = {}
        self.__nodes: List["coredis.client.Redis[Any]"] = []
        self.__max_idle_seconds = max_idle_seconds
        self.__confidence = self.__original_confidence = confidence
        self.__dynamic_confidence = dynamic_confidence
        self.__stats = stats or CacheStats()
        self.__client: Optional[
            weakref.ReferenceType["coredis.client.RedisCluster[Any]"]
        ] = None

    async def initialize(
        self,
        client: Union["coredis.client.Redis[Any]", "coredis.client.RedisCluster[Any]"],
    ) -> ClusterTrackingCache:
        import coredis.client

        assert isinstance(client, coredis.client.RedisCluster)

        self.__client = weakref.ref(client)
        self.__cache.clear()
        for sidecar in self.node_caches.values():
            sidecar.shutdown()
        self.node_caches.clear()
        self.__nodes = list(client.all_nodes)

        for node in self.__nodes:
            node_cache = NodeTrackingCache(
                max_idle_seconds=self.__max_idle_seconds,
                confidence=self.__confidence,
                dynamic_confidence=self.__dynamic_confidence,
                cache=self.__cache,
                stats=self.__stats,
            )
            await node_cache.initialize(node)
            assert node_cache.connection
            self.node_caches[node_cache.connection.location] = node_cache
        return self

    @property
    def client(self) -> Optional["coredis.client.RedisCluster[Any]"]:
        if self.__client:
            return self.__client()
        return None  # noqa

    @property
    def healthy(self) -> bool:
        return bool(
            self.client
            and self.client.connection_pool.initialized
            and self.node_caches
            and all(cache.healthy for cache in self.node_caches.values())
        )

    @property
    def confidence(self) -> float:
        return self.__confidence

    @property
    def stats(self) -> CacheStats:
        return self.__stats

    def get_client_id(self, connection: BaseConnection) -> Optional[int]:
        try:
            return self.node_caches[connection.location].get_client_id(connection)
        except KeyError:
            return None

    def get(self, command: bytes, key: bytes, *args: ValueT) -> ResponseType:
        try:
            cached = (
                self.__cache.get(b(key)).get(command).get(self.hashable_args(*args))
            )
            self.__stats.hit(key)
            return cached
        except KeyError:
            self.__stats.miss(key)
            raise

    def put(
        self, command: bytes, key: bytes, *args: ValueT, value: ResponseType
    ) -> None:
        self.__cache.setdefault(b(key), LRUCache()).setdefault(
            command, LRUCache()
        ).insert(self.hashable_args(*args), value)

    def invalidate(self, *keys: ValueT) -> None:
        for key in keys:
            self.__stats.invalidate(key)
            self.__cache.remove(b(key))

    def feedback(self, command: bytes, key: bytes, *args: ValueT, match: bool) -> None:
        if not match:
            self.__stats.mark_dirty(key)
            self.invalidate(key)
        if self.__dynamic_confidence:
            self.__confidence = min(
                100.0,
                max(0.0, self.__confidence * (1.0001 if match else 0.999)),
            )

    def reset(self) -> None:
        self.__cache.clear()
        self.__stats.compact()
        self.__confidence = self.__original_confidence

    def shutdown(self) -> None:
        if self.node_caches:
            for sidecar in self.node_caches.values():
                sidecar.shutdown()
            self.node_caches.clear()
            self.__nodes.clear()

    def __del__(self) -> None:
        self.shutdown()


class TrackingCache(
    AbstractCache, SupportsStats, SupportsSampling, SupportsClientTracking
):
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
        confidence: float = 100.0,
        dynamic_confidence: bool = False,
        cache: Optional[LRUCache[LRUCache[LRUCache[ResponseType]]]] = None,
        stats: Optional[CacheStats] = None,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param max_idle_seconds: maximum duration to tolerate no updates
         from the server. When the duration is exceeded the connection
         and cache will be reset.
        :param confidence: 0 - 100. Lower values will result in the client
         discarding and / or validating the cached responses
        :param dynamic_confidence: Whether to adjust the confidence based on
         sampled validations. Tainted values drop the confidence by 0.1% and
         confirmations of correct cached values will increase the confidence by 0.01%
         upto 100.
        """
        self.instance: Optional[Union[ClusterTrackingCache, NodeTrackingCache]] = None
        self.__max_keys = max_keys
        self.__max_size_bytes = max_size_bytes
        self.__max_idle_seconds = max_idle_seconds
        self.__confidence = confidence
        self.__dynamic_confidence = dynamic_confidence
        self.__cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self.__client: Optional[
            weakref.ReferenceType["coredis.client.Client[Any]"]
        ] = None
        self.__stats = stats or CacheStats()

    async def initialize(
        self,
        client: Union["coredis.client.Redis[Any]", "coredis.client.RedisCluster[Any]"],
    ) -> TrackingCache:
        import coredis.client

        if self.__client and self.__client() != client:
            copy = self.share()
            return await copy.initialize(client)

        self.__client = weakref.ref(client)

        if not self.instance:
            if isinstance(client, coredis.client.RedisCluster):
                self.instance = ClusterTrackingCache(
                    self.__max_keys,
                    self.__max_size_bytes,
                    self.__max_idle_seconds,
                    confidence=self.__confidence,
                    dynamic_confidence=self.__dynamic_confidence,
                    cache=self.__cache,
                    stats=self.__stats,
                )
            else:
                self.instance = NodeTrackingCache(
                    self.__max_keys,
                    self.__max_size_bytes,
                    self.__max_idle_seconds,
                    confidence=self.__confidence,
                    dynamic_confidence=self.__dynamic_confidence,
                    cache=self.__cache,
                    stats=self.__stats,
                )
        await self.instance.initialize(client)
        return self

    @property
    def healthy(self) -> bool:
        return bool(self.instance and self.instance.healthy)

    @property
    def confidence(self) -> float:
        if not self.instance:
            return self.__confidence
        return self.instance.confidence

    @property
    def stats(self) -> CacheStats:
        return self.__stats

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

    def invalidate(self, *keys: ValueT) -> None:
        if self.instance:
            self.instance.invalidate(*keys)

    def feedback(self, command: bytes, key: bytes, *args: ValueT, match: bool) -> None:
        if self.instance:
            self.instance.feedback(command, key, *args, match=match)

    def reset(self) -> None:
        if self.instance:
            self.instance.reset()

    def shutdown(self) -> None:
        if self.instance:
            self.instance.shutdown()
        self.__client = None

    def share(self) -> TrackingCache:
        """
        Create a copy of this cache that can be used to share
        memory with another client.

        In the example below ``c1`` and ``c2`` have their own
        instances of :class:`~coredis.cache.TrackingCache` but
        share the same in-memory local cached responses::

            c1 = await coredis.Redis(cache=TrackingCache())
            c2 = await coredis.Redis(cache=c1.cache.share())
        """
        copy = self.__class__(
            self.__max_keys,
            self.__max_size_bytes,
            self.__max_idle_seconds,
            self.__confidence,
            self.__dynamic_confidence,
            self.__cache,
            self.__stats,
        )
        return copy

    def __del__(self) -> None:
        self.shutdown()
