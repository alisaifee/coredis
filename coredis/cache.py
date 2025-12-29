from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections import Counter
from typing import TYPE_CHECKING, Any, cast

from anyio import (
    TASK_STATUS_IGNORED,
    ConnectionFailed,
    EndOfStream,
    create_task_group,
    sleep,
)
from anyio.abc import TaskStatus
from exceptiongroup import BaseExceptionGroup, catch

from coredis._utils import b, logger, make_hashable
from coredis.commands.constants import CommandName
from coredis.connection import BaseConnection
from coredis.pool.basic import ConnectionPool
from coredis.pool.cluster import ClusterConnectionPool
from coredis.typing import (
    Generic,
    Hashable,
    ModuleType,
    OrderedDict,
    RedisValueT,
    ResponseType,
    StringT,
    TypeVar,
)

asizeof: ModuleType | None = None

try:
    from pympler import asizeof
except (AttributeError, KeyError):
    # Not available in pypy
    pass

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

    def hit(self, key: RedisValueT) -> None:
        self.hits[b(key)] += 1

    def miss(self, key: RedisValueT) -> None:
        self.misses[b(key)] += 1

    def invalidate(self, key: RedisValueT) -> None:
        self.invalidations[b(key)] += 1

    def mark_dirty(self, key: RedisValueT) -> None:
        self.dirty[b(key)] += 1

    @property
    def summary(self) -> dict[str, int]:
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
    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        """
        Fetch the cached response for command/key/args combination
        """
        ...

    @abstractmethod
    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        """
        Cache the response for command/key/args combination
        """
        ...

    @abstractmethod
    def invalidate(self, *keys: RedisValueT) -> None:
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

    @property
    @abstractmethod
    def stats(self) -> CacheStats:
        """
        Returns the current stats for the cache
        """
        ...

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
    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        """
        Provide feedback about a key as having either a match or drift from the actual
        server side value
        """
        ...


ET = TypeVar("ET")


class LRUCache(Generic[ET]):
    def __init__(self, max_items: int = -1, max_bytes: int = -1):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self._cache: OrderedDict[Hashable, ET] = OrderedDict()

        if self.max_bytes > 0 and asizeof is not None:
            self.max_bytes += asizeof.asizeof(self._cache)
        elif self.max_bytes > 0:
            raise RuntimeError("max_bytes not supported as dependency pympler not available")

    def get(self, key: Hashable) -> ET:
        if key not in self._cache:
            raise KeyError(key)
        self._cache.move_to_end(key)

        return self._cache[key]

    def insert(self, key: Hashable, value: ET) -> None:
        self._check_capacity()
        self._cache[key] = value
        self._cache.move_to_end(key)

    def setdefault(self, key: Hashable, value: ET) -> ET:
        try:
            self._check_capacity()

            return self.get(key)
        except KeyError:
            self.insert(key, value)

            return self.get(key)

    def remove(self, key: Hashable) -> None:
        if key in self._cache:
            self._cache.pop(key)

    def clear(self) -> None:
        self._cache.clear()

    def popitem(self) -> tuple[Any, Any] | None:
        """
        Recursively remove the oldest entry. If
        the oldest entry is another LRUCache trigger
        the removal of its oldest entry and if that
        turns out to be an empty LRUCache, remove that.
        """
        try:
            oldest = next(iter(self._cache))
            item = self._cache[oldest]
        except StopIteration:
            return None

        if isinstance(item, LRUCache):
            if popped := item.popitem():
                return popped
        if entry := self._cache.popitem(last=False):
            return entry
        return None

    def shrink(self) -> None:
        """
        Remove old entries until the size of the cache
        is less than :paramref:`LRUCache.max_bytes` or if
        there is nothing left to remove.
        """

        if self.max_bytes > 0 and asizeof is not None:
            cur_size = asizeof.asizeof(self._cache)
            while cur_size > self.max_bytes:
                if (popped := self.popitem()) is None:
                    return
                cur_size -= asizeof.asizeof(popped[0]) + asizeof.asizeof(popped[1])

    def __repr__(self) -> str:
        if asizeof is not None:
            return (
                f"LruCache<max_items={self.max_items}, "
                f"current_items={len(self._cache)}, "
                f"max_bytes={self.max_bytes}, "
                f"current_size_bytes={asizeof.asizeof(self._cache)}>"
            )
        else:
            return f"LruCache<max_items={self.max_items}, current_items={len(self._cache)}, "

    def _check_capacity(self) -> None:
        if len(self._cache) == self.max_items:
            self._cache.popitem(last=False)


class NodeTrackingCache(AbstractCache):
    """
    An LRU cache that uses server assisted client caching
    to ensure local cache entries are invalidated if any
    operations are performed on the keys by another client.
    """

    def __init__(
        self,
        max_keys: int = 2**12,
        max_size_bytes: int = 64 * 1024 * 1024,
        max_idle_seconds: int = 30,
        confidence: float = 100,
        dynamic_confidence: bool = False,
        cache: LRUCache[LRUCache[LRUCache[ResponseType]]] | None = None,
        stats: CacheStats | None = None,
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
        self._max_idle_seconds = max_idle_seconds
        self._confidence = self._original_confidence = confidence
        self._dynamic_confidence = dynamic_confidence
        self._stats = stats or CacheStats()
        self._cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self.tries = 0
        self.client_id: int | None = None

    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """
        Run a single connection that listens for invalidation messages,
        with reconnection logic.
        """

        def handle_exception_group(group: BaseExceptionGroup) -> None:
            logger.error("Cache disconnected!")
            for error in group.exceptions:
                logger.error(error)
            logger.warning("Retrying...")

        started = False
        while True:
            # retry with exponential backoff
            await sleep(self.tries**2)
            self.tries += 1
            with catch({(ConnectionError, ConnectionFailed, EndOfStream): handle_exception_group}):
                async with pool.acquire() as self._connection:
                    if self._connection.tracking_client_id:
                        await self._connection.update_tracking_client(False)
                    self.client_id = self._connection.client_id
                    async with create_task_group() as tg:
                        tg.start_soon(self._consumer)
                        tg.start_soon(self._keepalive)
                        tg.start_soon(self._compact)
                        if not started:
                            task_status.started()
                            started = True
                        else:  # flush cache
                            self.reset()

    async def _keepalive(self) -> None:
        while True:
            await self._connection.send_command(CommandName.PING)
            self.tries = 0
            await sleep(30)

    async def _consumer(self) -> None:
        while True:
            response = await self._connection.fetch_push_message(True)
            messages = cast(list[StringT], response[1] or [])
            for key in messages:
                self.invalidate(key)

    async def _compact(self) -> None:
        while True:
            self._cache.shrink()
            self._stats.compact()
            await sleep(max(1, self._max_idle_seconds - 1))

    @property
    def confidence(self) -> float:
        return self._confidence

    @property
    def stats(self) -> CacheStats:
        return self._stats

    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        try:
            cached = self._cache.get(b(key)).get(command).get(make_hashable(*args))
            self._stats.hit(key)

            return cached
        except KeyError:
            self._stats.miss(key)
            raise

    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        self._cache.setdefault(b(key), LRUCache()).setdefault(command, LRUCache()).insert(
            make_hashable(*args), value
        )

    def invalidate(self, *keys: RedisValueT) -> None:
        for key in keys:
            self._stats.invalidate(key)
            self._cache.remove(b(key))

    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        if not match:
            self._stats.mark_dirty(key)
            self.invalidate(key)

        if self._dynamic_confidence:
            self._confidence = min(
                100.0,
                max(0.0, self._confidence * (1.0001 if match else 0.999)),
            )

    def reset(self) -> None:
        self._cache.clear()
        self._stats.compact()
        self._confidence = self._original_confidence


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
        confidence: float = 100,
        dynamic_confidence: bool = False,
        cache: LRUCache[LRUCache[LRUCache[ResponseType]]] | None = None,
        stats: CacheStats | None = None,
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
        self.node_caches: dict[str, NodeTrackingCache] = {}
        self._cache: LRUCache[LRUCache[LRUCache[ResponseType]]] = cache or LRUCache(
            max_keys, max_size_bytes
        )
        self._nodes: list[coredis.client.Redis[Any]] = []
        self._max_idle_seconds = max_idle_seconds
        self._confidence = self._original_confidence = confidence
        self._dynamic_confidence = dynamic_confidence
        self._stats = stats or CacheStats()

    async def run(
        self, pool: ClusterConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        self._nodes = [
            pool.nodes.get_redis_link(node.host, node.port) for node in pool.nodes.all_nodes()
        ]
        # TODO: make this work with cluster pool structure
        async with create_task_group() as tg:
            for node in self._nodes:
                node_cache = NodeTrackingCache(
                    max_idle_seconds=self._max_idle_seconds,
                    confidence=self._confidence,
                    dynamic_confidence=self._dynamic_confidence,
                    cache=self._cache,
                    stats=self._stats,
                )
                await tg.start(node_cache.run, pool)
                self.node_caches[node_cache._connection.location] = node_cache
            task_status.started()

    @property
    def confidence(self) -> float:
        return self._confidence

    @property
    def stats(self) -> CacheStats:
        return self._stats

    def get_client_id(self, connection: BaseConnection) -> int | None:
        try:
            return self.node_caches[connection.location].get_client_id(connection)  # type: ignore
        except KeyError:
            return None

    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        try:
            cached = self._cache.get(b(key)).get(command).get(make_hashable(*args))
            self._stats.hit(key)

            return cached
        except KeyError:
            self._stats.miss(key)
            raise

    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        self._cache.setdefault(b(key), LRUCache()).setdefault(command, LRUCache()).insert(
            make_hashable(*args), value
        )

    def invalidate(self, *keys: RedisValueT) -> None:
        for key in keys:
            self._stats.invalidate(key)
            self._cache.remove(b(key))

    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        if not match:
            self._stats.mark_dirty(key)
            self.invalidate(key)

        if self._dynamic_confidence:
            self._confidence = min(
                100.0,
                max(0.0, self._confidence * (1.0001 if match else 0.999)),
            )

    def reset(self) -> None:
        self._cache.clear()
        self._stats.compact()
        self._confidence = self._original_confidence
