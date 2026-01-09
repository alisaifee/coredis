from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections import Counter
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, cast

from anyio import (
    TASK_STATUS_IGNORED,
    ConnectionFailed,
    EndOfStream,
    create_task_group,
    current_time,
    sleep,
)
from anyio.abc import TaskStatus
from exceptiongroup import catch

from coredis._utils import b, logger, make_hashable
from coredis.commands.constants import CommandName
from coredis.exceptions import ConnectionError
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
_retryable_errors = (ConnectionError, ConnectionFailed, EndOfStream)


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

    @abstractmethod
    def shrink(self) -> None:
        """
        Shrink the cache to an acceptable size
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


class BoundedStorage(Generic[ET]):
    """
    Low-level LRU container.
    """

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
        the oldest entry is another BoundedStorage trigger
        the removal of its oldest entry and if that
        turns out to be an empty BoundedStorage, remove that.
        """
        try:
            oldest = next(iter(self._cache))
            item = self._cache[oldest]
        except StopIteration:
            return None

        if isinstance(item, BoundedStorage):
            if popped := item.popitem():
                return popped
        if entry := self._cache.popitem(last=False):
            return entry
        return None

    def shrink(self) -> None:
        """
        Remove old entries until the size of the cache
        is less than :paramref:`BoundedStorage.max_bytes` or if
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
                f"BoundedStorage<max_items={self.max_items}, "
                f"current_items={len(self._cache)}, "
                f"max_bytes={self.max_bytes}, "
                f"current_size_bytes={asizeof.asizeof(self._cache)}>"
            )
        else:
            return f"LruCache<max_items={self.max_items}, current_items={len(self._cache)}, "

    def _check_capacity(self) -> None:
        if len(self._cache) == self.max_items:
            self._cache.popitem(last=False)


class LRUCache(AbstractCache):
    """
    Concrete implementation of AbstractCache using an LRU eviction policy.
    Maintains storage, statistics, and confidence levels.
    """

    def __init__(
        self,
        max_keys: int = 2**12,
        max_size_bytes: int = 64 * 1024 * 1024,
        confidence: float = 100,
        dynamic_confidence: bool = False,
    ) -> None:
        """
        :param max_keys: maximum keys to cache. A negative value represents
         and unbounded cache.
        :param max_size_bytes: maximum size in bytes for the local cache.
         A negative value represents an unbounded cache.
        :param confidence: 0 - 100. Lower values will result in the client
         discarding and / or validating the cached responses
        :param dynamic_confidence: Whether to adjust the confidence based on
         sampled validations. Tainted values drop the confidence by 0.1% and
         confirmations of correct cached values will increase the confidence by 0.01%
         up to 100.
        """
        self._confidence = self._original_confidence = confidence
        self._dynamic_confidence = dynamic_confidence
        self._stats = CacheStats()
        # Nesting: Key -> Command -> Args -> Response
        self._storage: BoundedStorage[BoundedStorage[BoundedStorage[ResponseType]]] = (
            BoundedStorage(max_keys, max_size_bytes)
        )

    @property
    def stats(self) -> CacheStats:
        return self._stats

    @property
    def confidence(self) -> float:
        return self._confidence

    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        try:
            cached = self._storage.get(b(key)).get(command).get(make_hashable(*args))
            self._stats.hit(key)
            return cached
        except KeyError:
            self._stats.miss(key)
            raise

    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        self._storage.setdefault(b(key), BoundedStorage()).setdefault(
            command, BoundedStorage()
        ).insert(make_hashable(*args), value)

    def invalidate(self, *keys: RedisValueT) -> None:
        for key in keys:
            self._stats.invalidate(key)
            self._storage.remove(b(key))

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
        self._storage.clear()
        self._stats.compact()
        self._confidence = self._original_confidence

    def shrink(self) -> None:
        self._storage.shrink()
        self._stats.compact()


class TrackingCache(AbstractCache):
    """
    Abstract layout of a tracking cache to be used internally
    by coredis clients (Redis/RedisCluster)
    """

    _cache: AbstractCache

    @abstractmethod
    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        pass

    @abstractmethod
    def get_client_id(
        self,
        connection: coredis.connection.BaseConnection,
    ) -> int | None:
        pass

    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        return self._cache.get(command, key, *args)

    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        self._cache.put(command, key, *args, value=value)

    def invalidate(self, *keys: RedisValueT) -> None:
        self._cache.invalidate(*keys)

    def reset(self) -> None:
        self._cache.reset()

    def shrink(self) -> None:
        self._cache.shrink()

    @property
    def stats(self) -> CacheStats:
        return self._cache.stats

    @property
    def confidence(self) -> float:
        return self._cache.confidence

    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        self._cache.feedback(command, key, *args, match=match)


class NodeTrackingCache(TrackingCache):
    """
    Wraps an AbstractCache instance to use server assisted client caching
    to ensure local cache entries are invalidated if any operations are
    performed on the keys by another client.
    """

    def __init__(
        self, cache: AbstractCache | None = None, compact_interval_seconds: int = 300
    ) -> None:
        """
        :param cache: AbstractCache instance to wrap
        :param compact_interval_seconds: frequency to check if cache is too big and shrink it
        """
        self._cache = cache or LRUCache()
        self.client_id: int | None = None
        self.compact_interval = compact_interval_seconds

    def get_client_id(
        self,
        connection: coredis.connection.BaseConnection,
    ) -> int | None:
        return self.client_id

    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """
        Run a single connection that listens for invalidation messages,
        with reconnection logic.
        """
        start_time, started, tries = current_time(), False, 0

        def handle_error(*args: Any) -> None:
            nonlocal tries, start_time
            if current_time() - start_time > 10:
                tries = 0
            else:
                tries += 1
            logger.warning("Cache connection lost, retrying...")

        while True:
            # retry with exponential backoff
            await sleep(min(tries**2, 300))
            with catch({_retryable_errors: handle_error}):
                async with pool.acquire() as self._connection:
                    if self._connection.tracking_client_id:
                        await self._connection.update_tracking_client(False)
                    self.client_id = self._connection.client_id
                    start_time = current_time()
                    async with create_task_group() as self._tg:
                        self._tg.start_soon(self._consumer)
                        self._tg.start_soon(self._keepalive)
                        self._tg.start_soon(self._compact)
                        if not started:
                            task_status.started()
                            started = True
                        else:  # flush cache
                            self.reset()

    async def _keepalive(self) -> None:
        while True:
            await self._connection.send_command(CommandName.PING)
            await sleep(15)

    async def _consumer(self) -> None:
        while True:
            response = await self._connection.fetch_push_message(True)
            messages = cast(list[StringT], response[1] or [])
            for key in messages:
                self._cache.invalidate(key)

    async def _compact(self) -> None:
        while True:
            await sleep(self.compact_interval)
            self.shrink()


class ClusterTrackingCache(TrackingCache):
    """
    An LRU cache for redis cluster that uses server assisted client caching
    to ensure local cache entries are invalidated if any operations are performed
    on the keys by another client.

    The cache maintains an additional connection per node (including replicas)
    in the cluster to listen to invalidation events
    """

    def get_client_id(
        self,
        connection: coredis.connection.BaseConnection,
    ) -> int | None:
        if cache := self.node_caches.get(connection.location):
            return cache.client_id
        return None

    def __init__(self, cache: AbstractCache | None = None) -> None:
        """ """
        self.node_caches: dict[str, NodeTrackingCache] = {}
        self._cache = cache or LRUCache()
        self._nodes: list[coredis.client.Redis[Any]] = []

    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        assert isinstance(pool, ClusterConnectionPool)
        self._nodes = [
            pool.nodes.get_redis_link(node.host, node.port) for node in pool.nodes.all_nodes()
        ]
        async with AsyncExitStack() as stack:
            nodes = []
            for node in self._nodes:
                nodes.append(await stack.enter_async_context(node))

            async with create_task_group() as tg:
                self._task_group = tg

                for node in nodes:
                    node_cache = NodeTrackingCache(cache=self._cache)
                    await tg.start(node_cache.run, node.connection_pool)
                    self.node_caches[node_cache._connection.location] = node_cache
                task_status.started()
