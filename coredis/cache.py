from __future__ import annotations

import dataclasses
import time
from abc import ABC, abstractmethod
from collections import Counter
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, cast

from anyio import (
    TASK_STATUS_IGNORED,
    create_task_group,
    sleep,
)
from anyio.abc import TaskStatus

from coredis._utils import b, make_hashable
from coredis.commands.constants import CommandName
from coredis.exceptions import ConnectionError
from coredis.retry import ExponentialBackoffRetryPolicy
from coredis.typing import (
    OrderedDict,
    RedisValueT,
    ResponseType,
    StringT,
)

if TYPE_CHECKING:
    import coredis.client
    from coredis.pool.basic import ConnectionPool


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

    @property
    @abstractmethod
    def healthy(self) -> bool:
        """
        Whether the cache is healthy and should be taken seriuosly
        """
        ...


class LRUCache(AbstractCache):
    def __init__(
        self,
        max_keys: int = 2**12,
        confidence: float = 100,
        dynamic_confidence: bool = False,
    ) -> None:
        """
        Implementation of an LRU cache that can be used
        with :class:`coredis.Redis`  or :class:`coredis.RedisCluster`

        :param max_keys: maximum keys to cache. A negative value represents and unbounded cache.
        :param confidence: 0 - 100. Lower values will result in the client discarding
         and / or validating the cached responses
        :param dynamic_confidence: Whether to adjust the confidence based on sampled validations.
         Tainted values drop the confidence by 0.1% and confirmations of correct cached values
         will increase the confidence by 0.01% upto 100.
        """
        self._confidence = self._original_confidence = confidence
        self._dynamic_confidence = dynamic_confidence
        self._stats = CacheStats()
        self.max_keys = max_keys
        # key -> (command, args) -> response
        self._storage: OrderedDict[bytes, dict[tuple[bytes, Any], ResponseType]] = OrderedDict()

    def put(
        self, command: bytes, key: RedisValueT, *args: RedisValueT, value: ResponseType
    ) -> None:
        key_bytes = b(key)
        composite_key = (command, make_hashable(*args))

        if key_bytes not in self._storage and len(self._storage) >= self.max_keys:
            if self._storage:
                self._storage.popitem(last=False)

        # Get or create the key's cache dict
        if key_bytes not in self._storage:
            self._storage[key_bytes] = {}

        self._storage[key_bytes][composite_key] = value
        self._storage.move_to_end(key_bytes)

    def get(self, command: bytes, key: RedisValueT, *args: RedisValueT) -> ResponseType:
        key_bytes = b(key)
        if key_bytes not in self._storage:
            self._stats.miss(key)
            raise KeyError(key)

        # Move to end for LRU
        self._storage.move_to_end(key_bytes)
        composite_key = (command, make_hashable(*args))
        if composite_key not in self._storage[key_bytes]:
            self._stats.miss(key)
            raise KeyError(key)

        self._stats.hit(key)
        return self._storage[key_bytes][composite_key]

    def invalidate(self, *keys: RedisValueT) -> None:
        for key in keys:
            self._stats.invalidate(key)
            self._storage.pop(b(key), None)

    def reset(self) -> None:
        self._storage.clear()
        self._stats.compact()
        self._confidence = self._original_confidence

    @property
    def stats(self) -> CacheStats:
        return self._stats

    @property
    def confidence(self) -> float:
        return self._confidence

    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        if not match:
            self._stats.mark_dirty(key)
            self.invalidate(key)

        if self._dynamic_confidence:
            self._confidence = min(
                100.0,
                max(0.0, self._confidence * (1.0001 if match else 0.999)),
            )

    @property
    def healthy(self) -> bool:
        """
        The LRU Cache is always "healthy"
        """
        return True


class TrackingCache(AbstractCache):
    """
    Abstract layout of a tracking cache to be used internally
    by coredis clients (Redis/RedisCluster)
    """

    _cache: AbstractCache

    def __init__(self, cache: AbstractCache) -> None:
        self._cache = cache
        self._retry_policy = ExponentialBackoffRetryPolicy(
            (ConnectionError,), retries=None, base_delay=1, max_delay=16, jitter=True
        )

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

    @property
    def stats(self) -> CacheStats:
        return self._cache.stats

    @property
    def confidence(self) -> float:
        if not self.healthy:
            return 0
        return self._cache.confidence

    def feedback(self, command: bytes, key: RedisValueT, *args: RedisValueT, match: bool) -> None:
        self._cache.feedback(command, key, *args, match=match)


class NodeTrackingCache(TrackingCache):
    """
    Wraps an AbstractCache instance to use server assisted client caching
    to ensure local cache entries are invalidated if any operations are
    performed on the keys by another client.
    """

    _connection: coredis.connection.BaseConnection | None

    def __init__(self, cache: AbstractCache | None = None, max_idle_seconds: int = 5) -> None:
        """
        :param cache: AbstractCache instance to wrap
        :param max_idle_seconds: Maximum duration to tolerate no
         updates from the server before marking the cache as unhealthy.
        """
        super().__init__(cache or LRUCache())
        self.client_id: int | None = None
        self.__last_checkin: float = 0
        self.__max_idle_seconds = max_idle_seconds
        self._connection = None

    def get_client_id(
        self,
        connection: coredis.connection.BaseConnection,
    ) -> int | None:
        return self.client_id

    @property
    def healthy(self) -> bool:
        return bool(
            self._connection is not None
            and self._connection.is_connected
            and (time.monotonic() - self.__last_checkin) < self.__max_idle_seconds
        )

    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """
        Run a single connection that listens for invalidation messages,
        with reconnection logic.
        """
        started = False

        async def _run() -> None:
            nonlocal started
            async with pool.acquire() as self._connection:
                if self._connection.tracking_client_id:
                    await self._connection.update_tracking_client(False)
                self.client_id = self._connection.client_id
                async with create_task_group() as self._tg:
                    self._tg.start_soon(self._consumer)
                    self._tg.start_soon(self._keepalive)
                    if not started:
                        task_status.started()
                        started = True
                        self.__last_checkin = time.monotonic()
                    else:  # flush cache
                        self.reset()

        return await self._retry_policy.call_with_retries(_run)

    async def _keepalive(self) -> None:
        while True:
            if self._connection and await self._connection.create_request(CommandName.PING) in {
                b"OK",
                "OK",
            }:
                self.__last_checkin = time.monotonic()
            await sleep(min(1, self.__max_idle_seconds - 1))

    async def _consumer(self) -> None:
        while True:
            if self._connection:
                response = await self._connection.fetch_push_message(True)
                self.__last_checkin = time.monotonic()
                messages = cast(list[StringT], response[1] or [])
                for key in messages:
                    self._cache.invalidate(key)


class ClusterTrackingCache(TrackingCache):
    """
    An LRU cache for redis cluster that uses server assisted client caching
    to ensure local cache entries are invalidated if any operations are performed
    on the keys by another client.

    The cache maintains an additional connection per node (including replicas)
    in the cluster to listen to invalidation events
    """

    def get_client_id(self, connection: coredis.connection.BaseConnection) -> int | None:
        if cache := self.node_caches.get(connection.location):
            return cache.client_id
        return None

    def __init__(self, cache: AbstractCache | None = None, max_idle_seconds: int = 5) -> None:
        """ """
        super().__init__(cache or LRUCache())
        self.node_caches: dict[str, NodeTrackingCache] = {}
        self._nodes: list[coredis.client.Redis[Any]] = []
        self._max_idle_seconds = max_idle_seconds

    @property
    def healthy(self) -> bool:
        return all(cache.healthy for cache in self.node_caches.values())

    async def run(
        self, pool: ConnectionPool, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        from coredis.pool.cluster import ClusterConnectionPool

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
                    node_cache = NodeTrackingCache(
                        cache=self._cache, max_idle_seconds=self._max_idle_seconds
                    )
                    await tg.start(node_cache.run, node.connection_pool)
                    assert node_cache._connection
                    self.node_caches[node_cache._connection.location] = node_cache
                task_status.started()
