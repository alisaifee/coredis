Caching
-------
.. versionadded:: 3.9.0

**coredis** allows using a client side cache to eliminate readonly requests
to the redis server when a response is available in the local cache.

If enabled the clients will cache the responses for readonly commands by
`key`/`command`/`arguments`. The **coredis** clients accept any cache
implementing the :class:`~coredis.cache.AbstractCache` interface and will:

1. Cache responses for readonly commands acting on single keys (the docstring for the method
   will indicate whether it supports caching, for example :meth:`~coredis.Redis.get`).
2. Invalidate a key if a non readonly command is called on it
3. If the cache returns a :data:`~coredis.cache.AbstractCache.confidence` value lower
   than ``100`` the client will distrust the cached response ``(100-$confidence)%`` of the time and validate
   the cached response against the actual response from the server. The result of the comparison
   will be provided to the cache through a call to :meth:`~coredis.cache.AbstractCache.feedback` and
   it is up to the cache implementation to decide what to do with this feedback.

Tracking Cache
^^^^^^^^^^^^^^
**coredis** currently ships with an implementation of :term:`Server assisted client side caching`
that can be used with both the standalone :class:`~coredis.Redis` client or the :class:`~coredis.RedisCluster`
client.  This implementation enables client tracking for the client so that the redis server
remembers which keys the client has requested and if the key is modified (whether mutated, deleted or expired)
sends a notification that the cache subscribes to to invalidate the cache.

Specifically :class:`~coredis.cache.NodeTrackingCache` contains the implementation for a
single node and :class:`~coredis.cache.ClusterTrackingCache` tracks all the nodes in a redis cluster.

Users don't need to worry about how these implementations work, and instead can focus on implementing
a :class:`~coredis.cache.AbstractCache` instance or using the provided implementation, :class:`~coredis.cache.LRUCache`.

For example::

    import asyncio
    import coredis
    from coredis.cache import LRUCache

    cached_client = coredis.Redis(cache=LRUCache())
    regular_client = coredis.Redis()

    # or in cluster mode
    # cached_client = coredis.RedisCluster("localhost", 7000, cache=LRUCache())
    # regular_client = coredis.RedisCluster("localhost", 7000)

    async def test():
        async with cached_client, regular_client:
            assert not await cached_client.get("fubar") # None response cached
            await regular_client.set("fubar", "bar") # <- triggers a push message to cached_client
            await asyncio.sleep(0.01)
            assert b"bar" == await cached_client.get("fubar") # Cache should be invalidated
            assert b"bar" == await cached_client.get("fubar") # Fetched from local cache
            await cached_client.delete(["fubar"]) # Invalidates local cache immediately
            assert not await cached_client.get("fubar")

    asyncio.run(test())

:class:`~coredis.cache.LRUCache` exposes a few configuration options to fine tune
the cache. Specifically the following constructor arguments might be of interest:

:paramref:`~coredis.cache.LRUCache.max_keys`
    Maximum number of redis keys to track. This does not map directly to the number of
    cached entries as the cache maintains a per key, per command, per argument cache.

:paramref:`~coredis.cache.LRUCache.confidence`
    Confidence % in the cache. The client will sample cached values based on the confidence
    and if the cached value is not the same as the actual response from the server
    the actual value will be returned and the tainted key invalidated.

:paramref:`~coredis.cache.LRUCache.dynamic_confidence`
    If set to ``True`` the cache will adjust it's confidence based on sampled (sampling depends
    on the initial confidence value itself) validations.
