Caching
-------
:mod:`coredis.patterns.cache`

Built in caches
^^^^^^^^^^^^^^^

.. autoclass:: coredis.patterns.cache.LRUCache
   :class-doc-from: both

Implementing a custom cache
^^^^^^^^^^^^^^^^^^^^^^^^^^^
All caches accepted by :class:`~coredis.Redis` or :class:`~coredis.RedisCluster`
must implement :class:`~coredis.patterns.cache.AbstractCache`

.. autoclass:: coredis.patterns.cache.AbstractCache
.. autoclass:: coredis.patterns.cache.CacheStats

Internal cache wrappers
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: coredis.patterns.cache.NodeTrackingCache
   :class-doc-from: both

.. autoclass:: coredis.patterns.cache.ClusterTrackingCache
   :class-doc-from: both
