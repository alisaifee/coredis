Caching
-------
:mod:`coredis.cache`

Built in caches
^^^^^^^^^^^^^^^

.. autoclass:: coredis.cache.TrackingCache
   :class-doc-from: both

.. autoclass:: coredis.cache.NodeTrackingCache
   :class-doc-from: both

.. autoclass:: coredis.cache.ClusterTrackingCache
   :class-doc-from: both

Implementing a custom cache
^^^^^^^^^^^^^^^^^^^^^^^^^^^
All caches accepted by :class:`~coredis.Redis` or :class:`~coredis.RedisCluster`
must implement :class:`~coredis.cache.AbstractCache`

.. autoclass:: coredis.cache.AbstractCache
.. autoclass:: coredis.cache.CacheStats

