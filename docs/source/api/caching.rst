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

Additionally, caches can opt in to additional features by implementing any of the
following protocols:

.. autoclass:: coredis.cache.SupportsClientTracking
.. autoclass:: coredis.cache.SupportsStats
.. autoclass:: coredis.cache.SupportsSampling

.. autoclass:: coredis.cache.CacheStats

