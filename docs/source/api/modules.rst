Redis Modules
-------------

:mod:`coredis.modules`

Redis module commands in :mod:`coredis` are exposed under properties
of the :class:`~coredis.Redis` or :class:`~coredis.RedisCluster` clients
such as :attr:`~coredis.Redis.json`, :attr:`~coredis.Redis.bf`. These properties
in turn return instances of the module command group containers which are bound
to the client.

The module commands can also be accessed by instantiating the module command group classes
(listed in the sections below) directly.

To access the :class:`~coredis.modules.Json` command group from the :class:`coredis.modules.RedisJSON` module for example::

    import coredis

    client = coredis.Redis()

    # through the client
    await client.json.get("key", "$")
    # or directly
    json = coredis.modules.Json(client)
    await json.get("key", "$")

RedisJSON
^^^^^^^^^
.. autoclass:: coredis.modules.Json

RedisBloom
^^^^^^^^^^

===========
BloomFilter
===========
.. autoclass:: coredis.modules.BloomFilter

============
CuckooFilter
============
.. autoclass:: coredis.modules.CuckooFilter

================
Count Min Sketch
================
.. autoclass:: coredis.modules.CountMinSketch

=======
TDigest
=======
.. autoclass:: coredis.modules.TDigest

====
TopK
====
.. autoclass:: coredis.modules.TopK

RedisSearch
^^^^^^^^^^^

====================
Search & Aggregation
====================

.. autoclass:: coredis.modules.Search

.. autoclass:: coredis.modules.search.Field
.. autoclass:: coredis.modules.search.Filter
.. autoclass:: coredis.modules.search.Group
.. autoclass:: coredis.modules.search.Reduce
.. autoclass:: coredis.modules.search.Apply
.. autoclass:: coredis.modules.search.RRFCombine
.. autoclass:: coredis.modules.search.LinearCombine


============
Autocomplete
============
.. autoclass:: coredis.modules.Autocomplete

TimeSeries
^^^^^^^^^^
.. autoclass:: coredis.modules.TimeSeries
