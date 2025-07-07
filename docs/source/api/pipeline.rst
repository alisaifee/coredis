Pipeline Support
----------------
:mod:`coredis.pipeline`

:term:`Pipelining` and :term:`Transactions` are exposed by the following classes
that are returned by :meth:`coredis.Redis.pipeline` and :meth:`coredis.RedisCluster.pipeline`.
For examples refer to :ref:`handbook/pipelines:pipelines`.

.. autoclass:: coredis.pipeline.Pipeline
   :class-doc-from: both

.. autoclass:: coredis.pipeline.ClusterPipeline
   :class-doc-from: both
