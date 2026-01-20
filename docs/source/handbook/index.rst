Handbook
========

.. toctree::
    :hidden:
    :maxdepth: 3

    cluster
    sentinel
    credentials
    pubsub
    streams
    caching
    pipelines
    locks
    retries
    noreply
    scripting
    modules
    connections
    encoding
    optimization
    typing
    response
    development

This section contains details on high level concepts
of redis that are supported by **coredis**.

Deployment Topologies
---------------------

- :ref:`handbook/cluster:redis cluster`
- :ref:`handbook/sentinel:sentinel support`

Authentication
--------------
- :ref:`handbook/credentials:credential providers`

Event Driven Architectures
--------------------------
- :ref:`handbook/pubsub:pubsub`
- :ref:`handbook/streams:streams`

Server side scripting
---------------------
- :ref:`handbook/scripting:lua scripts`
- :ref:`handbook/scripting:library functions`

Modules
-------
- :ref:`handbook/modules:redisjson`
- :ref:`handbook/modules:redisbloom`
- :ref:`handbook/modules:redisearch`
- :ref:`handbook/modules:redistimeseries`

Performance
-----------
- :ref:`handbook/connections:connection pools`
- :ref:`handbook/caching:caching`
- :ref:`handbook/pipelines:pipelines`
- :ref:`handbook/noreply:no reply mode`
- :ref:`handbook/optimization:optimized mode`

Reliability
-----------
- :ref:`handbook/pipelines:atomicity & transactions`
- :ref:`handbook/cluster:replication`
- :ref:`handbook/locks:locks`
- :ref:`handbook/retries:retrying`

Implementation Details
----------------------
- :ref:`handbook/connections:connection types`
- :ref:`handbook/development:development`
- :ref:`handbook/response:resp`
- :ref:`handbook/response:api responses`
- :ref:`handbook/typing:typing`
