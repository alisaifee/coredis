Optimization
------------

Optimized mode
^^^^^^^^^^^^^^
**coredis** can be run in optimized mode to boost performance. In this mode the following behaviors are disabled:

- Runtime validation of parameter combinations for redis
  commands that can take various combinations of inputs (examples: :meth:`~coredis.Redis.set` or :meth:`~coredis.Redis.xadd`)
- Validation of correct use of iterables as parameters
- Compatibility checks by Redis server version

Optimized mode can be enabled in any of the following ways:

- Set the environment variable :envvar:`COREDIS_OPTIMIZED` to ``true``
- Run Python in optimized mode with :option:`-O` or setting :envvar:`PYTHONOPTIMIZE`
- Explicitly with ``coredis.Config.optimized=True``
