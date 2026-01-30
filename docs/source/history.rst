Project History
===============

coredis is a fork of the excellent `aredis <https://github.com/NoneGG/aredis>`_ client
developed and maintained by `Jason Chen <https://github.com/NoneGG>`_.

:pypi:`aredis` already had support for cluster & sentinel and was one of the best
performing async python clients. Since it had become unmaintained as of **October 2020**
The initial intention of the fork was add python ``3.10`` compatibility and
:ref:`release_notes:v2.0.0` was drop-in backward compatible
with :pypi:`aredis` and addd support up to python ``3.10``.

Version :ref:`release_notes:v6.0.0rc1` was a large scale refactor that replaced all internal async task
management and io with :mod:`anyio` (contributed by `Graeme Holliday <https://github.com/Graeme22>`_) This essentially
enables the library to be used with :mod:`trio`.

Divergence from aredis & redis-py
---------------------------------

Versions :ref:`release_notes:v3.0.0` and above no longer maintain compatibility with :pypi:`aredis`.
Since :pypi:`aredis` mostly mirrored the :pypi:`redis` client, this inherently means that **coredis**
diverges from both, most notable (at the time of writing) in the following general categories:

- API signatures for redis commands that take variable length arguments are only variadic if they are optional,
  for example :meth:`coredis.Redis.delete` takes a variable number of keys however, they are not optional.
  Thus the signature expects a collection of keys as the only positional argument

  .. automethod:: coredis.Redis.delete
     :noindex:
- Redis commands that accept tokens for controlling behavior now use :class:`~coredis.tokens.PureToken` and the coredis
  methods mirroring the commands use :class:`~typing.Literal` to document the acceptable values.
  An example of this is :meth:`coredis.Redis.expire`.

  .. automethod:: coredis.Redis.expire
     :noindex:
- Since :ref:`release_notes:v3.0.0` building an async redis client with strict type hints for all APIs
  has been one of the primary goals of the project. As of :ref:`release_notes:v5.0.0` all clients (including
  pipelines) were correctly statically typed and the use of type stubs was completely eliminated.
- As of :ref:`release_notes:v5.0.0` all core redis command methods exposed by the :ref:`api/clients:clients`
  were changed from coroutines to regular methods that return :class:`~coredis.commands.CommandRequest` objects
  which are of type :class:`~typing.Awaitable`.
- **coredis** takes a significantly different approach to connection pooling than :pypi:`redis` & :mod:`aredis`
  to optimize performance in an async context. In the simplest terms, the connection pool will only grow beyond
  ``1`` connection per target server (cluster connection pools maintain sub pools for every node in the cluster)
  if the connection has been acquired in a blocking request. For all non blocking requests **coredis** will
  pipeline concurrent requests on the same connection which will get resolved asynchronously. This significantly
  reduces the connection cost and inherently improves performance.
- **coredis** almost always tries to provide a simply pythonic ``1:1`` mapping of command parameters from the
  redis specification which means that higher level abstractions are rarely provided. This is signficantly
  different from the approach :pypi:`redis` takes, especially with respect to :ref:`handbook/modules:redis modules`
  support. See the RediSearch :meth:`~coredis.modules.Search.search` method as an example. For complex patterns such as :term:`Pub/Sub`,
  :term:`Pipelining` and :term:`Streams` however, light abstractions are provided. See :ref:`handbook/index:handbook` for more details.



Default RESP3
-------------

:ref:`release_notes:v3.0.0` supported selecting the protocol version to use when parsing responses
from the redis server and defaulted to the legacy ``RESP`` protocol. Since **coredis** has dropped
support for redis server versions below ``6.0`` the default protocol version is now :term:`RESP3`.

Parsers
-------
**coredis** versions ``2.x`` and ``3.x`` would default to a :pypi:`hiredis` based parser if the
dependency was available. This behavior was inherited from :pypi:`aredis` which inherited it from
:pypi:`redis`. Though :pypi:`hiredis` does provide a significant speedup when parsing responses
for large nested bulk responses, **coredis** has made sufficient improvements in performance
including using :pypi:`mypyc` to provide a native speedup when possible and therefore
no longer supports multiple parser implementations and always uses :class:`coredis.parser.Parser`.
