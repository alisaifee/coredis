Project History
===============

coredis is a fork of the excellent `aredis <https://github.com/NoneGG/aredis>`_ client
developed and maintained by `Jason Chen <https://github.com/NoneGG>`_.

**aredis** already had support for cluster & sentinel and was one of the best
performing async python clients. Since it had become unmaintained as of October 2020
The initial intention of the fork was add python 3.10 compatibility and
`coredis 2.x <https://github.com/alisaifee/coredis/tree/2.x>`__ is drop-in backward compatible with **aredis** and adds support up to python 3.10.


Divergence from aredis & redis-py
---------------------------------

Versions :ref:`release_notes:v3.0.0` and above no longer maintain compatibility with **aredis**. Since **aredis** mostly mirrored the :pypi:`redis`
client, this inherently means that **coredis** diverges from both, most notable (at the time of writing) in the following general categories:

- API signatures for redis commands that take variable length arguments are only variadic if they are optional, for example :meth:`coredis.Redis.delete`
  takes a variable number of keys however, they are not optional thus the signature expects a collection of keys as the only positional argument

  .. automethod:: coredis.Redis.delete
     :noindex:
- Redis commands that accept tokens for controlling behavior now use :class:`~coredis.tokens.PureToken` and the coredis methods mirroring the commands
  use :class:`~typing.Literal` to document the acceptable values. An example of this is :meth:`coredis.Redis.expire`.

  .. automethod:: coredis.Redis.expire
     :noindex:

Default RESP3
-------------

**coredis** version ``3.x`` supported selecting the protocol version to use when parsing responses
from the redis server and defaulted to the legacy ``RESP`` protocol. Since **coredis** has dropped
support for redis server versions below ``6.0`` the default protocol version is now :term:`RESP3`.


Parsers
-------
**coredis** versions ``2.x`` and ``3.x`` would default to a :pypi:`hiredis` based parser if the
dependency was available. This behavior was inherited from **aredis** which inherited it from
:pypi:`redis`. Though :pypi:`hiredis` does provide a significant speedup when parsing responses
for large nested bulk responses, **coredis** has made sufficient improvements in performance
including using :pypi:`mypyc` to provide a native speedup when possible and therefore
no longer supports multiple parser implementations and always uses :class:`coredis.parser.Parser`.
