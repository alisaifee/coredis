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

Versions :ref:`release_notes:v3.0.0` and above no longer maintain compatiblity with **aredis**. Since **aredis** mostly mirrored the :pypi:`redis`
client, this inherently means that **coredis** diverges from both, most notable (at the time of writing) in the following general categories:

- API signatures for redis commands that take variable length arguments are only variadic if they are optional, for example :meth:`coredis.Redis.delete`

  .. automethod:: coredis.Redis.delete
     :noindex:
- Redis commands that accept tokens for controlling behavior now use :class:`coredis.PureToken` and the coredis methods mirroring the commands
  use :class:`Literal` to document the acceptable values. An example of this is :meth:`coredis.Redis.expire`.

  .. automethod:: coredis.Redis.expire
     :noindex:

