Redis Response
--------------

As of redis `6.0.0` clients can use the
:term:`RESP3` protocol which provides support for a much larger set of types (which reduces the need for clients
to "guess" what the type of a command's response should be).
**coredis** provides backward compatibility for ``RESP``
and the structure of responses from coredis is consistent
between :term:`RESP` (``protocol_version=2``) and :term:`RESP3` (``protocol_version=3``) protocols.

To fallback to ``RESP`` the :paramref:`~coredis.Redis.protocol_version` constructor parameter
can be set to ``2``.

.. code-block:: python

    r = coredis.Redis(protocol_version=2)


