Redis Response
--------------

As of redis `6.0.0` clients can use the :term:`RESP3` protocol which provides support for a much
larger set of types (which reduces the need for clients to "guess" what the type of a command's response should be).

**coredis** versions up to ``5.x`` provided backward compatibility for ``RESP``, however this is no
longer supported and :term:`RESP3` is always used.
