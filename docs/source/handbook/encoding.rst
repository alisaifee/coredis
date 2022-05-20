Encoding/Decoding
-----------------

Constructor parameters :paramref:`~coredis.Redis.encoding` and :paramref:`~coredis.Redis.decode_responses`
are used to control response decoding.

``encoding`` is used for specifying with which encoding you want responses to be decoded.
``decode_responses`` is used for tell the client whether responses should be decoded.

If ``decode_responses`` is set to ``True`` and no encoding is specified, the client
will use ``utf-8`` by default.


API Responses
^^^^^^^^^^^^^
**coredis** tries to avoid decoding any data parsed from the server response
unless the :paramref:``decode_responses`` was set. However, in certain cases
(almost exclusively for server commands such as :meth:`coredis.Redis.info`
inspecting the response string is necessary to destructure it into a usable shape.
