Encoding/Decoding
-----------------

Constructor parameters :paramref:`~coredis.Redis.encoding` and :paramref:`~coredis.Redis.decode_responses`
are used to control request encoding and response decoding.

``encoding`` is used for specifying which codec you want requests values to be encoded with.
``decode_responses`` is used for tell the client whether responses should be decoded.

If ``decode_responses`` is set to ``True`` and no encoding is specified, the client
will use ``utf-8`` by default.


API Responses
^^^^^^^^^^^^^
**coredis** will not decode any simple or bulk string responses from the server
unless the :paramref:`decode_responses` was ``True``.

.. note:: In certain cases (exclusively for utility commands such as :meth:`coredis.Redis.info`)
   inspecting the response string is necessary to destructure it into a usable shape
   and the response from the API will always be decoded to unicode strings.
