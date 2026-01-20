Typing
------

:mod:`coredis.typing`

Input types
^^^^^^^^^^^
The API uses the following type aliases to describe the unions of acceptable types
for parameters to redis command wrappers.

.. autodata:: coredis.typing.KeyT
.. autodata:: coredis.typing.ValueT
.. autodata:: coredis.typing.RedisValueT
.. autodata:: coredis.typing.StringT
.. autodata:: coredis.typing.JsonType

For methods that accept non optional variable number of keys or values, coredis does **NOT**
use **positional** or **keyword varargs** and expects a "container" to be passed in for the argument.
Common examples of such APIs are :meth:`~coredis.Redis.delete` and :meth:`~coredis.Redis.exists`.

Instead of accepting :class:`~collections.abc.Iterable`, a union of select containers from the standard
library are accepted via :data:`~coredis.typing.Parameters`.

.. autodata:: coredis.typing.Parameters

Custom types
^^^^^^^^^^^^

.. autoclass:: coredis.typing.Serializable
   :class-doc-from: both
.. autoclass:: coredis.typing.TypeAdapter
   :class-doc-from: both

Redis Response (RESP) descriptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The follow types describe the total representation of parsed responses from the redis
serialization protocol(s) (RESP & RESP3) (See :ref:`handbook/response:redis response` for more details).

In most cases these are not exposed through the client API and are only meant
for internal pre-validation before the parsed response is transformed or narrowed
to the returns documented in the client API at :ref:`api/clients:clients`.

.. type:: coredis.typing.ResponsePrimitive

   Primitives returned by redis

.. type:: coredis.typing.HashableResponseType

   The structure of hashable response types (i.e. those that can
   be members of sets or keys for maps)

.. type:: coredis.typing.ResponseType

   The total structure of any response for any redis command.

Response Types
^^^^^^^^^^^^^^
In most cases the API returns native python types mapped as closely as possible
to the :term:`RESP3` type from redis:

========================= =================
RESP3 Type                Python type
========================= =================
Simple/Bulk String        :class:`bytes` or :class:`str` depending on the configuration of :paramref:`coredis.Redis.decode_responses`
Simple/Bulk Errors        :exc:`~coredis.exceptions.RedisError`
Integers                  :class:`int`
Double                    :class:`float`
Arrays                    :class:`tuple`
Nulls                     :class:`NoneType`
Booleans                  :class:`bool`
Set                       :class:`set`
Map                       :class:`dict`
========================= =================



In certain cases the response is too complex and therefore it is "lightly" typed
using :class:`~typing.NamedTuple` or :class:`~typing.TypedDict` for better documentation
and type safety.

The types below is a complete representation of responses from **coredis** that are not
builtin python types.

:mod:`coredis.response.types`

.. automodule:: coredis.response.types
   :no-inherited-members:
   :show-inheritance:

:mod:`coredis.modules.response.types`

.. automodule:: coredis.modules.response.types
   :no-inherited-members:
   :show-inheritance:
