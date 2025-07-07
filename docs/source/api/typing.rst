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
to the response from redis. The responses are normalized across :term:`RESP` versions ``2`` and ``3``
to maintain a consistent signature, i.e. :term:`RESP` responses are reshaped into their :term:`RESP3`
counterparts, for example:

- If redis returns a map for a command in :term:`RESP3`, coredis will ensure an identically
  shaped dictionary is returned for :term:`RESP` as well. (E.g. :meth:`~coredis.Redis.hgetall`)
- If redis returns a double value for a command in :term:`RESP3`, coredis will parse the string
  value as a python float for :term:`RESP`. (E.g. :meth:`~coredis.Redis.zscore`)
- If redis returns a set for a command in :term:`RESP3`, coredis will convert the redis array
  to a python set (E.g. :meth:`~coredis.Redis.smembers`)

In certain cases these are "lightly" typed using :class:`~typing.NamedTuple`
or :class:`~typing.TypedDict` for ease of documentation and in the case of "tuples"
returned by redis - to avoid errors in indexing.

:mod:`coredis.response.types`

.. automodule:: coredis.response.types
   :no-inherited-members:
   :show-inheritance:

:mod:`coredis.modules.response.types`

.. automodule:: coredis.modules.response.types
   :no-inherited-members:
   :show-inheritance:

