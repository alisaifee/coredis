Typing
------

:mod:`coredis.typing`

Input types
^^^^^^^^^^^
The API uses the following type aliases to describe the unions of acceptable types
for parameters to redis command wrappers.

.. autodata:: coredis.typing.KeyT
.. autodata:: coredis.typing.ValueT
.. autodata:: coredis.typing.StringT

For methods that accept non optional variable number of keys or values, coredis does **NOT**
use **positional** or **keyword varargs** and expects a "container" to be passed in for the argument.
Common examples of such APIs are :meth:`~coredis.Redis.delete` and :meth:`~coredis.Redis.exists`.

Instead of accepting :class:`~collections.abc.Iterable`, a union of select containers from the standard
library are accepted via :data:`~coredis.typing.Parameters`.

.. autodata:: coredis.typing.Parameters


Redis Response (RESP) descriptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The follow two types describe the total representation of parsed responses from the redis
serialization protocol(s) (RESP & RESP3) (See :ref:`handbook/response:redis response` for more details).

In most cases these are not exposed through the client API and are only meant
for internal pre-validation before the parsed response is transformed or narrowed
to the returns documented in the client API at :ref:`api/clients:clients`.

.. autodata:: coredis.typing.ResponsePrimitive
.. autodata:: coredis.typing.ResponseType


Response Types
^^^^^^^^^^^^^^
In most cases the API returns native python types mapped as closely as possible
to the response from redis. The responses are normalized across RESP versions ``2`` and ``3``
to maintain a consistent signature (Most notable example of this is dictionary

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

