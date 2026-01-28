Utility Classes
---------------

Enums
^^^^^

.. autoclass:: coredis.PureToken
   :no-inherited-members:
   :show-inheritance:

Retrying
^^^^^^^^
:mod:`coredis.retry`

Utilities for managing errors that can be recovered from by providing retry policies.

.. autoclass:: coredis.retry.ConstantRetryPolicy
   :class-doc-from: both
.. autoclass:: coredis.retry.ExponentialBackoffRetryPolicy
   :class-doc-from: both
.. autoclass:: coredis.retry.CompositeRetryPolicy
   :class-doc-from: both
.. autofunction:: coredis.retry.retryable

All retry policies need to derive from :class:`coredis.retry.RetryPolicy`

.. autoclass:: coredis.retry.RetryPolicy
