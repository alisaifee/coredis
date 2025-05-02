Utility Classes
---------------

Enums
^^^^^

.. autoclass:: coredis.PureToken
   :no-inherited-members:
   :show-inheritance:

Monitor
^^^^^^^
.. autoclass:: coredis.commands.Monitor
   :class-doc-from: both

Retries
^^^^^^^
:mod:`coredis.retry`

Utilities for managing errors that can be recovered from by providing retry policies.

.. autoclass:: coredis.retry.ConstantRetryPolicy
.. autoclass:: coredis.retry.ExponentialBackoffRetryPolicy
.. autoclass:: coredis.retry.CompositeRetryPolicy
.. autofunction:: coredis.retry.retryable

All retry policies need to derive from :class:`coredis.retry.RetryPolicy`

.. autoclass:: coredis.retry.RetryPolicy

