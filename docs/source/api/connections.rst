Connections & Connection Pooling
--------------------------------

Connection Pools
^^^^^^^^^^^^^^^^
.. currentmodule:: coredis

:mod:`coredis`

.. autoclass:: coredis.ConnectionPool
   :class-doc-from: both

.. autoclass:: coredis.ClusterConnectionPool
   :class-doc-from: both
   :show-inheritance:

.. autoclass:: coredis.sentinel.SentinelConnectionPool
   :class-doc-from: both
   :show-inheritance:

Connection Classes
^^^^^^^^^^^^^^^^^^
:mod:`coredis`

.. autoclass:: coredis.Connection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.UnixDomainSocketConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.ClusterConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.sentinel.SentinelManagedConnection
   :show-inheritance:
   :class-doc-from: both

All connection classes derive from the same base-class:

.. autoclass:: coredis.BaseConnection
   :show-inheritance:
   :class-doc-from: both
