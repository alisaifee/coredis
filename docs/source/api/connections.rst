Connections & Connection Pooling
--------------------------------

Connection Pools
^^^^^^^^^^^^^^^^
.. currentmodule:: coredis.pool

:mod:`coredis.pool`

.. autoclass:: coredis.pool.ConnectionPool
   :class-doc-from: both

.. autoclass:: coredis.pool.ConnectionPoolParams
   :undoc-members:
   :show-inheritance:

.. autoclass:: coredis.pool.ClusterConnectionPool
   :class-doc-from: both
   :show-inheritance:

.. autoclass:: coredis.pool.ClusterConnectionPoolParams
   :undoc-members:
   :show-inheritance:

.. autoclass:: coredis.pool.SentinelConnectionPool
   :class-doc-from: both
   :show-inheritance:

All connection pools derive from the same base-class:

.. autoclass:: coredis.pool.BaseConnectionPool
   :class-doc-from: both

.. autoclass:: coredis.pool.BaseConnectionPoolParams
   :undoc-members:
   :show-inheritance:

Connection Classes
^^^^^^^^^^^^^^^^^^
.. currentmodule:: coredis.connection

:mod:`coredis.connection`

.. autoclass:: coredis.connection.TCPConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.connection.UnixDomainSocketConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.connection.ClusterConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.connection.SentinelManagedConnection
   :show-inheritance:
   :class-doc-from: both

All connection classes derive from the same base-class:

.. autoclass:: coredis.connection.BaseConnection
   :show-inheritance:
   :class-doc-from: both

.. autoclass:: coredis.connection.BaseConnectionParams
   :show-inheritance:
