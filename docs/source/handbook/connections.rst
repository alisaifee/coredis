Connections
^^^^^^^^^^^

coredis ships with three types of connections.

- The default, :class:`coredis.connection.Connection`, is a normal TCP socket based connection.

- :class:`~coredis.connection.UnixDomainSocketConnection` allows
  for clients running on the same device as the server to connect via a unix domain socket.
  To use a :class:`~coredis.connection.UnixDomainSocketConnection` connection,
  simply pass the :paramref:`~coredis.Redis.unix_socket_path` argument,
  which is a string to the unix domain socket file.

  Additionally, make sure the parameter is defined in your redis.conf file. It's
  commented out by default.

  .. code-block:: python

      r = coredis.Redis(unix_socket_path='/tmp/redis.sock')

- :class:`~coredis.connection.ClusterConnection` connection which is essentially
  just :class:`~coredis.connection.Connection` with the exception of ensuring appropriate
  ``READONLY`` handling is set if configured (:paramref:`coredis.RedisCluster.readonly`)


Custom connection classes
-------------------------
You can create your own connection subclasses by deriving from
:class:`coredis.connection.BaseConnection` as well. This may be useful if
you want to control the socket behavior within an async framework. To
instantiate a client class using your own connection, you need to create
a connection pool, passing your class to the connection_class argument.
Other keyword parameters you pass to the pool will be passed to the class
specified during initialization.

.. code-block:: python

    pool = coredis.ConnectionPool(connection_class=YourConnectionClass,
                                    your_arg='...', ...)


