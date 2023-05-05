Clients
-------

Redis
^^^^^
.. autoclass:: coredis.Redis
   :class-doc-from: both

Cluster
^^^^^^^
.. autoclass:: coredis.RedisCluster
   :class-doc-from: both


Sentinel
^^^^^^^^
:mod:`coredis.sentinel`

.. autoclass:: coredis.sentinel.Sentinel
   :class-doc-from: both

KeyDB
^^^^^
.. autoclass:: coredis.KeyDB
   :no-inherited-members:

   .. automethod:: bitop
   .. automethod:: cron
   .. automethod:: expiremember
   .. automethod:: expirememberat
   .. automethod:: pexpirememberat
   .. automethod:: hrename
   .. automethod:: mexists
   .. automethod:: ttl
   .. automethod:: pttl
   .. automethod:: object_lastmodified

.. autoclass:: coredis.KeyDBCluster
   :no-inherited-members:

   .. automethod:: expiremember
   .. automethod:: expirememberat
   .. automethod:: pexpirememberat
   .. automethod:: hrename
   .. automethod:: mexists
   .. automethod:: ttl
   .. automethod:: pttl
   .. automethod:: object_lastmodified

