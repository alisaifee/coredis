



=============================================
Commands routed to all nodes in the cluster
=============================================



Server commands
  :meth:`coredis.RedisCluster.acl_deluser`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.acl_load`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.acl_save`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.acl_setuser`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.config_resetstat`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.config_set`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.memory_purge`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.module_load`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.module_loadex`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.module_unload`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.save`
      The result is the response from any node if all responses are consistent


Cluster commands
  :meth:`coredis.RedisCluster.cluster_saveconfig`
      The result is ``True`` if all nodes responded ``True``


Connection commands
  :meth:`coredis.RedisCluster.echo`
      The result is the response from any node if all responses are consistent


Pubsub commands
  :meth:`coredis.RedisCluster.pubsub_channels`
      The result is the union of the results
  :meth:`coredis.RedisCluster.pubsub_numsub`
      The result is the merged mapping
  :meth:`coredis.RedisCluster.pubsub_shardchannels`
      The result is the union of the results
  :meth:`coredis.RedisCluster.pubsub_shardnumsub`
      The result is the merged mapping


Scripting commands
  :meth:`coredis.RedisCluster.script_flush`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.script_load`
      The result is the response from any node if all responses are consistent



=================================================
Commands routed to all primaries in the cluster
=================================================



Server commands
  :meth:`coredis.RedisCluster.flushall`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.flushdb`
      The result is the response from any node if all responses are consistent


Scripting commands
  :meth:`coredis.RedisCluster.function_delete`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.function_flush`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.function_kill`
      The result is the first response that is not an error
  :meth:`coredis.RedisCluster.function_load`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.function_restore`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.RedisCluster.script_exists`
      The result is the logical AND of all responses
  :meth:`coredis.RedisCluster.script_kill`
      The result is the first response that is not an error


Generic commands
  :meth:`coredis.RedisCluster.keys`
      The result is the union of the results


Connection commands
  :meth:`coredis.RedisCluster.ping`
      The result is the response from any node if all responses are consistent


Search commands
  :meth:`coredis.modules.Search.config_set`
      The result is the response from any node if all responses are consistent
  :meth:`coredis.modules.Search.list`
      The result is the union of the results


Timeseries commands
  :meth:`coredis.modules.TimeSeries.mget`
      The result is the merged mapping
  :meth:`coredis.modules.TimeSeries.mrange`
      The result is the merged mapping
  :meth:`coredis.modules.TimeSeries.mrevrange`
      The result is the merged mapping
  :meth:`coredis.modules.TimeSeries.queryindex`
      The result is the union of the results



==============================================================
Commands routed to the nodes serving the keys in the command
==============================================================



Generic commands
  :meth:`coredis.RedisCluster.delete`
      The result is the sum of results
  :meth:`coredis.RedisCluster.exists`
      The result is the sum of results
  :meth:`coredis.RedisCluster.touch`
      The result is the sum of results
  :meth:`coredis.RedisCluster.unlink`
      The result is the sum of results


String commands
  :meth:`coredis.RedisCluster.mget`
      The result is the concatenations of the results
  :meth:`coredis.RedisCluster.mset`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.msetex`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.msetnx`
      The result is ``True`` if all nodes responded ``True``


Json commands
  :meth:`coredis.modules.Json.mget`
      The result is the concatenations of the results
  :meth:`coredis.modules.Json.mset`
      The result is ``True`` if all nodes responded ``True``



=================================================
Commands routed to a random node in the cluster
=================================================



Server commands
  :meth:`coredis.RedisCluster.acl_cat`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_dryrun`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_genpass`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_getuser`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_list`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_users`
      The result is the response from the node
  :meth:`coredis.RedisCluster.acl_whoami`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_count`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_docs`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_getkeys`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_getkeysandflags`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_info`
      The result is the response from the node
  :meth:`coredis.RedisCluster.command_list`
      The result is the response from the node
  :meth:`coredis.RedisCluster.info`
      The result is the response from the node
  :meth:`coredis.RedisCluster.module_list`
      The result is the response from the node


Cluster commands
  :meth:`coredis.RedisCluster.cluster_count_failure_reports`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_info`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_keyslot`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_meet`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_nodes`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_replicas`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_shards`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_slaves`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_slots`
      The result is the response from the node


Scripting commands
  :meth:`coredis.RedisCluster.function_dump`
      The result is the response from the node
  :meth:`coredis.RedisCluster.function_list`
      The result is the response from the node
  :meth:`coredis.RedisCluster.function_stats`
      The result is the response from the node


Pubsub commands
  :meth:`coredis.RedisCluster.publish`
      The result is the response from the node


Generic commands
  :meth:`coredis.RedisCluster.randomkey`
      The result is the response from the node


Search commands
  :meth:`coredis.modules.Search.config_get`
      The result is the response from the node



=============================================================================
Commands routed to node(s) based on the slot id(s) in the command arguments
=============================================================================



Cluster commands
  :meth:`coredis.RedisCluster.cluster_countkeysinslot`
      The result is the response from the node
  :meth:`coredis.RedisCluster.cluster_delslots`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.cluster_delslotsrange`
      The result is ``True`` if all nodes responded ``True``
  :meth:`coredis.RedisCluster.cluster_getkeysinslot`
      The result is the response from the node


    