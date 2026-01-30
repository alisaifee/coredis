Credential Providers
--------------------
:mod:`coredis.recipes.credentials`

Elasticache IAM Credential Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The implementation is based on `the Elasticache IAM provider described in redis-py's docs <https://redis.readthedocs.io/en/stable/examples/connection_examples.html#Connecting-to-a-redis-instance-with-ElastiCache-IAM-credential-provider.>`__

The :class:`~coredis.recipes.ElastiCacheIAMProvider` implements the
:class:`~coredis.credentials.AbstractCredentialProvider` interface.
It uses :pypi:`aiobotocore` to generate a short-lived authentication token
which can be used to authenticate with an IAM enabled Elasticache cluster.
The token is cached for its lifetime of 15 minutes to reduce the number
of unnecessary requests.

See https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/auth-iam.html for more details
on using IAM to authenticate with Elasticache.

.. autoclass:: coredis.recipes.ElastiCacheIAMProvider
   :class-doc-from: both
   :no-index:
