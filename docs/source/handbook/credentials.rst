Credential Providers
--------------------
.. versionadded:: 4.18.0

In addition to supplying a static username and/or password, **coredis** allows a credential provider
to be specified. The **coredis** clients accept any credential provider implementing the
:class:`~coredis.credentials.AbstractCredentialProvider` interface. This allows clients to access
credentials stored in external credential providers such as AWS IAM.

Using the Credential Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**coredis** comes with a basic implementation of the :class:`coredis.credentials.AbstractCredentialProvider`
interface. This is :class:`~coredis.credentials.UserPassCredentialProvider`. It allows a user
to specify a username and/or password that will be used to authenticate with a redis instance.
When the :meth:`~coredis.credentials.AbstractCredentialProvider.get_credentials` is called, the
username and/or password that was set will be returned whenever **coredis** needs the credentials
in order to authenticate with the redis or other server.


For example::


    import asyncio
    import coredis
    from coredis.credentials import UserPassCredentialProvider

    provider = UserPassCredentialProvider(username="default", password="abc123")
    client = coredis.Redis(credential_provider=provider)

    # or in cluster mode
    # client = coredis.RedisCluster("localhost", 7000, credential_provider=provider)


More complicated credential providers are also possible. For example, this pattern could be used
to use IAM credentials to authenticate with an AWS Elasticache cluster or any other credential
manager that requires programmatic access.

An example of this is described in :ref:`recipes/credentials:credential providers`.
