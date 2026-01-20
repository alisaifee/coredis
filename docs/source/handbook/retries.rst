Retrying
--------

The :ref:`api/utilities:retrying` subsystem in coredis provides flexible support for retrying operations
(including Redis commands) when certain exceptions occur. It’s designed to make transient failures easier to
handle in robust applications.

coredis clients can be instantiated with a retry policy using the :paramref:`~coredis.Redis.retry_policy`
argument which will be used whenever any redis command is called.


Retry Policies
^^^^^^^^^^^^^^
There are two primary built-in retry policies:

- :class:`~coredis.retry.ConstantRetryPolicy`
- :class:`~coredis.retry.ExponentialBackoffRetryPolicy`

For example to ensure any connection errors are automatically retried twice with a delay of 1 second
between attempts::

  import coredis
  client = coredis.Redis(
    retry_policy=coredis.retry.ConstantRetryPolicy(
        (coredis.exceptions.ConnectionError,),
        retries=2,
        delay=1.0
    )
  )


.. important::

   The ``retries`` parameter specifies the **number of retry attempts after
   the initial attempt**. For example, ``retries=2`` results in up to
   **3 total attempts**.

You can implement your own retry policy by subclassing the abstract :class:`~coredis.retry.RetryPolicy`.
As an example here's a random retry policy::

  import random
  from anyio import sleep

  class RandomRetryPolicy(coredis.retry.RetryPolicy):
      def __init__(
          self,
          retryable_exceptions: tuple[type[BaseException], ...],
          retries: int,
          max_delay: float
      ) -> None:
          super().__init__(retryable_exceptions=retryable_exceptions, retries=retries)
          self.max_delay = max_delay

      async def delay(self, attempt: int) -> None:
          await sleep(random.random() * self.max_delay)


A built in :class:`~coredis.retry.CompositeRetryPolicy` can be used to construct a comprehensive
retry policy that handles different classes of errors with different policies::

  retry_policy = coredis.retry.CompositeRetryPolicy(
      coredis.retry.ConstantRetryPolicy(
        (coredis.exceptions.RedisError,), retries=2, delay=1
      ),
      coredis.retry.ExponentialBackoffRetryPolicy(
        (coredis.exceptions.ConnectionError,), retries=5, initial_delay=0.1
      ), # etc...
  )


Explicitly retry a request
^^^^^^^^^^^^^^^^^^^^^^^^^^

There may be scenarios where a global retry policy that applies to every command issued
through the client is not desired. Responses from any redis command can also be chained
with the :meth:`~coredis.command.CommandRequest.retry` method.

For example::

    async with coredis.Redis() as client:
        await client.lpush(
            "mylist", [1,2,3]
        ).retry(coredis.retry.ConstantRetryPolicy(
            (coredis.exceptions.RedisError,), 2, 1)
        )



The above example can be made more interesting by considering the scenario where
you are trying to push to a key which might not be a list::

    async with coredis.Redis() as client:
        await client.lpush("mylist", [1,2,3]).retry(
            coredis.retry.ConstantRetryPolicy((coredis.exceptions.WrongTypeError,), 1, 1),
            failure_hook=lambda err: client.delete(["mylist"])
        )

You can achieve the same functionality as the :meth:`~coredis.command.CommandRequest.retry` chain call
by using the :meth:`~coredis.retry.RetryPolicy.call_with_retries` method directly::

    async with coredis.Redis() as client:
        policy = coredis.retry.ConstantRetryPolicy((coredis.exceptions.WrongTypeError,), 1, 1)
        await policy.call_with_retries(
            lambda: client.lpush("mylist", [1, 2, 3]),
            failure_hook=lambda err: client.delete(["mylist"]),
        )


Retry decorator
^^^^^^^^^^^^^^^

The :func:`~coredis.retry.retryable` decorator can be used to apply a retry
policy to any coroutine::

    @coredis.retry.retryable(
        coredis.retry.ConstantRetryPolicy(
            (coredis.exceptions.ConnectionError,), retries=2, delay=1
        )
    )
    async def push_values(client):
        await client.lpush("mylist", [1, 2, 3])
