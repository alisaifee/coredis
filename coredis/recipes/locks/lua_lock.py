from __future__ import annotations

import contextvars
import importlib.resources
import math
import time
import uuid
import warnings
from types import TracebackType
from typing import cast

from anyio import sleep

from coredis.client import Redis, RedisCluster
from coredis.commands import Script
from coredis.exceptions import (
    LockAcquisitionError,
    LockError,
    LockExtensionError,
    LockReleaseError,
    ReplicationError,
)
from coredis.tokens import PureToken
from coredis.typing import AnyStr, Generic, KeyT, StringT

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    EXTEND_SCRIPT = Script(script=importlib.resources.read_text(__package__, "extend.lua"))
    RELEASE_SCRIPT = Script(script=importlib.resources.read_text(__package__, "release.lua"))


class LuaLock(Generic[AnyStr]):
    """
    A shared, distributed Lock using LUA scripts.

    The lock can be used with both :class:`coredis.Redis`
    and :class:`coredis.RedisCluster` either explicitly or as an async context
    manager::


        import asyncio
        import coredis
        from coredis.exceptions import LockError
        from coredis.recipes.locks import LuaLock

        async def test():
            client = coredis.Redis()
            async with LuaLock(client, "mylock", timeout=1.0):
                # do stuff
                await asyncio.sleep(0.5)
                # lock is implictly released when the context manager exits
            try:
                async with LuaLock(client, "mylock", timeout=1.0):
                    # do stuff that takes too long
                    await asyncio.sleep(1)
                    # lock will raise upon exiting the context manager
            except LockError as err:
                # roll back stuff
                print(f"Expected error: {err}")
            lock = LuaLock(client, "mylock", timeout=1.0)
            await lock.acquire()
            # do stuff
            await asyncio.sleep(0.5)
            # do more stuff
            await lock.extend(1.0)
            await lock.release()

        asyncio.run(test())
    """

    @classmethod
    @RELEASE_SCRIPT.wraps(client_arg="client")
    async def lua_release(  # type: ignore[empty-body]
        cls,
        client: Redis[AnyStr] | RedisCluster[AnyStr],
        name: KeyT,
        expected_token: StringT,
    ) -> int: ...

    @classmethod
    @EXTEND_SCRIPT.wraps(client_arg="client")
    async def lua_extend(  # type: ignore[empty-body]
        cls,
        client: Redis[AnyStr] | RedisCluster[AnyStr],
        name: KeyT,
        expected_token: StringT,
        additional_time: int,
    ) -> int: ...

    local: contextvars.ContextVar[StringT | None]

    def __init__(
        self,
        client: Redis[AnyStr] | RedisCluster[AnyStr],
        name: StringT,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ):
        """
        :param timeout: indicates a maximum life for the lock.
         By default, it will remain locked until :meth:`release` is called.
         ``timeout`` can be specified as a float or integer, both representing
         the number of seconds to wait.

        :param sleep: indicates the amount of time to sleep per loop iteration
         when the lock is in blocking mode and another client is currently
         holding the lock.

        :param blocking: indicates whether calling :meth:`acquire` should block until
         the lock has been acquired or to fail immediately, causing :meth:`acquire`
         to return ``False`` and the lock not being acquired. Defaults to ``True``.

        :param blocking_timeout: indicates the maximum amount of time in seconds to
         spend trying to acquire the lock. A value of ``None`` indicates
         continue trying forever. ``blocking_timeout`` can be specified as a
         :class:`float` or :class:`int`, both representing the number of seconds to wait.
        """
        self.client: Redis[AnyStr] | RedisCluster[AnyStr] = client
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.local = contextvars.ContextVar[StringT | None]("token", default=None)

        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    async def __aenter__(
        self,
    ) -> LuaLock[AnyStr]:
        if await self.acquire():
            return self
        raise LockAcquisitionError("Could not acquire lock")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.release()

    async def acquire(
        self,
    ) -> bool:
        """
        Use :rediscommand:`SET` with the ``NX`` option
        to acquire a lock. If the lock is being used with a cluster client
        the :meth:`coredis.RedisCluster.ensure_replication` context manager
        will be used to ensure that the command was replicated to atleast
        half the replicas of the shard where the lock would be acquired.

        :raises: :exc:`~coredis.exceptions.LockError`
        """
        token = uuid.uuid1().hex
        blocking = self.blocking
        blocking_timeout = self.blocking_timeout
        stop_trying_at = None

        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout

        while True:
            if await self.__acquire(token, stop_trying_at):
                self.local.set(token)

                return True

            if not blocking:
                return False

            if stop_trying_at is not None and time.time() > stop_trying_at:
                return False
            await sleep(self.sleep)

    async def release(self) -> None:
        """
        Releases the already acquired lock

        :raises: :exc:`~coredis.exceptions.LockReleaseError`
        """
        expected_token = self.local.get()

        if expected_token is None:
            raise LockReleaseError("Cannot release an unlocked lock")
        self.local.set(None)
        await self.__release(expected_token)

    async def extend(self, additional_time: float) -> bool:
        """
        Adds more time to an already acquired lock.

        :param additional_time: can be specified as an integer or a float, both
         representing the number of seconds to add.

        :raises: :exc:`~coredis.exceptions.LockExtensionError`
        """

        if self.local.get() is None:
            raise LockExtensionError("Cannot extend an unlocked lock")

        if self.timeout is None:
            raise LockExtensionError("Cannot extend a lock with no timeout")

        return await self.__extend(additional_time)

    @property
    def replication_factor(self) -> int:
        """
        Number of replicas the lock needs to replicate to, to be
        considered acquired.
        """

        if isinstance(self.client, RedisCluster):
            return math.ceil(self.client.num_replicas_per_shard / 2)

        return 0

    async def __acquire(self, token: StringT, stop_trying_at: float | None) -> bool:
        if isinstance(self.client, RedisCluster):
            try:
                replication_wait = (
                    1000 * (max(0, stop_trying_at - time.time()))
                    if stop_trying_at is not None
                    else 100
                )
                with self.client.ensure_replication(
                    self.replication_factor, timeout_ms=int(replication_wait)
                ):
                    return await self.client.set(
                        self.name,
                        token,
                        condition=PureToken.NX,
                        px=int(self.timeout * 1000) if self.timeout else None,
                    )
            except ReplicationError:
                warnings.warn(
                    f"Unable to ensure lock {self.name!r} was replicated "
                    f"to {self.replication_factor} replicas",
                    category=RuntimeWarning,
                )
                await self.client.delete([self.name])

                return False
        else:
            return await self.client.set(
                self.name,
                token,
                condition=PureToken.NX,
                px=int(self.timeout * 1000) if self.timeout else None,
            )

    async def __release(self, expected_token: StringT) -> None:
        if not bool(
            await self.lua_release(
                self.client,
                self.name,
                expected_token,
            )
        ):
            raise LockReleaseError("Cannot release a lock that's no longer owned")

    async def __extend(self, additional_time: float) -> bool:
        additional_time = int(additional_time * 1000)

        if additional_time < 0:
            return True

        if not bool(
            await self.lua_extend(
                self.client,
                self.name,
                cast(AnyStr, self.local.get()),
                additional_time,
            )
        ):
            raise LockExtensionError("Cannot extend a lock that's no longer owned")

        return True
