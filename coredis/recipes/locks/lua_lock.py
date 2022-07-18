from __future__ import annotations

import asyncio
import contextvars
import importlib.resources
import time
import uuid
from types import TracebackType
from typing import cast

from coredis.client import Redis, RedisCluster
from coredis.commands import Script
from coredis.exceptions import LockError, ReplicationError
from coredis.tokens import PureToken
from coredis.typing import AnyStr, Generic, KeyT, Optional, StringT, Type, Union

EXTEND_SCRIPT = Script(script=importlib.resources.read_text(__package__, "extend.lua"))
RELEASE_SCRIPT = Script(
    script=importlib.resources.read_text(__package__, "release.lua")
)


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
    async def lua_release(
        cls,
        client: Union[Redis[AnyStr], RedisCluster[AnyStr]],
        name: KeyT,
        expected_token: StringT,
    ) -> int:
        ...

    @classmethod
    @EXTEND_SCRIPT.wraps(client_arg="client")
    async def lua_extend(
        cls,
        client: Union[Redis[AnyStr], RedisCluster[AnyStr]],
        name: KeyT,
        expected_token: StringT,
        additional_time: int,
    ) -> int:
        ...

    local: contextvars.ContextVar[Optional[StringT]]

    def __init__(
        self,
        client: Union[Redis[AnyStr], RedisCluster[AnyStr]],
        name: StringT,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
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
        self.client: Union[Redis[AnyStr], RedisCluster[AnyStr]] = client
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.local = contextvars.ContextVar[Optional[StringT]]("token", default=None)
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    async def __aenter__(
        self,
    ) -> LuaLock[AnyStr]:

        # force blocking, as otherwise the user would have to check whether
        # the lock was actually acquired or not.
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
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
        1 replica.

        :raises: :exc:`~coredis.exceptions.LockError`
        """
        token = uuid.uuid1().hex
        blocking = self.blocking
        blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout
        while True:
            if await self.__acquire(token):
                self.local.set(token)
                return True
            if not blocking:
                return False
            if stop_trying_at is not None and time.time() > stop_trying_at:
                return False
            await asyncio.sleep(self.sleep)

    async def release(self) -> None:
        """
        Releases the already acquired lock

        :raises: :exc:`~coredis.exceptions.LockError`
        """
        expected_token = self.local.get()
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        self.local.set(None)
        await self.__release(expected_token)

    async def extend(self, additional_time: float) -> bool:
        """
        Adds more time to an already acquired lock.

        :param additional_time: can be specified as an integer or a float, both
         representing the number of seconds to add.

        :raises: :exc:`~coredis.exceptions.LockError`
        """
        if self.local.get() is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return await self.__extend(additional_time)

    async def __acquire(self, token: StringT) -> bool:
        if isinstance(self.client, RedisCluster):
            try:
                with self.client.ensure_replication(1, timeout_ms=0):
                    return await self.client.set(
                        self.name,
                        token,
                        condition=PureToken.NX,
                        px=int(self.timeout * 1000) if self.timeout else None,
                    )
            except ReplicationError:
                raise LockError(f"Unable to ensure lock {self.name!r} was replicated ")
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
            raise LockError("Cannot release a lock that's no longer owned")

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
            raise LockError("Cannot extend a lock that's no longer owned")
        return True
