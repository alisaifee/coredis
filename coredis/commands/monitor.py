from __future__ import annotations

import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, Any

from deprecated.sphinx import deprecated

from coredis.commands.constants import CommandName
from coredis.exceptions import ConnectionError, RedisError
from coredis.response.types import MonitorResult
from coredis.typing import AnyStr, Callable, Generator, Generic, Self, TypeVar

if TYPE_CHECKING:
    import coredis.client
    import coredis.connection

MonitorT = TypeVar("MonitorT", bound="Monitor[Any]")


class Monitor(Generic[AnyStr]):
    """
    Monitor is useful for handling the ``MONITOR`` command to the redis server.

    It can be used as an infinite async iterator::

        async with client.monitor() as monitor:
            async for command in monitor:
                print(command.time, command.client_type, command.command, command.args)

    Alternatively, each command can be fetched explicitly::

        monitor = client.monitor()
        command1 = await monitor.get_command()
        command2 = await monitor.get_command()
        await monitor.aclose()

    If you are only interested in triggering callbacks when a command is received
    by the monitor::
        def monitor_handler(result: MonitorResult) -> None:
            ....

        monitor = await client.monitor(response_handler=monitor_handler)
        # when done
        await monitor.aclose()
    """

    def __init__(
        self,
        client: coredis.client.Client[AnyStr],
        response_handler: Callable[[MonitorResult], None] | None = None,
    ):
        """
        :param client: a Redis client
        :param response_handler: optional callback to call whenever a
         command is received by the monitor
        """
        self.client: coredis.client.Client[AnyStr] = client
        self.encoding = client.encoding
        self.connection: coredis.connection.Connection | None = None
        self.monitoring = False
        self._monitor_results: asyncio.Queue[MonitorResult] = asyncio.Queue()
        self._monitor_task: asyncio.Task[None] | None = None
        self._response_handler = response_handler

    def __aiter__(self) -> Monitor[AnyStr]:
        return self

    async def __anext__(self) -> MonitorResult:
        """
        Infinite iterator that streams back the next command processed by the
        monitored server.
        """
        return await self.get_command()

    def __await__(self: MonitorT) -> Generator[Any, None, MonitorT]:
        return self.__start_monitor().__await__()

    async def __aenter__(self) -> Self:
        await self.__start_monitor()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def get_command(self) -> MonitorResult:
        """
        Wait for the next command issued and return the details
        """
        await self.__start_monitor()
        return await self._monitor_results.get()

    async def aclose(self) -> None:
        """
        Stop monitoring by issuing a ``RESET`` command
        and release the connection.
        """
        return await self.__stop_monitoring()

    @deprecated("Use :meth:`aclose` instead", version="4.21.0")
    async def stop(self) -> None:
        """
        Stop monitoring by issuing a ``RESET`` command
        and release the connection.
        """
        return await self.aclose()

    async def __connect(self) -> None:
        if self.connection is None:
            self.connection = await self.client.connection_pool.get_connection()

    async def __start_monitor(self: MonitorT) -> MonitorT:
        if self.monitoring:
            return self
        await self.__connect()
        assert self.connection
        request = await self.connection.create_request(CommandName.MONITOR, decode=False)
        response = await request
        if not response == b"OK":  # noqa
            raise RedisError(f"Failed to start MONITOR {response!r}")
        if not self._monitor_task or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor())
        self.monitoring = True
        return self

    async def __stop_monitoring(self) -> None:
        if self.connection:
            request = await self.connection.create_request(CommandName.RESET, decode=False)
            response = await request
            if not response == CommandName.RESET:  # noqa
                raise RedisError("Failed to reset connection")
        self.__reset()

    def __reset(self) -> None:
        if self.connection:
            self.connection.disconnect()
            self.client.connection_pool.release(self.connection)
        if self._monitor_task and not self._monitor_task.done():
            try:
                self._monitor_task.cancel()
            except RuntimeError:  # noqa
                pass
        self.monitoring = False
        self.connection = None

    async def _monitor(self) -> None:
        while self.connection:
            try:
                response = await self.connection.fetch_push_message(block=True)
                if isinstance(response, bytes):
                    response = response.decode(self.encoding)
                assert isinstance(response, str)
                result = MonitorResult.parse_response_string(response)
                if self._response_handler:
                    self._response_handler(result)
                else:
                    self._monitor_results.put_nowait(result)
            except asyncio.CancelledError:
                break
            except ConnectionError:
                break
        self.__reset()
