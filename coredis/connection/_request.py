from __future__ import annotations

import dataclasses
from _weakref import ProxyType, proxy
from typing import TYPE_CHECKING, Any

from anyio import Event, get_cancelled_exc_class, move_on_after

from coredis._utils import nativestr
from coredis.typing import Generator, RedisValueT, ResponseType

if TYPE_CHECKING:
    from ._base import BaseConnection


@dataclasses.dataclass(slots=True)
class Request:
    connection: dataclasses.InitVar[BaseConnection]
    command: bytes
    args: tuple[RedisValueT, ...]
    decode: bool
    encoding: str | None = None
    raise_exceptions: bool = True
    response_timeout: float | None = None
    disconnect_on_cancellation: bool = False
    expects_response: bool = True

    _connection: ProxyType[BaseConnection] = dataclasses.field(init=False)
    _event: Event = dataclasses.field(init=False, default_factory=Event)
    _exc: BaseException | None = dataclasses.field(init=False, default=None)
    _result: ResponseType | None = dataclasses.field(init=False, default=None)

    def __post_init__(self, connection: BaseConnection) -> None:
        if not self.expects_response:
            self.resolve(None)
        self._connection = proxy(connection)

    def __await__(self) -> Generator[Any, None, ResponseType]:
        return self.get_result().__await__()

    def resolve(self, response: ResponseType) -> None:
        self._result = response
        self._event.set()

    def fail(self, error: BaseException) -> None:
        if not self._event.is_set():
            self._exc = error
            self._event.set()

    async def get_result(self) -> ResponseType:
        if not self._event.is_set():
            try:
                with move_on_after(self.response_timeout) as scope:
                    await self._event.wait()
                if scope.cancelled_caught and not self._event.is_set():
                    reason = (
                        f"{nativestr(self.command)} timed out after {self.response_timeout} seconds"
                    )
                    self._exc = TimeoutError(reason)
                    self._handle_response_cancellation(reason)
            except get_cancelled_exc_class():
                self._handle_response_cancellation(f"{nativestr(self.command)} was cancelled")
                raise
        return self._result_or_exc()

    def _handle_response_cancellation(self, reason: str) -> None:
        if self._connection and self.disconnect_on_cancellation:
            self._connection.terminate(reason)

    def _result_or_exc(self) -> ResponseType:
        if self._exc is not None:
            if self.raise_exceptions:
                raise self._exc
            return self._exc  # type: ignore
        return self._result
