from __future__ import annotations

import dataclasses
from _weakref import ProxyType, proxy
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from anyio import Event, get_cancelled_exc_class, move_on_after

from coredis._telemetry import get_telemetry_provider
from coredis._utils import nativestr
from coredis.typing import Generator, ResponseType

if TYPE_CHECKING:
    from ._base import BaseConnection


@dataclasses.dataclass
class BaseRequest(ABC):
    connection: dataclasses.InitVar[BaseConnection]
    disconnect_on_cancellation: bool = False
    response_timeout: float | None = None
    _connection: ProxyType[BaseConnection] = dataclasses.field(init=False)

    @property
    @abstractmethod
    def complete(self) -> bool: ...

    @property
    @abstractmethod
    def current_decode(self) -> bool: ...

    @property
    @abstractmethod
    def current_encoding(self) -> str | None: ...

    @abstractmethod
    def consume(self, response: ResponseType | BaseException) -> None: ...

    @abstractmethod
    def fail(self, error: BaseException) -> None: ...

    def _handle_response_cancellation(self, reason: str) -> None:
        if self._connection and self.disconnect_on_cancellation:
            self._connection.invalidate(reason)


@dataclasses.dataclass(kw_only=True)
class Request(BaseRequest):
    command: bytes
    decode: bool
    encoding: str | None = None
    raise_exceptions: bool = True
    expects_response: bool = True
    _event: Event = dataclasses.field(init=False, default_factory=Event)
    _exc: BaseException | None = dataclasses.field(init=False, default=None)
    _result: ResponseType | None = dataclasses.field(init=False, default=None)

    def __post_init__(self, connection: BaseConnection) -> None:
        get_telemetry_provider().update_span_attributes(connection)
        self._connection = proxy(connection)
        self._connection.statistics.request_created()
        if not self.expects_response:
            self.resolve(None)

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

    def _result_or_exc(self) -> ResponseType:
        if self._exc is not None:
            if self.raise_exceptions:
                raise self._exc
            return self._exc  # type: ignore
        return self._result

    @property
    def complete(self) -> bool:
        return self._event.is_set()

    @property
    def current_decode(self) -> bool:
        return self.decode

    @property
    def current_encoding(self) -> str | None:
        return self.encoding

    def consume(self, response: ResponseType | BaseException) -> None:
        self._connection.statistics.request_resolved()
        if isinstance(response, BaseException):
            self.fail(response)
        else:
            self.resolve(response)


@dataclasses.dataclass(kw_only=True)
class RequestBatch(BaseRequest):
    commands: tuple[bytes, ...]
    decode: tuple[bool, ...]
    encoding: tuple[str | None, ...]
    _event: Event = dataclasses.field(init=False, default_factory=Event)
    _cursor: int = dataclasses.field(init=False, default=0)
    _results: list[ResponseType | BaseException | None] = dataclasses.field(init=False)

    def __post_init__(self, connection: BaseConnection) -> None:
        self._connection = proxy(connection)
        self._results = [None] * len(self.commands)
        self._connection.statistics.request_created(len(self.commands))
        if not self.commands:
            self._event.set()

    @property
    def complete(self) -> bool:
        return self._cursor == len(self.commands)

    @property
    def current_decode(self) -> bool:
        return self.decode[self._cursor]

    @property
    def current_encoding(self) -> str | None:
        return self.encoding[self._cursor]

    def consume(self, response: ResponseType | BaseException) -> None:
        if not self.complete:
            self._results[self._cursor] = response
            self._cursor += 1
            self._connection.statistics.request_resolved()
            if self.complete:
                self._event.set()

    def fail(self, error: BaseException) -> None:
        while not self.complete:
            self._results[self._cursor] = error
            self._cursor += 1
        self._event.set()

    def __await__(self) -> Generator[Any, None, list[ResponseType | BaseException | None]]:
        return self.get_result().__await__()

    async def get_result(self) -> list[ResponseType | BaseException | None]:
        if not self._event.is_set():
            try:
                with move_on_after(self.response_timeout) as scope:
                    await self._event.wait()
                if scope.cancelled_caught and not self.complete:
                    reason = ""
                    while not self.complete:
                        reason = (
                            f"{nativestr(self.commands[self._cursor])} timed out after "
                            f"{self.response_timeout} seconds"
                        )
                        self._results[self._cursor] = TimeoutError(reason)
                        self._cursor += 1
                    self._event.set()
                    self._handle_response_cancellation(reason)
            except get_cancelled_exc_class():
                self._handle_response_cancellation("Batch was cancelled")
                raise
        return self._results
