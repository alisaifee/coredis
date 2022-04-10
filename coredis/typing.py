from __future__ import annotations

import os
import warnings
from abc import ABC, abstractmethod
from numbers import Number
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    AnyStr,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import (
    Deque,
    OrderedDict,
    ParamSpec,
    TypeAlias,
    TypedDict,
    runtime_checkable,
)

RUNTIME_TYPECHECKS = False

if os.environ.get("COREDIS_RUNTIME_CHECKS", "").lower() in ["1", "true", "t"]:
    try:
        import beartype

        if not TYPE_CHECKING:
            from beartype.typing import (  # noqa: F811
                Deque,
                Dict,
                Iterable,
                Iterator,
                List,
                Mapping,
                OrderedDict,
                Sequence,
                Set,
                Tuple,
                TypedDict,
            )

        RUNTIME_TYPECHECKS = True
    except ImportError:  # noqa
        warnings.warn("Runtime checks were requested but could not import beartype")

CommandArgList = List[Union[str, bytes, float, Number]]


P = ParamSpec("P")
R = TypeVar("R")

KeyT: TypeAlias = Union[str, bytes]
ValueT: TypeAlias = Union[str, bytes, int, float]
StringT: TypeAlias = KeyT

# TODO: mypy can't handle recursive types
ResponseType = Optional[
    Union[
        StringT,
        int,
        float,
        bool,
        AbstractSet,
        List,
        Tuple,
        Mapping,
        # AbstractSet["ResponseType"],
        # List["ResponseType"],
        # Mapping["ResponseType", "ResponseType"],
        Exception,
    ]
]


def add_runtime_checks(func: Callable[P, R]) -> Callable[P, R]:
    if RUNTIME_TYPECHECKS:
        return beartype.beartype(func)

    return func


@runtime_checkable
class SupportsWatch(Protocol):  # noqa
    async def __aenter__(self) -> "SupportsWatch":
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...

    async def watch(self, *keys: KeyT) -> bool:
        ...

    async def execute(self, raise_on_error=True) -> Any:
        ...


@runtime_checkable
class SupportsScript(Protocol):  # noqa
    async def evalsha(
        self,
        sha1: StringT,
        keys: Optional[Iterable[KeyT]] = None,
        args: Optional[Iterable[ValueT]] = None,
    ) -> Any:
        ...

    async def evalsha_ro(
        self,
        sha1: StringT,
        keys: Optional[Iterable[KeyT]] = None,
        args: Optional[Iterable[ValueT]] = None,
    ) -> Any:
        ...

    async def script_load(self, script: StringT) -> AnyStr:
        ...


@runtime_checkable
class SupportsPipeline(Protocol):  # noqa
    async def pipeline(
        self,
        transaction: Optional[bool] = True,
        watches: Optional[Iterable[StringT]] = None,
    ) -> SupportsWatch:
        ...


class AbstractExecutor(ABC, Generic[AnyStr]):
    @abstractmethod
    async def execute_command(self, command: bytes, *args: Any, **options: Any) -> Any:
        pass


__all__ = [
    "AbstractExecutor",
    "AbstractSet",
    "Any",
    "AnyStr",
    "AsyncGenerator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "CommandArgList",
    "Coroutine",
    "Deque",
    "Dict",
    "Generic",
    "KeyT",
    "Iterable",
    "Iterator",
    "List",
    "Literal",
    "Mapping",
    "MutableMapping",
    "NamedTuple",
    "OrderedDict",
    "Optional",
    "ParamSpec",
    "Protocol",
    "ResponseType",
    "Sequence",
    "Set",
    "SupportsWatch",
    "SupportsScript",
    "SupportsPipeline",
    "StringT",
    "Tuple",
    "Type",
    "TypedDict",
    "TypeVar",
    "Union",
    "ValueT",
    "TYPE_CHECKING",
    "RUNTIME_TYPECHECKS",
]
