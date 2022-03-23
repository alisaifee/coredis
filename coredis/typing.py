import os
import warnings
from abc import ABC, abstractmethod
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    AnyStr,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import (
    OrderedDict,
    ParamSpec,
    TypeAlias,
    TypedDict,
    runtime_checkable,
)

RUNTIME_TYPECHECKS = False

if os.environ.get("COREDIS_RUNTIME_CHECKS"):
    try:
        import beartype

        if not TYPE_CHECKING:
            from beartype.typing import (  # noqa: F811
                Dict,
                Iterable,
                Iterator,
                List,
                Mapping,
                OrderedDict,
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
    async def execute_command(self, command: Any, *args: Any, **options: Any) -> Any:
        pass


__all__ = [
    "AbstractExecutor",
    "Any",
    "AnyStr",
    "AsyncGenerator",
    "Awaitable",
    "Callable",
    "CommandArgList",
    "Coroutine",
    "Dict",
    "Generic",
    "KeyT",
    "Iterable",
    "Iterator",
    "List",
    "Literal",
    "Mapping",
    "NamedTuple",
    "OrderedDict",
    "Optional",
    "ParamSpec",
    "Protocol",
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
