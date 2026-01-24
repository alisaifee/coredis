"""
coredis.response.callbacks
--------------------------
"""

from __future__ import annotations

import datetime
import itertools
from abc import ABC, ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, cast

from coredis._utils import b, nativestr
from coredis.exceptions import ClusterResponseError, ResponseError
from coredis.typing import (
    AnyStr,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Mapping,
    ParamSpec,
    Protocol,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
    Sequence,
    StringT,
    TypeVar,
    add_runtime_checks,
    runtime_checkable,
)

R = TypeVar("R")
S = TypeVar("S")
T = TypeVar("T")
P = ParamSpec("P")
CR_co = TypeVar("CR_co", covariant=True)
CK_co = TypeVar("CK_co", covariant=True)

RESP3 = TypeVar("RESP3")

if TYPE_CHECKING:
    from coredis.client import Client


class ResponseCallbackMeta(ABCMeta):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, str]
    ) -> ResponseCallbackMeta:
        kls = super().__new__(cls, name, bases, namespace)
        setattr(kls, "transform", add_runtime_checks(getattr(kls, "transform")))
        return kls


class ClusterCallbackMeta(ABCMeta):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, str]
    ) -> ClusterCallbackMeta:
        kls = super().__new__(cls, name, bases, namespace)
        setattr(kls, "combine", add_runtime_checks(getattr(kls, "combine")))
        return kls


class ResponseCallback(ABC, Generic[RESP3, R], metaclass=ResponseCallbackMeta):
    def __init__(self, **options: Any) -> None:
        self.options = options

    def __call__(
        self,
        response: RESP3 | ResponseError | TimeoutError,
    ) -> R:
        if isinstance(response, (ResponseError, TimeoutError)):
            exc_to_response = self.handle_exception(response)
            if exc_to_response:
                return exc_to_response
        return self.transform(response)

    @abstractmethod
    def transform(self, response: RESP3) -> R:
        pass

    def handle_exception(self, exc: BaseException) -> R | None:
        return exc  # type: ignore


@runtime_checkable
class AsyncPreProcessingCallback(Protocol):
    async def pre_process(self, client: Client[Any], response: ResponseType) -> None: ...


class NoopCallback(ResponseCallback[R, R]):
    def transform(self, response: R) -> R:
        return response


class ClusterMultiNodeCallback(ABC, Generic[R], metaclass=ClusterCallbackMeta):
    def __call__(
        self,
        responses: Mapping[str, R | ResponseError | TimeoutError],
    ) -> R:
        return self.combine(responses)

    @property
    @abstractmethod
    def response_policy(self) -> str: ...

    @abstractmethod
    def combine(self, responses: Mapping[str, R], **options: Any) -> R:
        pass

    @classmethod
    def raise_any(cls, values: Iterable[R]) -> None:
        for value in values:
            if isinstance(value, BaseException):
                raise value


class ClusterBoolCombine(ClusterMultiNodeCallback[bool]):
    def __init__(self, any: bool = False):
        self.any = any

    def combine(self, responses: Mapping[str, bool], **options: Any) -> bool:
        values = tuple(responses.values())
        self.raise_any(values)
        assert (isinstance(value, bool) for value in values)
        return any(values) if self.any else all(values)

    @property
    def response_policy(self) -> str:
        return (
            "success if any shards responded ``True``"
            if self.any
            else "success if all shards responded ``True``"
        )


class ClusterAlignedBoolsCombine(ClusterMultiNodeCallback[tuple[bool, ...]]):
    def combine(
        self, responses: Mapping[str, tuple[bool, ...]], **options: Any
    ) -> tuple[bool, ...]:
        return tuple(all(k) for k in zip(*responses.values()))

    @property
    def response_policy(self) -> str:
        return "the logical AND of all responses"


class ClusterEnsureConsistent(ClusterMultiNodeCallback[R | None]):
    def __init__(self, ensure_consistent: bool = True):
        self.ensure_consistent = ensure_consistent

    def combine(self, responses: Mapping[str, R | None], **options: Any) -> R | None:
        values = tuple(responses.values())
        self.raise_any(values)
        if self.ensure_consistent and len(set(values)) != 1:
            raise ClusterResponseError("Inconsistent response from cluster nodes")
        elif values:
            return values[0]
        return None

    @property
    def response_policy(self) -> str:
        return (
            "the response from any shard if all responses are consistent"
            if self.ensure_consistent
            else "first response"
        )


class ClusterFirstNonException(ClusterMultiNodeCallback[R | None]):
    def combine(self, responses: Mapping[str, R | None], **options: Any) -> R | None:
        for r in responses.values():
            if not isinstance(r, BaseException):
                return r
        for r in responses.values():
            if isinstance(r, BaseException):
                raise r

    @property
    def response_policy(self) -> str:
        return "the first response that is not an error"


class ClusterMergeSets(ClusterMultiNodeCallback[set[R]]):
    def combine(self, responses: Mapping[str, set[R]], **options: Any) -> set[R]:
        self.raise_any(responses.values())
        return set(itertools.chain(*responses.values()))

    @property
    def response_policy(self) -> str:
        return "the union of the results"


class ClusterSum(ClusterMultiNodeCallback[int]):
    def combine(self, responses: Mapping[str, int | ResponseError], **options: Any) -> int:
        self.raise_any(responses.values())
        return sum(responses.values())

    @property
    def response_policy(self) -> str:
        return "the sum of results"


class ClusterMergeMapping(ClusterMultiNodeCallback[dict[CK_co, CR_co]]):
    def __init__(self, value_combine: Callable[[Iterable[CR_co]], CR_co]) -> None:
        self.value_combine = value_combine

    def combine(
        self, responses: Mapping[str, dict[CK_co, CR_co]], **options: Any
    ) -> dict[CK_co, CR_co]:
        self.raise_any(responses.values())
        response: dict[CK_co, CR_co] = {}
        for key in set(itertools.chain(*responses.values())):
            values = list(responses[n][key] for n in responses if key in responses[n])
            response[key] = self.value_combine(values)
        return response

    @property
    def response_policy(self) -> str:
        return "the merged mapping"


class ClusterConcatenateTuples(ClusterMultiNodeCallback[tuple[R, ...]]):
    def combine(self, responses: Mapping[str, tuple[R, ...]], **options: Any) -> tuple[R, ...]:
        self.raise_any(responses.values())
        return tuple(itertools.chain(*responses.values()))

    @property
    def response_policy(self) -> str:
        return "the concatenations of the results"


class SimpleStringCallback(ResponseCallback[StringT | None, bool]):
    def __init__(
        self,
        raise_on_error: type[Exception] | None = None,
        prefix_match: bool = False,
        ok_values: set[str] = {"OK"},
        **options: Any,
    ):
        self.raise_on_error = raise_on_error
        self.prefix_match = prefix_match
        self.ok_values = {b(v) for v in ok_values}
        super().__init__(**options)

    def transform(self, response: StringT | None, **options: Any) -> bool:
        if response:
            if not self.prefix_match:
                success = b(response) in self.ok_values
            else:
                success = any(b(response).startswith(ok) for ok in self.ok_values)
        else:
            success = False
        if not success and self.raise_on_error:
            raise self.raise_on_error(response)
        return success


class IntCallback(ResponseCallback[int, int]):
    def transform(self, response: ResponsePrimitive, **options: Any) -> int:
        if isinstance(response, int):
            return response
        raise ValueError(f"Unable to map {response!r} to int")


class AnyStrCallback(ResponseCallback[StringT, AnyStr]):
    def transform(self, response: StringT, **options: Any) -> AnyStr:
        if isinstance(response, (bytes, str)):
            return cast(AnyStr, response)

        raise ValueError(f"Unable to map {response!r} to AnyStr")


class FloatCallback(ResponseCallback[StringT | int | float, float]):
    def transform(self, response: ResponseType, **options: Any) -> float:
        if isinstance(response, float):
            return response
        if isinstance(response, (int, bytes, str)):
            return float(response)

        raise ValueError(f"Unable to map {response} to float")


class BoolCallback(ResponseCallback[int | bool, bool]):
    def transform(self, response: ResponseType, **options: Any) -> bool:
        if isinstance(response, bool):
            return response
        return bool(response)


class SimpleStringOrIntCallback(ResponseCallback[RedisValueT, bool | int]):
    def transform(self, response: RedisValueT, **options: Any) -> bool | int:
        if isinstance(response, (int, bool)):
            return response
        elif isinstance(response, (str, bytes)):
            return SimpleStringCallback()(response)
        raise ValueError(f"Unable to map {response!r} to bool")


class TupleCallback(ResponseCallback[list[ResponseType], tuple[CR_co, ...]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[CR_co, ...]:
        if isinstance(response, list):
            return cast(tuple[CR_co, ...], tuple(response))
        raise ValueError(f"Unable to map {response!r} to tuple")


class ItemOrTupleCallback(
    ResponseCallback[
        list[ResponseType] | ResponsePrimitive,
        tuple[CR_co, ...] | CR_co,
    ]
):
    def transform(
        self, response: list[ResponseType] | ResponsePrimitive, **options: Any
    ) -> tuple[CR_co, ...] | CR_co:
        if isinstance(response, list):
            return cast(tuple[CR_co, ...], tuple(response))
        return cast(CR_co, response)


class MixedTupleCallback(ResponseCallback[list[ResponseType], tuple[R, S]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[R, S]:
        if isinstance(response, list):
            return cast(tuple[R, S], tuple(response))
        raise ValueError(f"Unable to map {response!r} to tuple")


class ListCallback(ResponseCallback[list[ResponseType], list[CR_co]]):
    def transform(self, response: list[ResponseType], **options: Any) -> list[CR_co]:
        return cast(list[CR_co], response)


class DateTimeCallback(ResponseCallback[int | float, datetime.datetime]):
    def transform(
        self,
        response: int | float,
    ) -> datetime.datetime:
        ts = float(response) if not isinstance(response, float) else response
        if self.options.get("unit") == "milliseconds":
            ts = ts / 1000.0
        return datetime.datetime.fromtimestamp(ts)


class DictCallback(
    ResponseCallback[
        Sequence[ResponseType] | dict[ResponsePrimitive, ResponseType],
        dict[CK_co, CR_co],
    ]
):
    def __init__(
        self,
        flat: bool = True,
        recursive: list[str] | None = None,
        **options: Any,
    ):
        self.flat = flat
        self.recursive = recursive or []
        super().__init__(**options)

    def transform(
        self,
        response: Sequence[ResponseType] | dict[ResponsePrimitive, ResponseType],
        **options: Any,
    ) -> dict[CK_co, CR_co]:
        if isinstance(response, dict):
            return cast(dict[CK_co, CR_co], response)
        elif isinstance(response, list):
            if self.flat:
                if self.recursive:
                    return cast(dict[CK_co, CR_co], self.recursive_transformer(response))
                else:
                    it = iter(response)
                    return cast(dict[CK_co, CR_co], dict(zip(it, it)))
            else:
                return dict(r for r in response)
        raise ValueError(f"Unable to map {response!r} to mapping")

    def recursive_transformer(
        self, item: Sequence[ResponseType] | dict[ResponsePrimitive, ResponseType]
    ) -> dict[CK_co, CR_co] | list[CK_co] | list[CR_co] | tuple[CK_co, ...] | tuple[CR_co, ...]:
        if isinstance(item, (list, tuple)):
            if all(isinstance(k, Hashable) for k in item[::2]):
                dct = []
                for i in range(0, 2 * (len(item) // 2), 2):
                    key, value = item[i], item[i + 1]
                    value = (
                        self.recursive_transformer(value)
                        if (nativestr(key) in self.recursive)
                        else value
                    )
                    dct.append((key, value))
                return dict(dct)
            else:
                caster = list if isinstance(item, list) else tuple
                return caster(self.recursive_transformer(i) for i in item)
        else:
            return item


class SetCallback(
    ResponseCallback[
        list[ResponsePrimitive] | set[ResponsePrimitive],
        set[CR_co],
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive] | set[ResponsePrimitive],
        **options: Any,
    ) -> set[CR_co]:
        if isinstance(response, list):
            return cast(set[CR_co], set(response))
        elif isinstance(response, set):
            return cast(set[CR_co], response)
        raise ValueError(f"Unable to map {response} to set")


class OneOrManyCallback(
    ResponseCallback[
        CR_co | list[CR_co | None] | None,
        CR_co | list[CR_co | None] | None,
    ]
):
    def transform(
        self,
        response: CR_co | list[CR_co | None] | None,
        **options: Any,
    ) -> CR_co | list[CR_co | None] | None:
        return response


class BoolsCallback(ResponseCallback[ResponseType, tuple[bool, ...]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[bool, ...]:
        if isinstance(response, list):
            return tuple(BoolCallback()(r) for r in response)
        return ()


class FloatsCallback(ResponseCallback[ResponseType, tuple[float, ...]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[float, ...]:
        if isinstance(response, list):
            return tuple(FloatCallback()(r) for r in response)
        return ()


class OptionalFloatCallback(
    ResponseCallback[
        StringT | int | float | None,
        float | None,
    ]
):
    def transform(
        self,
        response: StringT | int | float | None,
        **options: Any,
    ) -> float | None:
        if response is None:
            return None
        return FloatCallback()(response)


class OptionalIntCallback(ResponseCallback[int | None, int | None]):
    def transform(self, response: int | None, **options: Any) -> int | None:
        if response is None:
            return None
        if isinstance(response, int):
            return response
        raise ValueError(f"Unable to map {response} to int")


class OptionalAnyStrCallback(
    ResponseCallback[
        StringT | None,
        AnyStr | None,
    ]
):
    def transform(self, response: StringT | None, **options: Any) -> AnyStr | None:
        if response is None:
            return None
        if isinstance(response, (bytes, str)):
            return cast(AnyStr, response)
        raise ValueError(f"Unable to map {response} to AnyStr")


class OptionalListCallback(ResponseCallback[list[ResponseType], list[CR_co] | None]):
    def transform(self, response: ResponseType, **options: Any) -> list[CR_co] | None:
        return cast(list[CR_co], response)


class FirstValueCallback(ResponseCallback[list[CR_co], CR_co]):
    def transform(self, response: list[CR_co], **options: Any) -> CR_co:
        if response:
            return response[0]
        else:
            raise ValueError("Empty response list")
