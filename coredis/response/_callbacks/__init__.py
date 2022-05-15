"""
coredis.response.callbacks
--------------------------
"""

# pyright: reportUnnecessaryIsInstance=false
from __future__ import annotations

import datetime
import itertools
from abc import ABC, ABCMeta, abstractmethod
from typing import SupportsFloat, SupportsInt, cast

from coredis.exceptions import ClusterResponseError, ResponseError
from coredis.typing import (
    AnyStr,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    ParamSpec,
    ResponsePrimitive,
    ResponseType,
    Sequence,
    Set,
    StringT,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValueT,
    add_runtime_checks,
)

R = TypeVar("R")
P = ParamSpec("P")
CR_co = TypeVar("CR_co", covariant=True)
CK_co = TypeVar("CK_co", covariant=True)

RESP = TypeVar("RESP")
RESP3 = TypeVar("RESP3")


class ResponseCallbackMeta(ABCMeta):
    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, str]
    ) -> ResponseCallbackMeta:
        kls = super().__new__(cls, name, bases, namespace)
        setattr(kls, "transform", add_runtime_checks(getattr(kls, "transform")))
        setattr(kls, "transform_3", add_runtime_checks(getattr(kls, "transform_3")))
        return kls


class ClusterCallbackMeta(ABCMeta):
    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, str]
    ) -> ClusterCallbackMeta:
        kls = super().__new__(cls, name, bases, namespace)
        setattr(kls, "combine", add_runtime_checks(getattr(kls, "combine")))
        setattr(kls, "combine_3", add_runtime_checks(getattr(kls, "combine_3")))
        return kls


class ResponseCallback(ABC, Generic[RESP, RESP3, R], metaclass=ResponseCallbackMeta):
    version: Literal[2, 3]

    def __call__(
        self,
        response: Union[RESP, RESP3, ResponseError],
        version: Literal[2, 3] = 2,
        **options: Optional[ValueT],
    ) -> R:
        self.version = version
        if isinstance(response, ResponseError):
            if exc_to_response := self.handle_exception(response):
                return exc_to_response
        if version == 3:
            return self.transform_3(cast(RESP3, response), **options)
        return self.transform(cast(RESP, response), **options)

    @abstractmethod
    def transform(self, response: RESP, **options: Optional[ValueT]) -> R:
        pass

    def transform_3(self, response: RESP3, **options: Optional[ValueT]) -> R:
        return self.transform(cast(RESP, response), **options)

    def handle_exception(self, exc: BaseException) -> Optional[R]:
        return exc  # type: ignore


class NoopCallback(ResponseCallback[R, R, R]):
    def transform(self, response: R, **kwargs: Optional[ValueT]) -> R:
        return response


class ClusterMultiNodeCallback(ABC, Generic[R], metaclass=ClusterCallbackMeta):
    def __call__(
        self, responses: Mapping[str, R], version: int = 2, **kwargs: Optional[ValueT]
    ) -> R:
        if version == 3:
            return self.combine_3(responses, **kwargs)
        return self.combine(responses, **kwargs)

    @abstractmethod
    def combine(self, responses: Mapping[str, R], **kwargs: Optional[ValueT]) -> R:
        pass

    def combine_3(self, responses: Mapping[str, R], **kwargs: Optional[ValueT]) -> R:
        return self.combine(responses, **kwargs)


class ClusterBoolCombine(ClusterMultiNodeCallback[bool]):
    def combine(
        self, responses: Mapping[str, bool], **kwargs: Optional[ValueT]
    ) -> bool:
        values = tuple(responses.values())
        assert (isinstance(value, bool) for value in values)
        return all(values)


class ClusterAlignedBoolsCombine(ClusterMultiNodeCallback[Tuple[bool, ...]]):
    def combine(
        self, responses: Mapping[str, Tuple[bool, ...]], **kwargs: Optional[ValueT]
    ) -> Tuple[bool, ...]:
        return tuple(all(k) for k in zip(*responses.values()))


class ClusterEnsureConsistent(ClusterMultiNodeCallback[Optional[R]]):
    def __init__(self, ensure_consistent: bool = True):
        self.ensure_consistent = ensure_consistent

    def combine(
        self, responses: Mapping[str, Optional[R]], **kwargs: Optional[ValueT]
    ) -> Optional[R]:
        values = tuple(responses.values())
        if self.ensure_consistent and len(set(values)) != 1:
            raise ClusterResponseError(
                "Inconsistent response from cluster nodes", responses
            )
        elif values:
            return values[0]
        return None


class ClusterConcatTuples(ClusterMultiNodeCallback[Tuple[R, ...]]):
    def combine(
        self, responses: Mapping[str, Tuple[R, ...]], **kwargs: Optional[ValueT]
    ) -> Set[R]:
        return tuple(itertools.chain(*responses.values()))


class ClusterMergeSets(ClusterMultiNodeCallback[Set[R]]):
    def combine(
        self, responses: Mapping[str, Set[R]], **kwargs: Optional[ValueT]
    ) -> Set[R]:
        return set(itertools.chain(*responses.values()))


class ClusterSum(ClusterMultiNodeCallback[int]):
    def combine(self, responses: Mapping[str, int], **kwargs: Optional[ValueT]) -> int:
        return sum(responses.values())


class ClusterMergeMapping(ClusterMultiNodeCallback[Dict[CK_co, CR_co]]):
    def __init__(self, value_combine: Callable[[Iterable[CR_co]], CR_co]) -> None:
        self.value_combine = value_combine

    def combine(
        self, responses: Mapping[str, Dict[CK_co, CR_co]], **kwargs: Optional[ValueT]
    ) -> Dict[CK_co, CR_co]:
        response: Dict[CK_co, CR_co] = {}
        for key in set(itertools.chain(*responses.values())):
            response[key] = self.value_combine(responses[n].get(key) for n in responses)
        return response


class SimpleStringCallback(
    ResponseCallback[Optional[StringT], Optional[StringT], bool]
):
    def __init__(
        self,
        raise_on_error: Optional[Type[Exception]] = None,
        prefix_match: bool = False,
    ):
        self.raise_on_error = raise_on_error
        self.prefix_match = prefix_match

    def transform(
        self, response: Optional[StringT], **options: Optional[ValueT]
    ) -> bool:
        if response:
            if not self.prefix_match:
                success = response in {"OK", b"OK"}
            else:
                success = response[:2] in {"OK", b"OK"}
        else:
            success = False
        if not success and self.raise_on_error:
            raise self.raise_on_error(response)
        return success


class IntCallback(ResponseCallback[int, int, int]):
    def transform(
        self, response: ResponsePrimitive, **options: Optional[ValueT]
    ) -> int:
        if isinstance(response, int):
            return response
        raise ValueError(f"Unable to map {response!r} to int")


class AnyStrCallback(ResponseCallback[StringT, StringT, AnyStr]):
    def transform(self, response: StringT, **options: Optional[ValueT]) -> AnyStr:
        if isinstance(response, (bytes, str)):
            return cast(AnyStr, response)

        raise ValueError(f"Unable to map {response!r} to AnyStr")


class BytesCallback(ResponseCallback[bytes, bytes, bytes]):
    def transform(self, response: bytes, **options: Optional[ValueT]) -> bytes:
        return response


class FloatCallback(
    ResponseCallback[Union[StringT, int, float], Union[StringT, int, float], float]
):
    def transform(self, response: ResponseType, **options: Optional[ValueT]) -> float:
        if isinstance(response, float):
            return response
        if isinstance(response, (SupportsFloat, SupportsInt, bytes, str)):
            return float(response)

        raise ValueError(f"Unable to map {response} to float")


class BoolCallback(ResponseCallback[Union[int, bool], Union[int, bool], bool]):
    def transform(self, response: ResponseType, **options: Optional[ValueT]) -> bool:
        if isinstance(response, bool):
            return response
        return bool(response)


class SimpleStringOrIntCallback(ResponseCallback[ValueT, ValueT, Union[bool, int]]):
    def transform(
        self, response: ValueT, **options: Optional[ValueT]
    ) -> Union[bool, int]:
        if isinstance(response, (int, bool)):
            return response
        elif isinstance(response, (str, bytes)):
            return SimpleStringCallback()(response)
        raise ValueError(f"Unable to map {response!r} to bool")


class TupleCallback(
    ResponseCallback[List[ResponseType], List[ResponseType], Tuple[CR_co, ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[CR_co, ...]:
        if isinstance(response, List):
            return cast(Tuple[CR_co, ...], tuple(response))
        raise ValueError(f"Unable to map {response!r} to tuple")


class ListCallback(
    ResponseCallback[List[ResponseType], List[ResponseType], List[CR_co]]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> List[CR_co]:
        return cast(List[CR_co], response)


class DateTimeCallback(
    ResponseCallback[Union[int, float], Union[int, float], datetime.datetime]
):
    def transform(
        self, response: Union[int, float], **kwargs: Optional[ValueT]
    ) -> datetime.datetime:
        ts = float(response) if not isinstance(response, float) else response
        if kwargs.get("unit") == "milliseconds":
            ts = ts / 1000.0
        return datetime.datetime.fromtimestamp(ts)


class DictCallback(
    ResponseCallback[
        Union[Sequence[ResponseType], Dict[ResponsePrimitive, ResponseType]],
        Union[Sequence[ResponseType], Dict[ResponsePrimitive, ResponseType]],
        Dict[CK_co, CR_co],
    ]
):
    def transform(
        self,
        response: Union[Sequence[ResponseType], Dict[ResponsePrimitive, ResponseType]],
        **options: Optional[ValueT],
    ) -> Dict[CK_co, CR_co]:
        if isinstance(response, list):
            it = iter(response)
            return cast(Dict[CK_co, CR_co], dict(zip(it, it)))
        raise ValueError(f"Unable to map {response!r} to mapping")

    def transform_3(
        self,
        response: Union[Sequence[ResponseType], Dict[ResponsePrimitive, ResponseType]],
        **options: Optional[ValueT],
    ) -> Dict[CK_co, CR_co]:

        if isinstance(response, Dict):
            return cast(Dict[CK_co, CR_co], response)
        return self.transform(response, **options)


class SetCallback(
    ResponseCallback[
        List[ResponsePrimitive],
        Set[ResponsePrimitive],
        Set[CR_co],
    ]
):
    def transform(
        self,
        response: Union[List[ResponsePrimitive], Set[ResponsePrimitive]],
        **options: Optional[ValueT],
    ) -> Set[CR_co]:
        if isinstance(response, list):
            return cast(Set[CR_co], set(response))
        raise ValueError(f"Unable to map {response} to set")

    def transform_3(
        self,
        response: Union[List[ResponsePrimitive], Set[ResponsePrimitive]],
        **options: Optional[ValueT],
    ) -> Set[CR_co]:
        if isinstance(response, set):
            return cast(Set[CR_co], response)
        else:
            return self.transform(response)


class BoolsCallback(ResponseCallback[ResponseType, ResponseType, Tuple[bool, ...]]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[bool, ...]:
        if isinstance(response, List):
            return tuple(BoolCallback()(r) for r in response)
        return ()


class OptionalFloatCallback(
    ResponseCallback[
        Optional[Union[StringT, int, float]],
        Optional[Union[StringT, int, float]],
        Optional[float],
    ]
):
    def transform(
        self,
        response: Optional[Union[StringT, int, float]],
        **options: Optional[ValueT],
    ) -> Optional[float]:
        if response is None:
            return None
        return FloatCallback()(response)


class OptionalIntCallback(
    ResponseCallback[Optional[int], Optional[int], Optional[int]]
):
    def transform(
        self, response: Optional[int], **options: Optional[ValueT]
    ) -> Optional[int]:
        if response is None:
            return None
        if isinstance(response, int):
            return response
        raise ValueError(f"Unable to map {response} to int")


class OptionalAnyStrCallback(
    ResponseCallback[
        Optional[StringT],
        Optional[AnyStr],
        Optional[AnyStr],
    ]
):
    def transform(
        self, response: Optional[StringT], **options: Optional[ValueT]
    ) -> Optional[AnyStr]:
        if response is None:
            return None
        if isinstance(response, (bytes, str)):
            return cast(AnyStr, response)
        raise ValueError(f"Unable to map {response} to AnyStr")


class OptionalListCallback(
    ResponseCallback[List[ResponseType], List[ResponseType], Optional[List[CR_co]]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Optional[List[CR_co]]:
        return cast(List[CR_co], response)
