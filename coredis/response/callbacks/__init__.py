"""
coredis.response.callbacks
--------------------------
"""

from __future__ import annotations

import datetime
import itertools
from abc import ABC, abstractmethod

from coredis.exceptions import ClusterResponseError
from coredis.typing import (
    Any,
    Callable,
    Generic,
    List,
    Mapping,
    Optional,
    ParamSpec,
    ResponseType,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

R = TypeVar("R")
P = ParamSpec("P")

_RESP = TypeVar("_RESP", bound=ResponseType)
_RESP3 = TypeVar("_RESP3", bound=ResponseType)


class ResponseCallback(ABC, Generic[_RESP, _RESP3, R]):
    def __call__(self, response: Any, version: int = 2, **kwargs: Any) -> R:
        if version == 3:
            return self.transform_3(response, **kwargs)
        return self.transform(response, **kwargs)

    @abstractmethod
    def transform(self, response: Union[_RESP, _RESP3], **kwargs: Any) -> R:
        pass

    def transform_3(self, response: _RESP3, **kwargs: Any) -> R:
        return self.transform(response, **kwargs)


class ClusterMultiNodeCallback(ABC):
    def __call__(
        self, responses: Mapping[str, Any], version: int = 2, **kwargs: Any
    ) -> Any:
        if version == 3:
            return self.combine_3(responses, **kwargs)
        return self.combine(responses, **kwargs)

    @abstractmethod
    def combine(self, responses: Mapping[str, Any], **kwargs: Any) -> Any:
        pass

    def combine_3(self, responses: Mapping[str, Any], **kwargs: Any) -> Any:
        return self.combine(responses, **kwargs)


class ClusterBoolCombine(ClusterMultiNodeCallback):
    def combine(self, responses: Mapping[str, Any], **kwargs: Any) -> bool:
        values = tuple(responses.values())
        assert (isinstance(value, bool) for value in values)
        return all(values)


class ClusterAlignedBoolsCombine(ClusterMultiNodeCallback):
    def combine(self, responses: Mapping[str, Any], **kwargs: Any) -> Tuple[bool, ...]:
        return tuple(all(k) for k in zip(*responses.values()))


class ClusterEnsureConsistent(ClusterMultiNodeCallback):
    def __init__(self, ensure_consistent=True):
        self.ensure_consistent = ensure_consistent

    def combine(self, responses: Mapping[str, Any], **kwargs: Any) -> Any:
        values = tuple(responses.values())
        if self.ensure_consistent and len(set(values)) != 1:
            raise ClusterResponseError(
                "Inconsistent response from cluster nodes", responses
            )
        else:
            return values and values[0]


class ClusterMergeSets(ClusterMultiNodeCallback):
    def combine(self, responses: Mapping[str, Any], **kwargs: Any) -> Set:
        return set(itertools.chain(*responses.values()))


class SimpleStringCallback(ResponseCallback):
    def __init__(self, raise_on_error: Optional[Type[Exception]] = None):
        self.raise_on_error = raise_on_error

    def transform(self, response: Any, **options: Any) -> Any:
        success = response and (response in {"OK", b"OK"})
        if not success and self.raise_on_error:
            raise self.raise_on_error(response)
        return success


class PrimitiveCallback(ResponseCallback[ResponseType, ResponseType, R]):
    @abstractmethod
    def transform(self, response: Any, **options: Any) -> Any:
        pass


class FloatCallback(PrimitiveCallback[float]):
    def transform(self, response: Any, **options: Any) -> float:
        return response if isinstance(response, float) else float(response)


class BoolCallback(PrimitiveCallback[bool]):
    def transform(self, response: Any, **options: Any) -> bool:
        if isinstance(response, bool):
            return response
        return bool(response)


class SimpleStringOrIntCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Union[bool, int]:
        if isinstance(response, (int, bool)):
            return response
        else:
            return SimpleStringCallback()(response)


class TupleCallback(PrimitiveCallback[Tuple]):
    def transform(self, response: Any, **options: Any) -> Tuple:
        return tuple(response)


class ListCallback(PrimitiveCallback[List]):
    def transform(self, response: Any, **options: Any) -> List:
        if isinstance(response, list):
            return response
        return list(response)


class DateTimeCallback(ResponseCallback):
    def transform(self, response: Any, **kwargs: Any) -> datetime.datetime:
        ts = response
        if kwargs.get("unit") == "milliseconds":
            ts = ts / 1000.0
        return datetime.datetime.fromtimestamp(ts)


class DictCallback(PrimitiveCallback[Mapping]):
    def __init__(
        self,
        transform_function: Optional[Callable[[Any], Mapping]] = None,
    ):
        self.transform_function = transform_function

    def transform(self, response: Any, **options: Any) -> Mapping:
        return (
            (response if isinstance(response, dict) else dict(response))
            if not self.transform_function
            else self.transform_function(response)
        )


class SetCallback(PrimitiveCallback[Set]):
    def transform(self, response: Any, **options: Any) -> Set:
        if isinstance(response, set):
            return response
        return set(response) if response else set()


class BoolsCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Tuple[bool, ...]:
        return tuple(BoolCallback()(r) for r in response)


class OptionalPrimitiveCallback(
    ResponseCallback[ResponseType, ResponseType, Optional[R]]
):
    def transform(self, response: Any, **options: Any) -> Optional[R]:
        return response


class OptionalFloatCallback(OptionalPrimitiveCallback[float]):
    def transform(self, response: Any, **options: Any) -> Optional[float]:
        if isinstance(response, float):
            return response
        return response and float(response)


class OptionalIntCallback(OptionalPrimitiveCallback[int]):
    def transform(self, response: Any, **options: Any) -> Optional[int]:
        if isinstance(response, int):
            return response
        return response and int(response)


class OptionalSetCallback(OptionalPrimitiveCallback[Set]):
    def transform(self, response: Any, **options: Any) -> Optional[Set]:
        if isinstance(response, set):
            return response
        return response and set(response)


class OptionalTupleCallback(OptionalPrimitiveCallback[Tuple]):
    def transform(self, response: Any, **options: Any) -> Optional[Tuple]:
        return response and tuple(response)
