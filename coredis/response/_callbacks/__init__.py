"""
coredis.response.callbacks
--------------------------
"""

from __future__ import annotations

import datetime
import itertools
from abc import ABC, abstractmethod
from typing import Any, cast

from coredis._utils import b
from coredis.exceptions import ClusterResponseError, ResponseError
from coredis.typing import (
    AnyStr,
    Callable,
    Generic,
    Iterable,
    ParamSpec,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
    Sequence,
    StringT,
    TypeGuard,
    TypeVar,
    add_runtime_checks,
)

R = TypeVar("R")
S = TypeVar("S")
T = TypeVar("T")
P = ParamSpec("P")
CR_co = TypeVar("CR_co", covariant=True)
CK_co = TypeVar("CK_co", covariant=True)
RESP3 = TypeVar("RESP3")


class ResponseCallback(ABC, Generic[RESP3, R]):
    def __init__(self, **options: Any) -> None:
        self.options = options

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        setattr(cls, "transform", add_runtime_checks(getattr(cls, "transform")))

    def __call__(
        self,
        response: RESP3,
    ) -> R:
        return self.transform(response)

    @abstractmethod
    def transform(self, response: RESP3) -> R:
        pass

    def handle_exception(self, exc: BaseException) -> R | None:
        return exc  # type: ignore


class NoopCallback(ResponseCallback[R, R]):
    def transform(self, response: R) -> R:
        return response


class ClusterMultiNodeCallback(ABC, Generic[R]):
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        setattr(cls, "combine", add_runtime_checks(getattr(cls, "combine")))

    def __call__(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R | ResponseError | TimeoutError],
    ) -> R:
        if self.successful(responses):
            return self.combine(key_positions, responses)
        else:
            return self.handle_error(key_positions, responses)

    @property
    @abstractmethod
    def response_policy(self) -> str: ...

    @abstractmethod
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R],
    ) -> R:
        pass

    def handle_error(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R | ResponseError | TimeoutError],
    ) -> R:
        for response in responses:
            if isinstance(response, (ResponseError, TimeoutError)):
                raise response
        assert False

    def successful(self, values: list[R | ResponseError | TimeoutError]) -> TypeGuard[list[R]]:
        for value in values:
            if isinstance(value, (ResponseError, TimeoutError)):
                return False
        return True


class ClusterNoMerge(ClusterMultiNodeCallback[R]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R],
    ) -> R:
        assert len(responses) == 1
        return responses[0]

    @property
    def response_policy(self) -> str:
        return "the response from the node"


class ClusterBoolCombine(ClusterMultiNodeCallback[bool]):
    def __init__(self, any: bool = False):
        self.any = any

    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[bool],
    ) -> bool:
        assert (isinstance(response, bool) for response in responses)
        return any(responses) if self.any else all(responses)

    @property
    def response_policy(self) -> str:
        return (
            "``True`` if any response is``True``"
            if self.any
            else "``True`` if all responses are ``True``"
        )


class ClusterAlignedBoolsCombine(ClusterMultiNodeCallback[tuple[bool, ...]]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[tuple[bool, ...]],
    ) -> tuple[bool, ...]:
        return tuple(all(k) for k in zip(*responses))

    @property
    def response_policy(self) -> str:
        return "the logical AND of all responses"


class ClusterEnsureConsistent(ClusterMultiNodeCallback[R | None]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R | None],
    ) -> R | None:
        if len(set(responses)) != 1:
            raise ClusterResponseError("Inconsistent response from cluster nodes")
        elif responses:
            return responses[0]
        return None

    @property
    def response_policy(self) -> str:
        return "the first response if all responses are consistent"


class ClusterFirstNonException(ClusterMultiNodeCallback[R | None]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R | None],
    ) -> R | None:
        if responses:
            return responses[0]
        return None

    def handle_error(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[R | None | ResponseError | TimeoutError],
    ) -> R | None:
        first_exc: ResponseError | TimeoutError | None = None
        for response in responses:
            if not isinstance(response, (ResponseError, TimeoutError)):
                return response
            else:
                first_exc = response
        if first_exc is not None:
            raise first_exc
        return None

    @property
    def response_policy(self) -> str:
        return "the first response that is not an error"


class ClusterMergeSets(ClusterMultiNodeCallback[set[R]]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[set[R]],
    ) -> set[R]:
        return set(itertools.chain(*responses))

    @property
    def response_policy(self) -> str:
        return "the union of the results"


class ClusterSum(ClusterMultiNodeCallback[int]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[int],
    ) -> int:
        return sum(responses)

    @property
    def response_policy(self) -> str:
        return "the sum of results"


class ClusterMergeMapping(ClusterMultiNodeCallback[dict[R, S]]):
    def __init__(self, value_combine: Callable[[Iterable[S]], S]) -> None:
        self.value_combine = value_combine

    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[dict[R, S]],
    ) -> dict[R, S]:
        response: dict[R, S] = {}
        for key in set(itertools.chain(*responses)):
            values = list(
                response[key] for idx, response in enumerate(responses) if key in response
            )
            response[key] = self.value_combine(values)
        return response

    @property
    def response_policy(self) -> str:
        return "the merged mapping"


class ClusterConcatenateTuples(ClusterMultiNodeCallback[tuple[R, ...]]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[tuple[R, ...]],
    ) -> tuple[R, ...]:
        total = sum(len(response) for response in responses)
        output: list[R | None] = [None] * total
        for chunk_key_positions, result in zip(key_positions, responses):
            for position, value in zip(chunk_key_positions, result):
                output[position] = value
        return tuple(cast(list[R], output))

    @property
    def response_policy(self) -> str:
        return "the concatenations of the results"


class ClusterConcatenateLists(ClusterMultiNodeCallback[list[R]]):
    def combine(
        self,
        key_positions: list[tuple[int, ...]],
        responses: list[list[R]],
    ) -> list[R]:
        total = sum(len(response) for response in responses)
        output: list[R | None] = [None] * total
        for chunk_key_positions, result in zip(key_positions, responses):
            for position, value in zip(chunk_key_positions, result):
                output[position] = value
        return cast(list[R], output)

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
    def transform(self, response: StringT | int | float, **options: Any) -> float:
        if isinstance(response, float):
            return response
        if isinstance(response, (int, bytes, str)):
            return float(response)

        raise ValueError(f"Unable to map {response} to float")


class BoolCallback(ResponseCallback[int | bool, bool]):
    def transform(self, response: int | bool, **options: Any) -> bool:
        if isinstance(response, bool):
            return response
        return bool(response)


class SimpleStringOrIntCallback(ResponseCallback[RedisValueT, bool | int]):
    def transform(self, response: RedisValueT, **options: Any) -> bool | int:
        if isinstance(response, (int, bool)):
            return response
        elif isinstance(response, (str, bytes)):
            return SimpleStringCallback().transform(response)
        raise ValueError(f"Unable to map {response!r} to bool")


class TupleCallback(ResponseCallback[list[ResponseType], tuple[R, ...]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[R, ...]:
        if isinstance(response, list):
            return cast(tuple[R, ...], tuple(response))
        raise ValueError(f"Unable to map {response!r} to tuple")


class ItemOrTupleCallback(
    ResponseCallback[
        list[ResponseType] | ResponsePrimitive,
        tuple[R, ...] | R,
    ]
):
    def transform(
        self, response: list[ResponseType] | ResponsePrimitive, **options: Any
    ) -> tuple[R, ...] | R:
        if isinstance(response, list):
            return cast(tuple[R, ...], tuple(response))
        return cast(R, response)


class MixedTupleCallback(ResponseCallback[list[ResponseType], tuple[R, S]]):
    def transform(self, response: ResponseType, **options: Any) -> tuple[R, S]:
        if isinstance(response, list):
            return cast(tuple[R, S], tuple(response))
        raise ValueError(f"Unable to map {response!r} to tuple")


class ListCallback(ResponseCallback[list[ResponseType], list[R]]):
    def transform(self, response: list[ResponseType], **options: Any) -> list[R]:
        return cast(list[R], response)


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
        dict[R, S],
    ]
):
    def transform(
        self,
        response: Sequence[ResponseType] | dict[ResponsePrimitive, ResponseType],
        **options: Any,
    ) -> dict[R, S]:
        if isinstance(response, dict):
            return cast(dict[R, S], response)
        elif isinstance(response, list):
            it = iter(response)
            return cast(dict[R, S], dict(zip(it, it)))
        raise ValueError(f"Unable to map {response!r} to mapping")


class SetCallback(
    ResponseCallback[
        list[ResponsePrimitive] | set[ResponsePrimitive],
        set[R],
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive] | set[ResponsePrimitive],
        **options: Any,
    ) -> set[R]:
        if isinstance(response, list):
            return cast(set[R], set(response))
        elif isinstance(response, set):
            return cast(set[R], response)
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


class BoolsCallback(ResponseCallback[Iterable[int | bool], tuple[bool, ...]]):
    def transform(self, response: Iterable[int | bool], **options: Any) -> tuple[bool, ...]:
        if isinstance(response, list):
            return tuple(BoolCallback().transform(r) for r in response)
        return ()


class FloatsCallback(ResponseCallback[list[StringT | float | int], tuple[float, ...]]):
    def transform(self, response: list[StringT | float | int], **options: Any) -> tuple[float, ...]:
        if isinstance(response, list):
            return tuple(FloatCallback().transform(r) for r in response)
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
        return FloatCallback().transform(response)


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


class FirstValueCallback(ResponseCallback[dict[R, S], S]):
    def transform(self, response: dict[R, S], **options: Any) -> S:
        if response:
            return list(response.values())[0]
        else:
            raise ValueError("Empty response")
