from __future__ import annotations

from typing import SupportsFloat, cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ScoredMember, ScoredMembers
from coredis.typing import (
    AnyStr,
    Generic,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Union,
    ValueT,
)


class ZRankCallback(
    ResponseCallback[
        Optional[Union[int, list[ResponsePrimitive]]],
        Optional[Union[int, list[ResponsePrimitive]]],
        Optional[Union[int, tuple[int, float]]],
    ],
):
    def transform(
        self,
        response: Optional[Union[int, list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[int, tuple[int, float]]]:
        if options.get("withscore"):
            return (response[0], float(response[1])) if response else None
        else:
            return cast(Optional[int], response)

    def transform_3(
        self,
        response: Optional[Union[int, list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[int, tuple[int, float]]]:
        if options.get("withscore"):
            return (response[0], response[1]) if response else None
        else:
            return cast(Optional[int], response)


class ZMembersOrScoredMembers(
    ResponseCallback[
        list[Union[AnyStr, list[ResponsePrimitive]]],
        list[Union[AnyStr, list[ResponsePrimitive]]],
        tuple[Union[AnyStr, ScoredMember], ...],
    ],
):
    def transform(
        self,
        response: list[Union[AnyStr, list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> tuple[Union[AnyStr, ScoredMember], ...]:
        if not response:
            return ()
        elif options.get("withscores"):
            it = iter(cast(list[AnyStr], response))
            return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))
        else:
            return cast(tuple[AnyStr, ...], tuple(response))

    def transform_3(
        self,
        response: list[Union[AnyStr, list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> tuple[Union[AnyStr, ScoredMember], ...]:
        if options.get("withscores"):
            return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))
        else:
            return cast(tuple[AnyStr, ...], tuple(response))


class ZSetScorePairCallback(
    ResponseCallback[
        Optional[list[ResponsePrimitive]],
        Optional[list[Union[ResponsePrimitive, list[ResponsePrimitive]]]],
        Optional[Union[ScoredMember, ScoredMembers]],
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: Optional[list[ResponsePrimitive]], **options: Optional[ValueT]
    ) -> Optional[Union[ScoredMember, ScoredMembers]]:
        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(cast(AnyStr, response[0]), float(cast(SupportsFloat, response[1])))

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: Optional[list[Union[ResponsePrimitive, list[ResponsePrimitive]]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[ScoredMember, ScoredMembers]]:
        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(*cast(tuple[AnyStr, float], response))

        return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))


class ZMPopCallback(
    ResponseCallback[
        Optional[list[ResponseType]],
        Optional[list[ResponseType]],
        Optional[tuple[AnyStr, ScoredMembers]],
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: Optional[list[ResponseType]], **options: Optional[ValueT]
    ) -> Optional[tuple[AnyStr, ScoredMembers]]:
        r = cast(tuple[AnyStr, list[tuple[AnyStr, int]]], response)
        if r:
            return r[0], tuple(ScoredMember(v[0], float(v[1])) for v in r[1])

        return None


class ZMScoreCallback(
    ResponseCallback[list[ResponsePrimitive], list[ResponsePrimitive], tuple[Optional[float], ...]]
):
    def transform(
        self, response: list[ResponsePrimitive], **options: Optional[ValueT]
    ) -> tuple[Optional[float], ...]:
        return tuple(score if score is None else float(score) for score in response)


class ZScanCallback(
    ResponseCallback[list[ResponseType], list[ResponseType], tuple[int, ScoredMembers]],
    Generic[AnyStr],
):
    def transform(
        self, response: list[ResponseType], **options: Optional[ValueT]
    ) -> tuple[int, ScoredMembers]:
        cursor, r = cast(tuple[int, list[AnyStr]], response)
        it = iter(r)
        return int(cursor), tuple(
            ScoredMember(*cast(tuple[AnyStr, float], v)) for v in zip(it, map(float, it))
        )


class ZRandMemberCallback(
    ResponseCallback[
        Optional[Union[AnyStr, list[ResponsePrimitive]]],
        Optional[Union[AnyStr, list[list[ResponsePrimitive]], list[ResponsePrimitive]]],
        Optional[Union[AnyStr, tuple[AnyStr, ...], ScoredMembers]],
    ]
):
    def transform(
        self,
        response: Optional[Union[AnyStr, list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, tuple[AnyStr, ...], ScoredMembers]]:
        if not (response and options.get("withscores")):
            return tuple(response) if isinstance(response, list) else response

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: Optional[Union[AnyStr, list[list[ResponsePrimitive]], list[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, tuple[AnyStr, ...], ScoredMembers]]:
        if not (response and options.get("withscores")):
            return tuple(response) if isinstance(response, list) else response

        return tuple(ScoredMember(*v) for v in response)


class BZPopCallback(
    ResponseCallback[
        Optional[list[ResponsePrimitive]],
        Optional[list[ResponsePrimitive]],
        Optional[tuple[AnyStr, AnyStr, float]],
    ]
):
    def transform(
        self, response: Optional[list[ResponsePrimitive]], **options: Optional[ValueT]
    ) -> Optional[tuple[AnyStr, AnyStr, float]]:
        if response:
            return response[0], response[1], float(response[2])
        return None


class ZAddCallback(ResponseCallback[ResponsePrimitive, Union[int, float], Union[int, float]]):
    def transform(
        self, response: ResponsePrimitive, **options: Optional[ValueT]
    ) -> Union[int, float]:
        if options.get("condition"):
            return float(response)
        return int(response)

    def transform_3(
        self, response: Union[int, float], **options: Optional[ValueT]
    ) -> Union[int, float]:
        return response
