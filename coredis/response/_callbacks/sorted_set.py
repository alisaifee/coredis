from __future__ import annotations

from typing import SupportsFloat, cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ScoredMember, ScoredMembers
from coredis.typing import (
    AnyStr,
    Generic,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Tuple,
    Union,
    ValueT,
)


class ZMembersOrScoredMembers(
    ResponseCallback[
        List[Union[AnyStr, List[ResponsePrimitive]]],
        List[Union[AnyStr, List[ResponsePrimitive]]],
        Tuple[Union[AnyStr, ScoredMember], ...],
    ],
):
    def transform(
        self,
        response: List[Union[AnyStr, List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:

        if not response:
            return ()
        elif options.get("withscores"):
            it = iter(cast(List[AnyStr], response))
            return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))
        else:
            return cast(Tuple[AnyStr, ...], tuple(response))

    def transform_3(
        self,
        response: List[Union[AnyStr, List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:

        if options.get("withscores"):
            return tuple(
                ScoredMember(*v) for v in cast(List[Tuple[AnyStr, float]], response)
            )
        else:
            return cast(Tuple[AnyStr, ...], tuple(response))


class ZSetScorePairCallback(
    ResponseCallback[
        Optional[List[ResponsePrimitive]],
        Optional[List[Union[ResponsePrimitive, List[ResponsePrimitive]]]],
        Optional[Union[ScoredMember, ScoredMembers]],
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: Optional[List[ResponsePrimitive]], **options: Optional[ValueT]
    ) -> Optional[Union[ScoredMember, ScoredMembers]]:

        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(
                cast(AnyStr, response[0]), float(cast(SupportsFloat, response[1]))
            )

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: Optional[List[Union[ResponsePrimitive, List[ResponsePrimitive]]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[ScoredMember, ScoredMembers]]:

        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(*cast(Tuple[AnyStr, float], response))

        return tuple(
            ScoredMember(*v) for v in cast(List[Tuple[AnyStr, float]], response)
        )


class ZMPopCallback(
    ResponseCallback[
        Optional[List[ResponseType]],
        Optional[List[ResponseType]],
        Optional[Tuple[AnyStr, ScoredMembers]],
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: Optional[List[ResponseType]], **options: Optional[ValueT]
    ) -> Optional[Tuple[AnyStr, ScoredMembers]]:

        if r := cast(Tuple[AnyStr, List[Tuple[AnyStr, int]]], response):
            return r[0], tuple(ScoredMember(v[0], float(v[1])) for v in r[1])

        return None


class ZMScoreCallback(
    ResponseCallback[
        List[ResponsePrimitive], List[ResponsePrimitive], Tuple[Optional[float], ...]
    ]
):
    def transform(
        self, response: List[ResponsePrimitive], **options: Optional[ValueT]
    ) -> Tuple[Optional[float], ...]:

        return tuple(score if score is None else float(score) for score in response)


class ZScanCallback(
    ResponseCallback[List[ResponseType], List[ResponseType], Tuple[int, ScoredMembers]],
    Generic[AnyStr],
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Tuple[int, ScoredMembers]:

        cursor, r = cast(Tuple[int, List[AnyStr]], response)
        it = iter(r)
        return int(cursor), tuple(
            ScoredMember(*cast(Tuple[AnyStr, float], v))
            for v in zip(it, map(float, it))
        )


class ZRandMemberCallback(
    ResponseCallback[
        Optional[Union[AnyStr, List[ResponsePrimitive]]],
        Optional[Union[AnyStr, List[ResponsePrimitive]]],
        Optional[Union[AnyStr, ScoredMembers]],
    ]
):
    def transform(
        self,
        response: Optional[Union[AnyStr, List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, ScoredMembers]]:

        if not (response and options.get("withscores")):
            return response

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: Optional[Union[AnyStr, List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, ScoredMembers]]:

        if not (response and options.get("withscores")):
            return response

        return tuple(ScoredMember(*v) for v in response)


class BZPopCallback(
    ResponseCallback[
        Optional[List[ResponsePrimitive]],
        Optional[List[ResponsePrimitive]],
        Optional[Tuple[AnyStr, AnyStr, float]],
    ]
):
    def transform(
        self, response: Optional[List[ResponsePrimitive]], **options: Optional[ValueT]
    ) -> Optional[Tuple[AnyStr, AnyStr, float]]:
        if response:
            return response[0], response[1], float(response[2])
        return None


class ZAddCallback(
    ResponseCallback[ResponsePrimitive, Union[int, float], Union[int, float]]
):
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
