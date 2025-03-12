from __future__ import annotations

from typing import SupportsFloat, cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ScoredMember, ScoredMembers
from coredis.typing import (
    AnyStr,
    Generic,
    ResponsePrimitive,
    ResponseType,
    ValueT,
)


class ZRankCallback(
    ResponseCallback[
        int | list[ResponsePrimitive] | None,
        int | list[ResponsePrimitive] | None,
        int | tuple[int, float] | None,
    ],
):
    def transform(
        self,
        response: int | list[ResponsePrimitive] | None,
        **options: ValueT | None,
    ) -> int | tuple[int, float] | None:
        if options.get("withscore"):
            return (response[0], float(response[1])) if response else None
        else:
            return cast(int | None, response)

    def transform_3(
        self,
        response: int | list[ResponsePrimitive] | None,
        **options: ValueT | None,
    ) -> int | tuple[int, float] | None:
        if options.get("withscore"):
            return (response[0], response[1]) if response else None
        else:
            return cast(int | None, response)


class ZMembersOrScoredMembers(
    ResponseCallback[
        list[AnyStr | list[ResponsePrimitive]],
        list[AnyStr | list[ResponsePrimitive]],
        tuple[AnyStr | ScoredMember, ...],
    ],
):
    def transform(
        self,
        response: list[AnyStr | list[ResponsePrimitive]],
        **options: ValueT | None,
    ) -> tuple[AnyStr | ScoredMember, ...]:
        if not response:
            return ()
        elif options.get("withscores"):
            it = iter(cast(list[AnyStr], response))
            return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))
        else:
            return cast(tuple[AnyStr, ...], tuple(response))

    def transform_3(
        self,
        response: list[AnyStr | list[ResponsePrimitive]],
        **options: ValueT | None,
    ) -> tuple[AnyStr | ScoredMember, ...]:
        if options.get("withscores"):
            return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))
        else:
            return cast(tuple[AnyStr, ...], tuple(response))


class ZSetScorePairCallback(
    ResponseCallback[
        list[ResponsePrimitive] | None,
        list[ResponsePrimitive | list[ResponsePrimitive]] | None,
        ScoredMember | ScoredMembers | None,
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: list[ResponsePrimitive] | None, **options: ValueT | None
    ) -> ScoredMember | ScoredMembers | None:
        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(cast(AnyStr, response[0]), float(cast(SupportsFloat, response[1])))

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: list[ResponsePrimitive | list[ResponsePrimitive]] | None,
        **options: ValueT | None,
    ) -> ScoredMember | ScoredMembers | None:
        if not response:
            return None

        if not (options.get("withscores") or options.get("count")):
            return ScoredMember(*cast(tuple[AnyStr, float], response))

        return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))


class ZMPopCallback(
    ResponseCallback[
        list[ResponseType] | None,
        list[ResponseType] | None,
        tuple[AnyStr, ScoredMembers] | None,
    ],
    Generic[AnyStr],
):
    def transform(
        self, response: list[ResponseType] | None, **options: ValueT | None
    ) -> tuple[AnyStr, ScoredMembers] | None:
        r = cast(tuple[AnyStr, list[tuple[AnyStr, int]]], response)
        if r:
            return r[0], tuple(ScoredMember(v[0], float(v[1])) for v in r[1])

        return None


class ZMScoreCallback(
    ResponseCallback[list[ResponsePrimitive], list[ResponsePrimitive], tuple[float | None, ...]]
):
    def transform(
        self, response: list[ResponsePrimitive], **options: ValueT | None
    ) -> tuple[float | None, ...]:
        return tuple(score if score is None else float(score) for score in response)


class ZScanCallback(
    ResponseCallback[list[ResponseType], list[ResponseType], tuple[int, ScoredMembers]],
    Generic[AnyStr],
):
    def transform(
        self, response: list[ResponseType], **options: ValueT | None
    ) -> tuple[int, ScoredMembers]:
        cursor, r = cast(tuple[int, list[AnyStr]], response)
        it = iter(r)
        return int(cursor), tuple(
            ScoredMember(*cast(tuple[AnyStr, float], v)) for v in zip(it, map(float, it))
        )


class ZRandMemberCallback(
    ResponseCallback[
        AnyStr | list[ResponsePrimitive] | None,
        AnyStr | list[list[ResponsePrimitive]] | list[ResponsePrimitive] | None,
        AnyStr | tuple[AnyStr, ...] | ScoredMembers | None,
    ]
):
    def transform(
        self,
        response: AnyStr | list[ResponsePrimitive] | None,
        **options: ValueT | None,
    ) -> AnyStr | tuple[AnyStr, ...] | ScoredMembers | None:
        if not (response and options.get("withscores")):
            return tuple(response) if isinstance(response, list) else response

        it = iter(response)
        return tuple(ScoredMember(*v) for v in zip(it, map(float, it)))

    def transform_3(
        self,
        response: AnyStr | list[list[ResponsePrimitive]] | list[ResponsePrimitive] | None,
        **options: ValueT | None,
    ) -> AnyStr | tuple[AnyStr, ...] | ScoredMembers | None:
        if not (response and options.get("withscores")):
            return tuple(response) if isinstance(response, list) else response

        return tuple(ScoredMember(*v) for v in response)


class BZPopCallback(
    ResponseCallback[
        list[ResponsePrimitive] | None,
        list[ResponsePrimitive] | None,
        tuple[AnyStr, AnyStr, float] | None,
    ]
):
    def transform(
        self, response: list[ResponsePrimitive] | None, **options: ValueT | None
    ) -> tuple[AnyStr, AnyStr, float] | None:
        if response:
            return response[0], response[1], float(response[2])
        return None


class ZAddCallback(ResponseCallback[ResponsePrimitive, int | float, int | float]):
    def transform(self, response: ResponsePrimitive, **options: ValueT | None) -> int | float:
        if options.get("condition"):
            return float(response)
        return int(response)

    def transform_3(self, response: int | float, **options: ValueT | None) -> int | float:
        return response
