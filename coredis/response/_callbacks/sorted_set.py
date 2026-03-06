from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ScoredMember, ScoredMembers
from coredis.typing import (
    AnyStr,
    Generic,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


class ZRankCallback(
    ResponseCallback[
        int | list[ResponsePrimitive] | None,
        int | tuple[int, float] | None,
    ],
):
    def transform(
        self,
        response: int | list[ResponsePrimitive] | None,
    ) -> int | tuple[int, float] | None:
        if response is not None:
            if self.options.get("withscore"):
                return cast(tuple[int, float], tuple(cast(list[ResponsePrimitive], response)[:2]))
            else:
                return cast(int, response)
        return response


class ZMembersOrScoredMembers(
    ResponseCallback[
        list[AnyStr | list[ResponsePrimitive]],
        tuple[AnyStr | ScoredMember, ...],
    ],
):
    def transform(
        self,
        response: list[AnyStr | list[ResponsePrimitive]],
    ) -> tuple[AnyStr | ScoredMember, ...]:
        if self.options.get("withscores"):
            return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))
        else:
            return cast(tuple[AnyStr, ...], tuple(response))


class ZSetScorePairCallback(
    ResponseCallback[
        list[ResponsePrimitive | list[ResponsePrimitive]] | None,
        ScoredMember | ScoredMembers | None,
    ],
    Generic[AnyStr],
):
    def transform(
        self,
        response: list[ResponsePrimitive | list[ResponsePrimitive]] | None,
    ) -> ScoredMember | ScoredMembers | None:
        if not response:
            return None

        if not (self.options.get("withscores") or self.options.get("count")):
            return ScoredMember(*cast(tuple[AnyStr, float], response))

        return tuple(ScoredMember(*v) for v in cast(list[tuple[AnyStr, float]], response))


class ZMPopCallback(
    ResponseCallback[
        list[ResponseType] | None,
        tuple[AnyStr, ScoredMembers] | None,
    ],
    Generic[AnyStr],
):
    def transform(
        self,
        response: list[ResponseType] | None,
    ) -> tuple[AnyStr, ScoredMembers] | None:
        r = cast(tuple[AnyStr, list[tuple[AnyStr, int]]], response)
        if r:
            return r[0], tuple(ScoredMember(v[0], float(v[1])) for v in r[1])

        return None


class ZScanCallback(
    ResponseCallback[list[ResponseType], tuple[int, ScoredMembers]],
    Generic[AnyStr],
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[int, ScoredMembers]:
        cursor, r = cast(tuple[int, list[AnyStr]], response)
        it = iter(r)
        return int(cursor), tuple(ScoredMember(*v) for v in zip(it, map(float, it)))


class ZRandMemberCallback(
    ResponseCallback[
        AnyStr | list[list[StringT | float]] | list[StringT] | None,
        AnyStr | tuple[AnyStr, ...] | ScoredMembers | None,
    ]
):
    def transform(
        self,
        response: AnyStr | list[list[StringT | float]] | list[StringT] | None,
    ) -> AnyStr | tuple[AnyStr, ...] | ScoredMembers | None:
        if response:
            if not isinstance(response, list):
                return response

            if self.options.get("withscores"):
                return tuple(ScoredMember(*cast(tuple[StringT, float], tuple(v))) for v in response)
            else:
                return tuple(cast(list[AnyStr], response))
        return None


class BZPopCallback(
    ResponseCallback[
        list[StringT | float] | None,
        tuple[AnyStr, AnyStr, float] | None,
    ]
):
    def transform(
        self,
        response: list[StringT | float] | None,
    ) -> tuple[AnyStr, AnyStr, float] | None:
        if response:
            return cast(AnyStr, response[0]), cast(AnyStr, response[1]), float(response[2])
        return None


class ZAddCallback(ResponseCallback[int | float, int | float]):
    def transform(
        self,
        response: int | float,
    ) -> int | float:
        return response
