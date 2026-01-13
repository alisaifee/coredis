from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ScoredMember, ScoredMembers
from coredis.typing import (
    AnyStr,
    Generic,
    ResponsePrimitive,
    ResponseType,
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
        if self.options.get("withscore"):
            return (response[0], response[1]) if response else None
        else:
            return cast(int | None, response)


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


class ZMScoreCallback(ResponseCallback[list[ResponsePrimitive], tuple[float | None, ...]]):
    def transform(
        self,
        response: list[ResponsePrimitive],
    ) -> tuple[float | None, ...]:
        return tuple(score if score is None else float(score) for score in response)


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
        return int(cursor), tuple(
            ScoredMember(*cast(tuple[AnyStr, float], v)) for v in zip(it, map(float, it))
        )


class ZRandMemberCallback(
    ResponseCallback[
        AnyStr | list[list[ResponsePrimitive]] | list[ResponsePrimitive] | None,
        AnyStr | tuple[AnyStr, ...] | ScoredMembers | None,
    ]
):
    def transform(
        self,
        response: AnyStr | list[list[ResponsePrimitive]] | list[ResponsePrimitive] | None,
    ) -> AnyStr | tuple[AnyStr, ...] | ScoredMembers | None:
        if not (response and self.options.get("withscores")):
            return tuple(response) if isinstance(response, list) else response

        return tuple(ScoredMember(*v) for v in response)


class BZPopCallback(
    ResponseCallback[
        list[ResponsePrimitive] | None,
        tuple[AnyStr, AnyStr, float] | None,
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive] | None,
    ) -> tuple[AnyStr, AnyStr, float] | None:
        if response:
            return response[0], response[1], float(response[2])
        return None


class ZAddCallback(ResponseCallback[int | float, int | float]):
    def transform(
        self,
        response: int | float,
    ) -> int | float:
        return response
