from __future__ import annotations

from typing import Any, cast

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import GeoCoordinates, GeoSearchResult
from coredis.typing import AnyStr, Generic, ResponsePrimitive, StringT


class GeoSearchCallback(
    Generic[AnyStr],
    ResponseCallback[
        list[StringT | list[ResponsePrimitive | list[ResponsePrimitive]]],
        tuple[AnyStr | GeoSearchResult, ...],
    ],
):
    def transform(
        self,
        response: list[StringT | list[ResponsePrimitive | list[ResponsePrimitive]]],
        **options: Any,
    ) -> tuple[AnyStr | GeoSearchResult, ...]:
        if not (
            self.options.get("withdist")
            or self.options.get("withcoord")
            or self.options.get("withhash")
        ):
            return tuple(cast(list[AnyStr], response))

        results: list[GeoSearchResult] = []

        for result in response:
            chunk = cast(list[list[ResponsePrimitive | list[ResponsePrimitive]]], result)
            results.append(
                GeoSearchResult(
                    cast(StringT, chunk.pop(0)),
                    float(cast(StringT, chunk.pop(0))) if self.options.get("withdist") else None,
                    cast(int, chunk.pop(0)) if self.options.get("withhash") else None,
                    (
                        GeoCoordinates(*cast(list[float], chunk.pop(0)))
                        if self.options.get("withcoord")
                        else None
                    ),
                )
            )

        return tuple(results)


class GeoCoordinatesCallback(
    ResponseCallback[list[list[float] | None], tuple[GeoCoordinates | None, ...]]
):
    def transform(
        self, response: list[list[float] | None], **options: Any
    ) -> tuple[GeoCoordinates | None, ...]:
        return tuple(
            map(
                lambda ll: GeoCoordinates(float(ll[0]), float(ll[1])) if ll is not None else None,
                response,
            )
        )
