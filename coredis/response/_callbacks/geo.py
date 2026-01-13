from __future__ import annotations

from typing import Any

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import GeoCoordinates, GeoSearchResult
from coredis.typing import AnyStr, Generic, ResponseType


class GeoSearchCallback(
    Generic[AnyStr],
    ResponseCallback[
        ResponseType,
        tuple[AnyStr | GeoSearchResult, ...],
    ],
):
    def transform(
        self, response: ResponseType, **options: Any
    ) -> tuple[AnyStr | GeoSearchResult, ...]:
        if not (
            self.options.get("withdist")
            or self.options.get("withcoord")
            or self.options.get("withhash")
        ):
            return tuple(list(response))

        results: list[GeoSearchResult] = []

        for result in response:
            results.append(
                GeoSearchResult(
                    result.pop(0),
                    float(result.pop(0)) if self.options.get("withdist") else None,
                    result.pop(0) if self.options.get("withhash") else None,
                    (
                        GeoCoordinates(*map(float, result.pop(0)))
                        if self.options.get("withcoord")
                        else None
                    ),
                )
            )

        return tuple(results)


class GeoCoordinatessCallback(ResponseCallback[ResponseType, tuple[GeoCoordinates | None, ...]]):
    def transform(
        self, response: ResponseType, **options: Any
    ) -> tuple[GeoCoordinates | None, ...]:
        return tuple(
            map(
                lambda ll: (GeoCoordinates(float(ll[0]), float(ll[1])) if ll is not None else None),
                response,
            )
        )
