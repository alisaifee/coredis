from __future__ import annotations

from coredis.response._callbacks import ResponseCallback
from coredis.response.types import GeoCoordinates, GeoSearchResult
from coredis.typing import AnyStr, Optional, ResponseType, Union, ValueT


class GeoSearchCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        Union[int, tuple[Union[AnyStr, GeoSearchResult], ...]],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Union[int, tuple[Union[AnyStr, GeoSearchResult], ...]]:
        if options.get("store") or options.get("storedist"):
            return response

        if not (options.get("withdist") or options.get("withcoord") or options.get("withhash")):
            return tuple(list(response))

        results: list[GeoSearchResult] = []

        for result in response:
            results.append(
                GeoSearchResult(
                    result.pop(0),
                    float(result.pop(0)) if options.get("withdist") else None,
                    result.pop(0) if options.get("withhash") else None,
                    (
                        GeoCoordinates(*map(float, result.pop(0)))
                        if options.get("withcoord")
                        else None
                    ),
                )
            )

        return tuple(results)


class GeoCoordinatessCallback(
    ResponseCallback[ResponseType, ResponseType, tuple[Optional[GeoCoordinates], ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> tuple[Optional[GeoCoordinates], ...]:
        return tuple(
            map(
                lambda ll: (GeoCoordinates(float(ll[0]), float(ll[1])) if ll is not None else None),
                response,
            )
        )
