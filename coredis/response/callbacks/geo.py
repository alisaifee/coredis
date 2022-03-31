from __future__ import annotations

from coredis.commands import ResponseCallback
from coredis.response.types import GeoCoordinates, GeoSearchResult
from coredis.typing import Any, List, Optional, Tuple, Union


class GeoSearchCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Union[int, Tuple[Union[str, GeoSearchResult], ...]]:
        if options.get("store") or options.get("storedist"):
            return response

        response_list = response if isinstance(response, list) else [response]

        if not (options["withdist"] or options["withcoord"] or options["withhash"]):
            return tuple(list(response_list))

        results: List[GeoSearchResult] = []

        for result in response_list:
            results.append(
                GeoSearchResult(
                    result.pop(0),
                    float(result.pop(0)) if options.get("withdist") else None,
                    result.pop(0) if options.get("withhash") else None,
                    GeoCoordinates(*map(float, result.pop(0)))
                    if options.get("withcoord")
                    else None,
                )
            )

        return tuple(results)


class GeoCoordinatessCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Tuple[Optional[GeoCoordinates], ...]:
        return tuple(
            map(
                lambda ll: (
                    GeoCoordinates(float(ll[0]), float(ll[1]))
                    if ll is not None
                    else None
                ),
                response,
            )
        )
