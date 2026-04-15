from __future__ import annotations

from typing import Any, cast

from coredis.response._callbacks import (
    ClusterMergeMapping,
    ResponseCallback,
)
from coredis.typing import (
    AnyStr,
    StringT,
)


class SampleCallback(
    ResponseCallback[
        list[int | float],
        tuple[int, float] | tuple[()],
    ]
):
    def transform(
        self,
        response: list[int | float],
    ) -> tuple[int, float] | tuple[()]:
        return (int(response[0]), float(response[1])) if response else ()


class SamplesCallback(
    ResponseCallback[
        list[list[int | float]] | None,
        tuple[tuple[int, float], ...] | tuple[()],
    ],
):
    def transform(
        self,
        response: list[list[int | float]] | None,
    ) -> tuple[tuple[int, float], ...] | tuple[()]:
        if response:
            return tuple(cast(tuple[int, float], SampleCallback().transform(r)) for r in response)
        return ()


class TimeSeriesCallback(
    ResponseCallback[
        dict[StringT, list[dict[StringT, StringT] | list[int | float]]],
        dict[AnyStr, tuple[dict[AnyStr, AnyStr], tuple[int, float] | tuple[()]]],
    ]
):
    def transform(
        self,
        response: dict[StringT, list[dict[StringT, StringT] | list[int | float]]],
    ) -> dict[AnyStr, tuple[dict[AnyStr, AnyStr], tuple[int, float] | tuple[()]]]:
        return {
            cast(AnyStr, k): (
                cast(dict[AnyStr, AnyStr], v[0]),
                cast(tuple[int, float] | tuple[()], tuple(v[1])),
            )
            for k, v in response.items()
        }


class TimeSeriesMultiCallback(
    ResponseCallback[
        dict[StringT, list[dict[StringT, StringT | list[StringT]] | list[list[int | float]]]],
        dict[
            AnyStr,
            tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
        ],
    ]
):
    def transform(
        self,
        response: dict[
            StringT, list[dict[StringT, StringT | list[StringT]] | list[list[int | float]]]
        ],
    ) -> dict[
        AnyStr,
        tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
    ]:
        return {
            cast(AnyStr, k): (
                cast(dict[AnyStr, AnyStr], r[0]),
                tuple(
                    cast(tuple[int, float], SampleCallback().transform(cast(list[int | float], t)))
                    for t in r[-1]
                ),
            )
            for k, r in response.items()
        }


class ClusterMergeTimeSeries(ClusterMergeMapping[AnyStr, tuple[Any, ...]]):
    def __init__(self) -> None:
        super().__init__(value_combine=self.merge)

    def merge(self, values: Any) -> tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...]]:
        merged_labels: dict[AnyStr, AnyStr] = {}
        merged_series: tuple[tuple[int, float], ...] = ()
        for value in values:
            merged_labels.update(value[0])
            merged_series = merged_series + value[1]
        return merged_labels, tuple(merged_series)
