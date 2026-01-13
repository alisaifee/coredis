from __future__ import annotations

from typing import Any, cast

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import (
    ClusterMergeMapping,
    DictCallback,
    ResponseCallback,
)
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
    Sequence,
)


class SampleCallback(
    ResponseCallback[
        list[RedisValueT],
        tuple[int, float] | tuple[()],
    ]
):
    def transform(
        self,
        response: list[RedisValueT],
    ) -> tuple[int, float] | tuple[()]:
        return (int(response[0]), float(response[1])) if response else ()


class SamplesCallback(
    ResponseCallback[
        list[list[RedisValueT]] | None,
        tuple[tuple[int, float], ...] | tuple[()],
    ],
):
    def transform(
        self,
        response: list[list[RedisValueT]] | None,
    ) -> tuple[tuple[int, float], ...] | tuple[()]:
        if response:
            return tuple(cast(tuple[int, float], SampleCallback().transform(r)) for r in response)
        return ()


class TimeSeriesInfoCallback(DictCallback[AnyStr, ResponseType]):
    def transform(
        self,
        response: Sequence[ResponseType] | dict[ResponsePrimitive, ResponseType],
    ) -> dict[AnyStr, ResponseType]:
        dct = EncodingInsensitiveDict(super().transform(response))
        if "labels" in dct:
            dct["labels"] = dict(dct["labels"])
        if "Chunks" in dct:
            dct["Chunks"] = [flat_pairs_to_dict(chunk) for chunk in dct["Chunks"]]
        if "rules" in dct and not isinstance(dct["rules"], dict):
            dct["rules"] = {rule[0]: rule[1:] for rule in dct["rules"]}

        return dict(dct)


class TimeSeriesCallback(
    ResponseCallback[
        ResponseType,
        dict[AnyStr, tuple[dict[AnyStr, AnyStr], tuple[int, float] | tuple[()]]],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[AnyStr, tuple[dict[AnyStr, AnyStr], tuple[int, float] | tuple[()]]]:
        if isinstance(response, dict):
            return {k: (v[0], tuple(v[1])) for k, v in response.items()}
        else:
            return {
                r[0]: (dict(r[1]), (r[2][0], float(r[2][1])) if r[2] else tuple()) for r in response
            }


class TimeSeriesMultiCallback(
    ResponseCallback[
        ResponseType,
        dict[
            AnyStr,
            tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
        ],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[
        AnyStr,
        tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
    ]:
        if isinstance(response, dict):
            if self.options.get("grouped"):
                return {
                    k: (
                        r[0],
                        tuple(SampleCallback().transform(t) for t in r[-1]),
                    )
                    for k, r in response.items()
                }
            else:
                return {
                    k: (
                        r[0],
                        tuple(SampleCallback().transform(t) for t in r[-1]),
                    )
                    for k, r in response.items()
                }
        else:
            if self.options.get("grouped"):
                return {
                    r[0]: (
                        flat_pairs_to_dict(r[1][0]) if r[1] else {},
                        tuple(SampleCallback().transform(t) for t in r[2]),
                    )
                    for r in cast(Any, response)
                }
            else:
                return {
                    r[0]: (
                        dict(r[1]),
                        tuple(SampleCallback().transform(t) for t in r[2]),
                    )
                    for r in cast(Any, response)
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
