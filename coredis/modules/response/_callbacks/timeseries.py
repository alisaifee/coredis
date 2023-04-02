from __future__ import annotations

from typing import (
    Any,
    AnyStr,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import (
    ClusterMergeMapping,
    DictCallback,
    ResponseCallback,
)
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import ResponsePrimitive, ResponseType, ValueT


class SampleCallback(
    ResponseCallback[
        List[ValueT],
        List[ValueT],
        Union[Tuple[int, float], Tuple[()]],
    ]
):
    def transform(
        self,
        response: List[ValueT],
        **options: Optional[ValueT],
    ) -> Union[Tuple[int, float], Tuple[()]]:
        return (int(response[0]), float(response[1])) if response else ()


class SamplesCallback(
    ResponseCallback[
        Optional[List[List[ValueT]]],
        Optional[List[List[ValueT]]],
        Union[Tuple[Tuple[int, float], ...], Tuple[()]],
    ],
):
    def transform(
        self,
        response: Optional[List[List[ValueT]]],
        **options: Optional[ValueT],
    ) -> Union[Tuple[Tuple[int, float], ...], Tuple[()]]:
        if response:
            return tuple(
                cast(Tuple[int, float], SampleCallback().transform(r)) for r in response
            )
        return ()


class TimeSeriesInfoCallback(DictCallback[AnyStr, ResponseType]):
    def transform(
        self,
        response: Union[Sequence[ResponseType], Dict[ResponsePrimitive, ResponseType]],
        **options: Optional[ValueT],
    ) -> Dict[AnyStr, ResponseType]:
        dct = EncodingInsensitiveDict(super().transform(response, **options))
        if "labels" in dct:
            dct["labels"] = dict(dct["labels"])
        if "Chunks" in dct:
            dct["Chunks"] = [flat_pairs_to_dict(chunk) for chunk in dct["Chunks"]]
        return dict(dct)


class TimeSeriesCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        Dict[AnyStr, Tuple[Dict[AnyStr, AnyStr], Tuple[int, float]]],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, Tuple[Dict[AnyStr, AnyStr], Tuple[int, float]]]:
        return {
            r[0]: (dict(r[1]), (r[2][0], float(r[2][1])) if r[2] else tuple())
            for r in response
        }


class TimeSeriesMultiCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        Dict[
            AnyStr,
            Tuple[
                Dict[AnyStr, AnyStr], Union[Tuple[Tuple[int, float], ...], Tuple[()]]
            ],
        ],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[
        AnyStr,
        Tuple[Dict[AnyStr, AnyStr], Union[Tuple[Tuple[int, float], ...], Tuple[()]]],
    ]:
        if options.get("grouped"):
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


class ClusterMergeTimeSeries(ClusterMergeMapping[AnyStr, Tuple[Any, ...]]):
    def __init__(self) -> None:
        self.value_combine = self.merge

    def combine(
        self,
        responses: Mapping[str, Dict[AnyStr, Tuple[Any, ...]]],
        **kwargs: Optional[ValueT],
    ) -> Dict[AnyStr, Tuple[Any, ...]]:
        if not kwargs.get("grouped"):
            return super().combine(responses, **kwargs)
        raise NotImplementedError(
            "Unable to merge response from multiple cluster nodes when used with grouping"
        )

    def merge(
        self, values: Any
    ) -> Tuple[Dict[AnyStr, AnyStr], Tuple[Tuple[int, float], ...]]:
        merged_labels: Dict[AnyStr, AnyStr] = {}
        merged_series: Tuple[Tuple[int, float], ...] = ()
        for value in values:
            merged_labels.update(value[0])
            merged_series = merged_series + value[1]
        return merged_labels, tuple(merged_series)
