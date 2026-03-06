from __future__ import annotations

from collections import ChainMap, OrderedDict
from typing import Any, cast

from coredis._json import json
from coredis._utils import EncodingInsensitiveDict
from coredis.modules.response.types import (
    HybridResult,
    SearchAggregationResult,
    SearchDocument,
    SearchResult,
)
from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


class SearchConfigCallback(
    ResponseCallback[
        dict[AnyStr, ResponseType],
        dict[AnyStr, ResponsePrimitive],
    ]
):
    def transform(
        self,
        response: dict[AnyStr, ResponseType],
    ) -> dict[AnyStr, ResponsePrimitive]:
        config = {}
        for item, value in response.items():
            try:
                config[item] = json.loads(value)
            except (ValueError, TypeError):
                config[item] = value
        return config


class SearchResultCallback(
    ResponseCallback[
        dict[AnyStr, ResponseType],
        SearchResult[AnyStr],
    ]
):
    def transform(
        self,
        response: dict[AnyStr, ResponseType],
    ) -> SearchResult[AnyStr]:
        results = []
        search_results = EncodingInsensitiveDict(response)
        for result in search_results["results"]:
            result = EncodingInsensitiveDict(result)
            score_explain = None
            if self.options.get("explainscore"):
                score, score_explain = result.get("score")
            else:
                score = result.get("score", None)
            fields = EncodingInsensitiveDict(result.get("extra_attributes", {}))
            if "$" in fields:
                fields = json.loads(fields.pop("$"))
            results.append(
                SearchDocument(
                    result["id"],
                    float(score) if score else None,
                    score_explain,
                    result["payload"] if self.options.get("withpayloads") else None,
                    result["sortkey"] if self.options.get("withsortkeys") else None,
                    cast(dict[Any, ResponseType], fields),
                )
            )
        return SearchResult[AnyStr](search_results["total_results"], tuple(results))


class AggregationResultCallback(
    ResponseCallback[
        dict[StringT, ResponseType] | list[dict[StringT, ResponseType] | int],
        SearchAggregationResult[AnyStr],
    ]
):
    def transform(
        self, response: dict[StringT, ResponseType] | list[dict[StringT, ResponseType] | int]
    ) -> SearchAggregationResult[AnyStr]:
        if self.options.get("with_cursor"):
            agg, cursor = cast(tuple[dict[AnyStr, ResponseType], int], tuple(response))
        else:
            agg = cast(dict[AnyStr, ResponseType], response)
            cursor = None
        aggregations = EncodingInsensitiveDict(agg)
        return SearchAggregationResult[AnyStr](
            [
                {
                    r: self.try_json(self.options, v)
                    for r, v in EncodingInsensitiveDict(k)["extra_attributes"].items()
                }
                for k in aggregations["results"]
            ],
            cursor,
        )

    @staticmethod
    def try_json(options: dict[str, Any], value: StringT) -> ResponseType:
        if not options.get("dialect", None) == 3:
            return cast(ResponseType, value)
        try:
            return cast(ResponseType, json.loads(value))
        except ValueError:
            return cast(ResponseType, value)


class SpellCheckCallback(
    ResponseCallback[
        dict[StringT, ResponseType],
        dict[AnyStr, OrderedDict[AnyStr, float]],
    ]
):
    def transform(
        self,
        response: dict[StringT, ResponseType],
    ) -> dict[AnyStr, OrderedDict[AnyStr, float]]:
        corrections = EncodingInsensitiveDict(response)
        return {
            key: OrderedDict(ChainMap(*result)) for key, result in corrections["results"].items()
        }


class HybridSearchCallback(
    ResponseCallback[
        dict[StringT, int | float | list[StringT | dict[StringT, StringT]]], HybridResult[AnyStr]
    ]
):
    def transform(
        self,
        response: dict[StringT, int | float | list[StringT | dict[StringT, StringT]]],
    ) -> HybridResult[AnyStr]:
        results = EncodingInsensitiveDict(response)
        return HybridResult[AnyStr](
            cast(int, results["total_results"]),
            cast(float, results["execution_time"]),
            cast(list[AnyStr], results["warnings"]),
            cast(list[dict[AnyStr, AnyStr]], results["results"]),
        )
