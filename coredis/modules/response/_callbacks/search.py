from __future__ import annotations

from collections import ChainMap, OrderedDict
from functools import partial

from coredis._json import json
from coredis._utils import EncodingInsensitiveDict
from coredis.modules.response.types import (
    HybridResult,
    SearchAggregationResult,
    SearchDocument,
    SearchResult,
)
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
)


class SearchConfigCallback(
    ResponseCallback[
        dict[AnyStr, ResponseType] | list[list[ResponsePrimitive]],
        dict[AnyStr, ResponsePrimitive],
    ]
):
    def transform(
        self,
        response: dict[AnyStr, ResponseType] | list[list[ResponsePrimitive]],
    ) -> dict[AnyStr, ResponsePrimitive]:
        if isinstance(response, list):
            command_arguments = []
            for item in response:
                try:
                    v = (item[0], json.loads(item[1]))
                except (ValueError, TypeError):
                    v = item
                command_arguments.append(v)
            return dict(command_arguments)
        else:
            config = {}
            for item, value in response.items():
                try:
                    config[item] = json.loads(value)
                except (ValueError, TypeError):
                    config[item] = value
            return config


class SearchResultCallback(
    ResponseCallback[
        list[ResponseType] | dict[AnyStr, ResponseType],
        SearchResult[AnyStr],
    ]
):
    def transform(
        self,
        response: list[ResponseType] | dict[AnyStr, ResponseType],
    ) -> SearchResult[AnyStr]:
        results = []
        if isinstance(response, list):
            if self.options.get("nocontent"):
                return SearchResult[AnyStr](
                    response[0],
                    tuple(SearchDocument(i, None, None, None, None, {}) for i in response[1:]),
                )
            step = 2
            results = []
            score_idx = payload_idx = sort_key_idx = 0
            if self.options.get("withscores"):
                score_idx = 1
                step += 1
            if self.options.get("withpayloads"):
                payload_idx = score_idx + 1
                step += 1
            if self.options.get("withsortkeys"):
                sort_key_idx = payload_idx + 1
                step += 1

            for k in range(1, len(response) - 1, step):
                section = response[k : k + step]
                score_explain = None
                if self.options.get("explainscore"):
                    score = section[score_idx][0]
                    score_explain = section[score_idx][1]
                else:
                    score = section[score_idx] if score_idx else None
                fields = EncodingInsensitiveDict(flat_pairs_to_dict(section[-1]))
                if "$" in fields:
                    fields = json.loads(fields.pop("$"))
                results.append(
                    SearchDocument(
                        section[0],
                        float(score) if score else None,
                        score_explain,
                        section[payload_idx] if payload_idx else None,
                        section[sort_key_idx] if sort_key_idx else None,
                        fields,
                    )
                )
            return SearchResult[AnyStr](response[0], tuple(results))
        else:
            response = EncodingInsensitiveDict(response)
            for result in response["results"]:
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
                        fields,
                    )
                )
            return SearchResult[AnyStr](response["total_results"], tuple(results))


class AggregationResultCallback(
    ResponseCallback[
        dict[AnyStr, ResponseType] | list[ResponseType],
        SearchAggregationResult[AnyStr],
    ]
):
    def transform(
        self,
        response: dict[AnyStr, ResponseType] | list[ResponseType],
    ) -> SearchAggregationResult:
        if (
            self.options.get("with_cursor")
            and isinstance(response[0], dict)
            or isinstance(response, dict)
        ):
            response, cursor = response if self.options.get("with_cursor") else (response, None)
            response = EncodingInsensitiveDict(response)
            return SearchAggregationResult[AnyStr](
                [
                    {
                        r: self.try_json(self.options, v)
                        for r, v in EncodingInsensitiveDict(k)["extra_attributes"].items()
                    }
                    for k in response["results"]
                ],
                cursor,
            )
        else:
            return SearchAggregationResult[AnyStr](
                [
                    flat_pairs_to_dict(k, partial(self.try_json, self.options))
                    for k in (
                        response[1:] if not self.options.get("with_cursor") else response[0][1:]
                    )
                ],
                response[1] if self.options.get("with_cursor") else None,
            )

    @staticmethod
    def try_json(options, value):
        if not options.get("dialect", None) == 3:
            return value
        try:
            return json.loads(value)
        except ValueError:
            return value


class SpellCheckCallback(
    ResponseCallback[
        dict[AnyStr, ResponseType] | list[ResponseType],
        dict[AnyStr, OrderedDict[AnyStr, float]],
    ]
):
    def transform(
        self,
        response: dict[AnyStr, ResponseType] | list[ResponseType],
    ) -> dict[AnyStr, OrderedDict[AnyStr, float]]:
        # For older versions of redis search that didn't support RESP3
        if isinstance(response, list):
            return {
                result[1]: OrderedDict(
                    (suggestion[1], float(suggestion[0])) for suggestion in result[2]
                )
                for result in response
            }
        response = EncodingInsensitiveDict(response)
        return {key: OrderedDict(ChainMap(*result)) for key, result in response["results"].items()}


class HybridSearchCallback(ResponseCallback[dict[AnyStr, ResponseType], HybridResult]):
    def transform(self, response: dict[AnyStr, ResponseType]) -> HybridResult:
        response = EncodingInsensitiveDict(response)
        return HybridResult(
            response["total_results"],
            response["execution_time"],
            response["warnings"],
            response["results"],
        )
