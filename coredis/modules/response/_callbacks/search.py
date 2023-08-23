from __future__ import annotations

from collections import ChainMap, OrderedDict
from functools import partial

from coredis._json import json
from coredis._utils import EncodingInsensitiveDict
from coredis.modules.response.types import (
    SearchAggregationResult,
    SearchDocument,
    SearchResult,
)
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    StringT,
    TypedDict,
    Union,
    ValueT,
)


class SearchConfigCallback(
    ResponseCallback[
        List[List[ResponsePrimitive]],
        Union[Dict[AnyStr, ResponseType], List[List[ResponsePrimitive]]],
        Dict[AnyStr, ResponsePrimitive],
    ]
):
    def transform(
        self, response: List[List[ResponsePrimitive]], **options: Optional[ValueT]
    ) -> Dict[AnyStr, ResponsePrimitive]:
        pieces = []
        for item in response:
            try:
                v = (item[0], json.loads(item[1]))
            except (ValueError, TypeError):
                v = item
            pieces.append(v)
        return dict(pieces)

    def transform_3(
        self,
        response: Union[Dict[AnyStr, ResponseType], List[List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Dict[AnyStr, ResponsePrimitive]:
        if isinstance(response, list):
            return self.transform(response, **options)
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
        List[ResponseType],
        Union[List[ResponseType], Dict[AnyStr, ResponseType]],
        SearchResult[AnyStr],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> SearchResult[AnyStr]:
        if options.get("nocontent"):
            return SearchResult[AnyStr](
                response[0],
                tuple(
                    SearchDocument(i, None, None, None, None, {}) for i in response[1:]
                ),
            )
        step = 2
        results = []
        score_idx = payload_idx = sort_key_idx = 0
        if options.get("withscores"):
            score_idx = 1
            step += 1
        if options.get("withpayloads"):
            payload_idx = score_idx + 1
            step += 1
        if options.get("withsortkeys"):
            sort_key_idx = payload_idx + 1
            step += 1

        for k in range(1, len(response) - 1, step):
            section = response[k : k + step]
            score_explain = None
            if options.get("explainscore"):
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

    def transform_3(
        self,
        response: Union[List[ResponseType], Dict[AnyStr, ResponseType]],
        **options: Optional[ValueT],
    ) -> SearchResult[AnyStr]:
        results = []
        if isinstance(response, list):
            return self.transform(response, **options)
        else:
            response = EncodingInsensitiveDict(response)
            for result in response["results"]:
                result = EncodingInsensitiveDict(result)
                score_explain = None
                if options.get("explainscore"):
                    score = result["score"][0]
                    score_explain = result["score"][1]
                else:
                    score = result["score"]
                fields = EncodingInsensitiveDict(result["extra_attributes"])
                if "$" in fields:
                    fields = json.loads(fields.pop("$"))
                results.append(
                    SearchDocument(
                        result["id"],
                        float(score) if score else None,
                        score_explain,
                        result["payload"] if options.get("withpayloads") else None,
                        result["sortkey"] if options.get("withsortkeys") else None,
                        fields,
                    )
                )
            return SearchResult[AnyStr](response["total_results"], tuple(results))


class AggregationResultCallback(
    ResponseCallback[
        List[ResponseType],
        Union[Dict[AnyStr, ResponseType], List[ResponseType]],
        SearchAggregationResult[AnyStr],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> SearchAggregationResult:
        return SearchAggregationResult[AnyStr](
            [
                flat_pairs_to_dict(k, partial(self.try_json, options))
                for k in (
                    response[1:] if not options.get("with_cursor") else response[0][1:]
                )
            ],
            response[1] if options.get("with_cursor") else None,
        )

    def transform_3(
        self,
        response: Union[Dict[AnyStr, ResponseType], List[ResponseType]],
        **options: Optional[ValueT],
    ) -> SearchAggregationResult:
        if (
            options.get("with_cursor")
            and isinstance(response[0], dict)
            or isinstance(response, dict)
        ):
            response, cursor = (
                response if options.get("with_cursor") else (response, None)
            )
            response = EncodingInsensitiveDict(response)
            return SearchAggregationResult[AnyStr](
                [
                    {
                        k: self.try_json(options, v)
                        for k, v in k["extra_attributes"].items()
                    }
                    for k in (response["results"])
                ],
                cursor,
            )
        else:
            return self.transform(response, **options)

    @staticmethod
    def try_json(options, value):
        if not options.get("dialect", None) == 3:
            return value
        try:
            return json.loads(value)
        except ValueError:
            return value


class SpellCheckResult(TypedDict):
    term: StringT
    suggestions: OrderedDict[StringT, int]


class SpellCheckCallback(
    ResponseCallback[
        List[ResponseType],
        Union[Dict[AnyStr, ResponseType], List[ResponseType]],
        SpellCheckResult,
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> SpellCheckResult:
        suggestions = {}
        for result in response:
            suggestions[result[1]] = OrderedDict((k[1], float(k[0])) for k in result[2])

        return suggestions

    def transform_3(
        self,
        response: Union[Dict[AnyStr, ResponseType], List[ResponseType]],
        **options: Optional[ValueT],
    ) -> SpellCheckResult:
        if isinstance(response, list):
            return self.transform(response, **options)
        else:
            return {
                key: OrderedDict(ChainMap(*result))
                for key, result in response["results"].items()
            }
