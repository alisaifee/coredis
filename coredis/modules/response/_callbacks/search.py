from __future__ import annotations

from collections import OrderedDict

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
    ValueT,
)


class SearchConfigCallback(
    ResponseCallback[
        List[List[ResponsePrimitive]],
        List[List[ResponsePrimitive]],
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


class SearchResultCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        SearchResult[AnyStr],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> SearchResult[AnyStr]:
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


class AggregationResultCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        SearchAggregationResult[AnyStr],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> SearchAggregationResult:
        def try_json(value):
            if not options.get("dialect", None) == 3:
                return value
            try:
                return json.loads(value)
            except ValueError:
                return value

        return SearchAggregationResult[AnyStr](
            [
                flat_pairs_to_dict(k, try_json)
                for k in (
                    response[1:] if not options.get("with_cursor") else response[0][1:]
                )
            ],
            response[1] if options.get("with_cursor") else None,
        )


class SpellCheckResult(TypedDict):
    term: StringT
    suggestions: OrderedDict[StringT, int]


class SpellCheckCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
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
