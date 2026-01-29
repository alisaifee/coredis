from __future__ import annotations

import dataclasses

from coredis._json import json
from coredis.typing import (
    AnyStr,
    Generic,
    ResponseType,
    StringT,
)


@dataclasses.dataclass
class SearchDocument(Generic[AnyStr]):
    """
    Search document as returned by `FT.SEARCH <https://redis.io/commands/ft.search>`__
    """

    #: Document id
    id: StringT
    #: Search score if the :paramref:`~coredis.modules.search.Search.search.withscores`
    #: option was used
    score: float | None
    #: Explanation of the score if the
    #:  :paramref:`~coredis.modules.search.Search.search.explainscore` option was used
    score_explanation: list[AnyStr] | None
    #: Payload associated with the document if
    #:  :paramref:`~coredis.modules.search.Search.search.withpayloads` was used
    payload: StringT | None
    sortkeys: StringT | None
    #: Mapping of properties returned for the document
    properties: dict[AnyStr, ResponseType]


@dataclasses.dataclass
class SearchResult(Generic[AnyStr]):
    """
    Search results as returned by `FT.SEARCH <https://redis.io/commands/ft.search>`__
    """

    #: The total number of results found for the query
    total: int
    #: The documents returned by the query
    documents: tuple[SearchDocument[AnyStr], ...]


@dataclasses.dataclass
class SearchAggregationResult(Generic[AnyStr]):
    """
    Search aggregations as returned by `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`__
    """

    #: The aggregation results
    results: list[dict[StringT, ResponseType]]
    #: The cursor id if :paramref:`~coredis.modules.search.aggregate.with_cursor` was `True`
    cursor: int | None

    def __post_init__(self) -> None:
        for idx, result in enumerate(self.results):
            json_key = b"$" if b"$" in result else "$" if "$" in result else None
            if json_key:
                self.results[idx] = json.loads(result.pop(json_key))


@dataclasses.dataclass
class HybridResult(Generic[AnyStr]):
    total_results: int
    execution_time: float
    warnings: list[StringT]
    results: list[dict[StringT, StringT]]


@dataclasses.dataclass
class AutocompleteSuggestion(Generic[AnyStr]):
    """
    Autocomplete suggestion as returned by `FT.SUGGET <https://redis.io/commands/ft.sugget>`__
    """

    #: the suggestion string
    string: AnyStr
    #: the score of the suggestion if
    #:  :paramref:`~coredis.modules.autocomplete.Autocomplete.sugget.withscores` was used
    score: float | None
    #: the payload associated with the suggestion if
    #:  :paramref:`~coredis.modules.autocomplete.Autocomplete.sugget.withpayloads` was used
    payload: AnyStr | None
