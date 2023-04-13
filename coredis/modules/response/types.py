from __future__ import annotations

import dataclasses
from typing import Any

from coredis.typing import (
    AnyStr,
    Dict,
    Generic,
    List,
    Optional,
    ResponseType,
    StringT,
    Tuple,
    Union,
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
    score: Optional[float]
    #: Explanation of the score if the
    #:  :paramref:`~coredis.modules.search.Search.search.explainscore` option was used
    score_explanation: Optional[List[AnyStr]]
    #: Payload associated with the document if
    #:  :paramref:`~coredis.modules.search.Search.search.withpayloads` was used
    payload: Optional[StringT]
    sortkeys: Optional[StringT]
    #: Mapping of properties returned for the document
    properties: Dict[AnyStr, ResponseType]


@dataclasses.dataclass
class SearchResult(Generic[AnyStr]):
    """
    Search results as returned by `FT.SEARCH <https://redis.io/commands/ft.search>`__
    """

    #: The total number of results found for the query
    total: int
    #: The documents returned by the query
    documents: Tuple[SearchDocument[AnyStr], ...]


@dataclasses.dataclass
class SearchAggregationResult(Generic[AnyStr]):
    """
    Search aggregations as returned by `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`__
    """

    #: The aggregation results
    results: List[Dict[StringT, ResponseType]]
    #: The cursor id if :paramref:`~coredis.modules.search.aggregate.with_cursor` was `True`
    cursor: Optional[int]


@dataclasses.dataclass
class AutocompleteSuggestion(Generic[AnyStr]):
    """
    Autocomplete suggestion as returned by `FT.SUGGET <https://redis.io/commands/ft.sugget>`__
    """

    #: the suggestion string
    string: AnyStr
    #: the score of the suggestion if
    #:  :paramref:`~coredis.modules.autocomplete.Autocomplete.sugget.withscores` was used
    score: Optional[float]
    #: the payload associated with the suggestion if
    #:  :paramref:`~coredis.modules.autocomplete.Autocomplete.sugget.withpayloads` was used
    payload: Optional[AnyStr]


#: Type alias for valid python types that can be represented as json
JsonType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
