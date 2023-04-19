from __future__ import annotations

import dataclasses
from typing import Any, NamedTuple

from coredis._json import json
from coredis.typing import (
    AnyStr,
    Dict,
    Generic,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Set,
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

    def __post_init__(self) -> None:
        for idx, result in enumerate(self.results):
            json_key = b"$" if b"$" in result else "$" if "$" in result else None
            if json_key:
                self.results[idx] = json.loads(result.pop(json_key))


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


@dataclasses.dataclass
class GraphNode(Generic[AnyStr]):
    """
    Representation of a graph node
    """

    #: The node's internal ID
    id: int
    #: A set of labels associated with the node
    labels: Set[AnyStr]
    #: Mapping of property names to values
    properties: Dict[AnyStr, ResponseType]


@dataclasses.dataclass
class GraphRelation(Generic[AnyStr]):
    """
    Representation of a relation between two nodes
    """

    #: The relationship's internal ID
    id: int
    #: Relation type
    type: AnyStr
    #: Source node ID
    src_node: int
    #: Destination node ID
    destination_node: int
    #: Mapping of all properties the relation possesses
    properties: Dict[AnyStr, ResponseType]


@dataclasses.dataclass
class GraphPath(Generic[AnyStr]):
    """
    Representation of a graph path
    """

    #: The nodes in the path
    nodes: List[GraphNode[AnyStr]]
    #: The relations in the path
    relations: List[GraphRelation[AnyStr]]

    NULL_NODE = GraphNode[AnyStr](0, set(), {})

    @property
    def path(self) -> Tuple[Union[GraphNode[AnyStr], GraphRelation[AnyStr]], ...]:
        """
        The path as a tuple of nodes and relations
        """
        if self.nodes and self.relations:
            return tuple(
                [
                    item
                    for pair in zip(self.nodes, self.relations + [self.NULL_NODE])
                    for item in pair
                ][:-1]
            )
        return ()


@dataclasses.dataclass
class GraphQueryResult(Generic[AnyStr]):
    """
    Response from `GRAPH.QUERY <https://redis.io/commands/graph.query>`__
    """

    #: List of entries in the response header
    header: Tuple[AnyStr, ...]
    #: The result set from the query
    result_set: Tuple[
        Union[
            ResponsePrimitive,
            List[
                Union[
                    ResponsePrimitive,
                    GraphNode[AnyStr],
                    GraphRelation[AnyStr],
                    GraphPath[AnyStr],
                ]
            ],
        ],
        ...,
    ]
    #: Mapping of query statistics
    stats: Dict[str, ResponsePrimitive]


class GraphSlowLogInfo(NamedTuple):
    """
    Response from `GRAPH.SLOWLOG <https://redis.io/commands/graph.slowlog>`__
    """

    #: The unix timestamp at which the logged command was processed.
    start_time: int
    #: The array composing the arguments of the command.
    command: StringT
    #: query name
    query: StringT
    #: The amount of time needed for its execution, in microseconds.
    duration: float
