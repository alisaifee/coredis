from __future__ import annotations

import asyncio
import enum
from typing import TYPE_CHECKING, Any

from coredis._utils import b, nativestr
from coredis.modules.response.types import (
    GraphNode,
    GraphPath,
    GraphQueryResult,
    GraphRelation,
    GraphSlowLogInfo,
)
from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    Generic,
    Literal,
    ResponsePrimitive,
    ResponseType,
    StringT,
)

if TYPE_CHECKING:
    from coredis.client import Client


class RedisValueTypes(enum.IntEnum):
    VALUE_UNKNOWN = 0
    VALUE_NULL = 1
    VALUE_STRING = 2
    VALUE_INTEGER = 3
    VALUE_BOOLEAN = 4
    VALUE_DOUBLE = 5
    VALUE_ARRAY = 6
    VALUE_EDGE = 7
    VALUE_NODE = 8
    VALUE_PATH = 9
    VALUE_MAP = 10
    VALUE_POINT = 11


PROCEDURE_CALLS = {
    "labels": "db.labels()",
    "relationships": "db.relationshipTypes()",
    "properties": "db.propertyKeys()",
}

SCALAR_MAPPING = {
    RedisValueTypes.VALUE_INTEGER: int,
    RedisValueTypes.VALUE_BOOLEAN: lambda v: b(v) == b"true",
    RedisValueTypes.VALUE_DOUBLE: float,
    RedisValueTypes.VALUE_STRING: lambda v: v,
    RedisValueTypes.VALUE_NULL: lambda _: None,
}


class QueryCallback(
    ResponseCallback[ResponseType, ResponseType, GraphQueryResult[AnyStr]],
    Generic[AnyStr],
):
    properties: dict[int, StringT]
    relationships: dict[int, StringT]
    labels: dict[int, StringT]

    def __init__(self, graph: StringT, **options: Any):
        self.graph = graph
        self.properties = {}
        self.relationships = {}
        self.labels = {}
        super().__init__(**options)

    async def pre_process(
        self,
        client: Client[Any],
        response: ResponseType,
    ) -> None:
        if not len(response) == 3:
            return
        result_set = response[1]
        max_label_id, max_relation_id, max_property_id = -1, -1, -1

        cache = client.callback_storage[self.__class__]
        self.labels = cache.setdefault(f"{self.graph}:labels", {})
        self.relationships = cache.setdefault(f"{self.graph}:relationships", {})
        self.properties = cache.setdefault(f"{self.graph}:properties", {})
        for row in result_set:
            for entity in row:
                max_label_id, max_relation_id, max_property_id = self.fetch_max_ids(
                    entity, max_label_id, max_relation_id, max_property_id
                )
        if any(k != -1 for k in [max_label_id, max_relation_id, max_property_id]):
            self.labels, self.relationships, self.properties = await asyncio.gather(
                self.fetch_mapping(max_label_id, "labels", client),
                self.fetch_mapping(max_relation_id, "relationships", client),
                self.fetch_mapping(max_property_id, "properties", client),
            )

    def fetch_max_ids(
        self, entity: Any, max_label_id: int, max_relation_id: int, max_property_id: int
    ) -> tuple[int, int, int]:
        result_type = entity[0]
        if result_type == RedisValueTypes.VALUE_NODE:
            for label_id in entity[1][1]:
                max_label_id = max(max_label_id, label_id)
            for property_id in [k[0] for k in entity[1][2]]:
                max_property_id = max(max_property_id, property_id)
        elif result_type == RedisValueTypes.VALUE_EDGE:
            max_relation_id = max(max_relation_id, entity[1][1])
            for property_id in [k[0] for k in entity[1][4]]:
                max_property_id = max(max_property_id, property_id)
        elif result_type == RedisValueTypes.VALUE_PATH:
            for segment in entity[1]:
                max_label_id, max_relation_id, max_property_id = self.fetch_max_ids(
                    segment, max_label_id, max_relation_id, max_property_id
                )
        elif result_type == RedisValueTypes.VALUE_ARRAY:
            for segment in entity[1]:
                max_label_id, max_relation_id, max_property_id = self.fetch_max_ids(
                    segment, max_label_id, max_relation_id, max_property_id
                )
        return max_label_id, max_relation_id, max_property_id

    async def fetch_mapping(
        self,
        max_id: int,
        type: Literal["labels", "properties", "relationships"],
        client: Client[Any],
    ) -> dict[int, StringT]:
        cache = client.callback_storage[self.__class__]
        if max_id > max(cache[f"{self.graph}:{type}"] or [-1]):
            cache[f"{self.graph}:{type}"] = dict(
                enumerate(
                    [
                        k[0]
                        for k in (
                            await client.graph.ro_query(self.graph, f"CALL {PROCEDURE_CALLS[type]}")
                        ).result_set
                    ],
                )
            )
        return cache[f"{self.graph}:{type}"]

    def transform(
        self,
        response: ResponseType,
    ) -> GraphQueryResult[AnyStr]:
        result_set = []
        headers = []
        if len(response) == 3:
            headers = [k[1] for k in response[0]]
            stats = response[2]
            for result in response[1]:
                entities = []
                for entity in result:
                    entities.append(self.parse_entity(entity))
                result_set.append(entities)
        else:
            stats = response[0]
        stats_mapping = dict(
            map(
                lambda v: int(v) if v.isalnum() else v,
                map(lambda v: v.strip(), nativestr(m).split(":")),
            )
            for m in stats
        )
        return GraphQueryResult(tuple(headers), tuple(result_set), stats_mapping)

    def parse_entity(self, entity):
        result_type = entity[0]
        if result_type in [
            RedisValueTypes.VALUE_NULL,
            RedisValueTypes.VALUE_STRING,
            RedisValueTypes.VALUE_INTEGER,
            RedisValueTypes.VALUE_BOOLEAN,
            RedisValueTypes.VALUE_DOUBLE,
        ]:
            return SCALAR_MAPPING[result_type](entity[1])
        elif result_type == RedisValueTypes.VALUE_MAP:
            it = iter(entity[1])
            return dict(zip(it, map(self.parse_entity, it)))
        elif result_type == RedisValueTypes.VALUE_ARRAY:
            return [self.parse_entity(k) for k in entity[1]]
        elif result_type == RedisValueTypes.VALUE_POINT:
            return tuple(map(float, entity[1]))
        elif result_type == RedisValueTypes.VALUE_EDGE:
            return GraphRelation(
                id=entity[1][0],
                type=self.relationships[entity[1][1]],
                src_node=entity[1][2],
                destination_node=entity[1][3],
                properties={
                    self.properties[k[0]]: self.parse_entity((k[1], k[2])) for k in entity[1][4]
                },
            )
        elif result_type == RedisValueTypes.VALUE_NODE:
            return GraphNode(
                id=entity[1][0],
                labels={self.labels[k] for k in entity[1][1]},
                properties={
                    self.properties[k[0]]: self.parse_entity((k[1], k[2])) for k in entity[1][2]
                },
            )
        elif result_type == RedisValueTypes.VALUE_PATH:
            nodes, relations = entity[1]
            nodes = self.parse_entity(nodes)
            relations = self.parse_entity(relations)
            return GraphPath(nodes, relations)


class GraphSlowLogCallback(
    ResponseCallback[ResponseType, ResponseType, tuple[GraphSlowLogInfo, ...]]
):
    def transform(
        self,
        response: ResponseType,
    ) -> tuple[GraphSlowLogInfo, ...]:
        return tuple(GraphSlowLogInfo(int(k[0]), k[1], k[2], float(k[3])) for k in response)


class ConfigGetCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        ResponsePrimitive | dict[AnyStr, ResponsePrimitive],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> ResponsePrimitive | dict[AnyStr, ResponsePrimitive]:
        if isinstance(response, list):
            if isinstance(response[0], list):
                return dict(response)
            else:
                return response[1]
