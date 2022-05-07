from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import ClusterNode, ClusterNodeDetail
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Mapping,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Tuple,
    Union,
    ValueT,
)


class ClusterLinksCallback(
    ResponseCallback[ResponseType, ResponseType, List[Dict[AnyStr, ResponsePrimitive]]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> List[Dict[AnyStr, ResponsePrimitive]]:
        transformed: List[Dict[AnyStr, ResponsePrimitive]] = []

        for item in response:
            transformed.append(flat_pairs_to_dict(item))
        return transformed

    def transform_3(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> List[Dict[AnyStr, ResponsePrimitive]]:

        return response


class ClusterInfoCallback(ResponseCallback[ResponseType, ResponseType, Dict[str, str]]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[str, str]:

        response_str = nativestr(response)
        return dict([line.split(":") for line in response_str.splitlines() if line])


class ClusterSlotsCallback(
    ResponseCallback[
        ResponseType, ResponseType, Dict[Tuple[int, int], Tuple[ClusterNode, ...]]
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[Tuple[int, int], Tuple[ClusterNode, ...]]:

        res: Dict[Tuple[int, int], Tuple[ClusterNode, ...]] = {}

        for slot_info in response:
            min_slot, max_slot = map(int, slot_info[:2])
            nodes = slot_info[2:]
            res[(min_slot, max_slot)] = tuple(self.parse_node(node) for node in nodes)
            res[(min_slot, max_slot)][0]["server_type"] = "master"

        return res

    def parse_node(self, node: List[Union[int, str]]) -> ClusterNode:
        return ClusterNode(
            host=nativestr(node[0]),
            port=int(node[1]),
            node_id=nativestr(node[2]) if len(node) > 2 else "",
            server_type="slave",
        )


class ClusterNodesCallback(
    ResponseCallback[ResponseType, ResponseType, List[ClusterNodeDetail]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> List[ClusterNodeDetail]:
        resp: Union[List[str], str]

        if isinstance(response, list):
            resp = [nativestr(row) for row in response]
        else:
            resp = nativestr(response)
        current_host = nativestr(options.get("current_host", ""))

        def parse_slots(s: str) -> Tuple[List[int], List[Dict[str, ValueT]]]:
            slots: List[int] = []
            migrations: List[Dict[str, ValueT]] = []

            for r in s.split(" "):
                if "->-" in r:
                    slot_id, dst_node_id = r[1:-1].split("->-", 1)
                    migrations.append(
                        {
                            "slot": int(slot_id),
                            "node_id": dst_node_id,
                            "state": "migrating",
                        }
                    )
                elif "-<-" in r:
                    slot_id, src_node_id = r[1:-1].split("-<-", 1)
                    migrations.append(
                        {
                            "slot": int(slot_id),
                            "node_id": src_node_id,
                            "state": "importing",
                        }
                    )
                elif "-" in r:
                    start, end = r.split("-")
                    slots.extend(list(range(int(start), int(end) + 1)))
                else:
                    slots.append(int(r))

            return slots, migrations

        if isinstance(resp, str):
            resp = resp.splitlines()

        nodes: List[ClusterNodeDetail] = []

        for line in resp:
            parts = line.split(" ", 8)
            (
                self_id,
                addr,
                flags,
                master_id,
                ping_sent,
                pong_recv,
                _,
                link_state,
            ) = parts[:8]

            host, port = addr.rsplit(":", 1)

            node: ClusterNodeDetail = {
                "id": self_id,
                "host": host or current_host,
                "port": int(port.split("@")[0]),
                "flags": tuple(flags.split(",")),
                "master": master_id if master_id != "-" else None,
                "ping_sent": int(ping_sent),
                "pong_recv": int(pong_recv),
                "link_state": link_state,
                "slots": [],
                "migrations": [],
            }
            if len(parts) >= 9:
                slots, migrations = parse_slots(parts[8])
                node["slots"], node["migrations"] = slots, migrations

            nodes.append(node)

        return nodes


class ClusterShardsCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        List[Dict[AnyStr, Union[List[ValueT], Mapping[AnyStr, ValueT]]]],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> List[Dict[AnyStr, Union[List[ValueT], Mapping[AnyStr, ValueT]]]]:
        shard_mapping: List[
            Dict[AnyStr, Union[List[ValueT], Mapping[AnyStr, ValueT]]]
        ] = []

        for shard in response:
            transformed = EncodingInsensitiveDict(flat_pairs_to_dict(shard))
            node_mapping: List[Dict[AnyStr, ValueT]] = []
            for node in transformed["nodes"]:
                node_mapping.append(flat_pairs_to_dict(node))

            transformed["nodes"] = node_mapping
            shard_mapping.append(transformed.__wrapped__)  # type: ignore
        return shard_mapping

    def transform_3(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> List[Dict[AnyStr, Union[List[ValueT], Mapping[AnyStr, ValueT]]]]:

        return response
