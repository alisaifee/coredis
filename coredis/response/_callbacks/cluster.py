from __future__ import annotations

from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ClusterNode, ClusterNodeDetail
from coredis.typing import (
    AnyStr,
    Mapping,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
)


class ClusterLinksCallback(ResponseCallback[ResponseType, list[dict[AnyStr, ResponsePrimitive]]]):
    def transform(
        self,
        response: ResponseType,
    ) -> list[dict[AnyStr, ResponsePrimitive]]:
        return response


class ClusterInfoCallback(ResponseCallback[ResponseType, dict[str, str]]):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[str, str]:
        response_str = nativestr(response)
        return dict([line.split(":") for line in response_str.splitlines() if line])


class ClusterSlotsCallback(
    ResponseCallback[ResponseType, dict[tuple[int, int], tuple[ClusterNode, ...]]]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[tuple[int, int], tuple[ClusterNode, ...]]:
        res: dict[tuple[int, int], tuple[ClusterNode, ...]] = {}

        for slot_info in response:
            min_slot, max_slot = map(int, slot_info[:2])
            nodes = slot_info[2:]
            res[(min_slot, max_slot)] = tuple(self.parse_node(node) for node in nodes)
            res[(min_slot, max_slot)][0]["server_type"] = "master"

        return res

    def parse_node(self, node: list[int | str]) -> ClusterNode:
        return ClusterNode(
            host=nativestr(node[0]),
            port=int(node[1]),
            node_id=nativestr(node[2]) if len(node) > 2 else "",
            server_type="slave",
        )


class ClusterNodesCallback(ResponseCallback[ResponseType, list[ClusterNodeDetail]]):
    def transform(
        self,
        response: ResponseType,
    ) -> list[ClusterNodeDetail]:
        resp: list[str] | str

        if isinstance(response, list):
            resp = [nativestr(row) for row in response]
        else:
            resp = nativestr(response)
        current_host = nativestr(self.options.get("current_host", ""))

        def parse_slots(s: str) -> tuple[list[int], list[dict[str, RedisValueT]]]:
            slots: list[int] = []
            migrations: list[dict[str, RedisValueT]] = []

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

        nodes: list[ClusterNodeDetail] = []

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
        list[dict[AnyStr, list[RedisValueT] | Mapping[AnyStr, RedisValueT]]],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> list[dict[AnyStr, list[RedisValueT] | Mapping[AnyStr, RedisValueT]]]:
        return response
