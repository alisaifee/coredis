from __future__ import annotations

from coredis.commands import ResponseCallback
from coredis.response.utils import flat_pairs_to_dict
from coredis.typing import Any, List, Mapping, MutableMapping, Tuple, TypedDict, Union
from coredis.utils import EncodingInsensitiveDict, nativestr


class ClusterNode(TypedDict):
    host: str
    port: int
    node_id: str
    server_type: str


class ClusterLinksCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> List[MutableMapping[str, Any]]:
        transformed = []
        for item in response:
            transformed.append(flat_pairs_to_dict(item))
        return transformed

    def transform_3(
        self, response: Any, **options: Any
    ) -> List[MutableMapping[str, Any]]:
        return response


class ClusterInfoCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> MutableMapping[str, str]:
        response = nativestr(response)

        return dict([line.split(":") for line in response.splitlines() if line])


class ClusterSlotsCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Mapping[Tuple[int, int], Tuple[ClusterNode, ...]]:
        res = {}

        for slot_info in response:
            min_slot, max_slot = map(int, slot_info[:2])
            nodes = slot_info[2:]
            res[(min_slot, max_slot)] = tuple(self.parse_node(node) for node in nodes)
            res[(min_slot, max_slot)][0]["server_type"] = "master"

        return res

    def parse_node(self, node) -> ClusterNode:
        return ClusterNode(
            host=node[0],
            port=node[1],
            node_id=node[2] if len(node) > 2 else "",
            server_type="slave",
        )


class ClusterNodesCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> List[MutableMapping[str, str]]:
        resp: Union[List[str], str]
        if isinstance(response, list):
            resp = [nativestr(row) for row in response]
        else:
            resp = nativestr(response)
        current_host = options.get("current_host", "")

        def parse_slots(s):
            slots: List[Any] = []
            migrations: List[Any] = []

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
                    slots.extend(range(int(start), int(end) + 1))
                else:
                    slots.append(int(r))

            return slots, migrations

        if isinstance(resp, str):
            resp = resp.splitlines()

        nodes = []

        for line in resp:
            parts = line.split(" ", 8)
            (
                self_id,
                addr,
                flags,
                master_id,
                ping_sent,
                pong_recv,
                config_epoch,
                link_state,
            ) = parts[:8]

            host, port = addr.rsplit(":", 1)

            node: MutableMapping = {
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
                node["slots"], node["migrations"] = tuple(slots), migrations

            nodes.append(node)

        return nodes


class ClusterShardsCallback(ResponseCallback):
    def transform(self, response: Any, **kwargs: Any) -> List[Mapping]:
        shard_mapping = []
        for shard in response:
            transformed = EncodingInsensitiveDict(flat_pairs_to_dict(shard))
            node_mapping = []
            for node in transformed["nodes"]:
                node_mapping.append(flat_pairs_to_dict(node))

            transformed["nodes"] = node_mapping
            shard_mapping.append(transformed.__wrapped__)
        return shard_mapping

    def transform_3(self, response: Any, **kwargs: Any) -> List[Mapping]:
        return response
