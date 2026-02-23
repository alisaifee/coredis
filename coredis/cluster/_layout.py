from __future__ import annotations

import random
from typing import cast

from coredis._utils import b, hash_slot, nativestr
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.connection import TCPLocation
from coredis.typing import (
    ExecutionParameters,
    Iterator,
    RedisValueT,
    StringT,
    Unpack,
)

from ._node import ClusterNodeLocation


class ClusterLayout:
    def __init__(
        self,
        nodes: dict[TCPLocation, ClusterNodeLocation],
        slots: dict[int, list[ClusterNodeLocation]],
    ) -> None:
        self._slots: dict[int, list[ClusterNodeLocation]] = slots
        self._nodes: dict[TCPLocation, ClusterNodeLocation] = nodes

    def determine_slots(
        self,
        command: bytes,
        *args: RedisValueT,
        readonly: bool = False,
        **options: Unpack[ExecutionParameters],
    ) -> set[int]:
        """Determines the slots the command and args would touch"""
        keys = cast(tuple[RedisValueT, ...], options.get("keys")) or KeySpec.extract_keys(
            command,
            *args,
            readonly_command=readonly,
        )
        if (
            command
            in {
                CommandName.EVAL,
                CommandName.EVAL_RO,
                CommandName.EVALSHA,
                CommandName.EVALSHA_RO,
                CommandName.FCALL,
                CommandName.FCALL_RO,
                CommandName.PUBLISH,
            }
            and not keys
        ):
            return set()

        return {hash_slot(b(key)) for key in keys}

    def determine_node(
        self,
        command: bytes,
        *args: RedisValueT,
        node_flag: NodeFlag | None = None,
        readonly: bool = False,
        **kwargs: Unpack[ExecutionParameters],
    ) -> list[ClusterNodeLocation] | None:
        if node_flag == NodeFlag.RANDOM:
            return [self.random_node(primary=not readonly)]
        elif node_flag == NodeFlag.PRIMARIES:
            return list(self.all_primaries())
        elif node_flag == NodeFlag.ALL:
            return list(self.all_nodes())
        elif node_flag == NodeFlag.SLOT_ID and (
            slot_arguments_range := kwargs.get("slot_arguments_range", None)
        ):
            slot_start, slot_end = slot_arguments_range
            nodes = list(
                self.nodes_from_slots(*cast(tuple[int, ...], args[slot_start:slot_end])).keys()
            )
            return nodes
        return None

    def keys_to_nodes_by_slot(
        self, *keys: RedisValueT
    ) -> dict[ClusterNodeLocation, dict[int, list[RedisValueT]]]:
        mapping: dict[ClusterNodeLocation, dict[int, list[RedisValueT]]] = {}
        for k in keys:
            node = self.node_from_slot(hash_slot(b(k)))
            if node:
                mapping.setdefault(node, {}).setdefault(hash_slot(b(k)), []).append(k)
        return mapping

    def keys_to_slots(self, *keys: RedisValueT) -> set[int]:
        slots = set()
        for k in keys:
            node = self.node_from_slot(hash_slot(b(k)))
            if node:
                slots.add(hash_slot(b(k)))
        return slots

    def node_from_location(self, location: TCPLocation) -> ClusterNodeLocation | None:
        return self._nodes.get(location)

    def node_from_slot(self, slot: int, primary: bool = True) -> ClusterNodeLocation | None:
        primary_node: ClusterNodeLocation | None = None
        replica_nodes: list[ClusterNodeLocation] = []
        for node in self._slots[slot]:
            if node.server_type == "primary":
                primary_node = node
            else:
                replica_nodes.append(node)
        if primary:
            return primary_node
        elif replica_nodes:
            return random.choice(replica_nodes)
        return None

    def nodes_from_slots(
        self, *slots: int, primary: bool = True
    ) -> dict[ClusterNodeLocation, list[int]]:
        mapping: dict[ClusterNodeLocation, list[int]] = {}
        for slot in slots:
            if node := self.node_from_slot(slot, primary):
                mapping.setdefault(node, []).append(slot)
        return mapping

    def all_nodes(self) -> Iterator[ClusterNodeLocation]:
        yield from self._nodes.values()

    def all_primaries(self) -> Iterator[ClusterNodeLocation]:
        for node in self._nodes.values():
            if node.server_type == "primary":
                yield node

    def all_replicas(self) -> Iterator[ClusterNodeLocation]:
        for node in self._nodes.values():
            if node.server_type == "replica":
                yield node

    def random_node(self, primary: bool = True) -> ClusterNodeLocation:
        if primary:
            return random.choice(list(self.all_primaries()))
        else:
            return random.choice(list(self._nodes.values()))

    def update_primary(
        self,
        slot: int,
        host: StringT,
        port: int,
    ) -> ClusterNodeLocation:
        """Updates the primary for a specific slot"""
        node = ClusterNodeLocation(
            host=nativestr(host),
            port=port,
            server_type="primary",
            node_id=None,
        )
        self._nodes[TCPLocation(node.host, node.port)] = node
        current_primary: int | None = None
        for idx, current in enumerate(self._slots.get(slot, [])):
            if current.server_type == "primary" and (
                current.host != node.host or current.port != node.port
            ):
                current_primary = idx
                break
        if current_primary is not None:
            self._slots[slot].pop(current_primary)
        self._slots.setdefault(slot, []).append(node)
        return node
