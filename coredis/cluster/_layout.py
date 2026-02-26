from __future__ import annotations

import random

from coredis._utils import nativestr
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.connection import TCPLocation
from coredis.exceptions import ClusterCrossSlotError, RedisClusterError
from coredis.globals import READONLY_COMMANDS, ROUTE_FLAGS, SPLIT_FLAGS
from coredis.typing import ExecutionParameters, Iterator, RedisValueT, StringT

from ._discovery import DiscoveryService
from ._node import ClusterNodeLocation


class ClusterLayout:
    def __init__(
        self,
        discovery_service: DiscoveryService,
    ) -> None:
        self.__slots: dict[int, list[ClusterNodeLocation]] = {}
        self.__nodes: dict[TCPLocation, ClusterNodeLocation] = {}
        self.__discovery_service = discovery_service

    async def refresh(self) -> None:
        nodes, slots = await self.__discovery_service.get_cluster_layout()
        self.__nodes.clear()
        for node in nodes:
            self.__nodes[TCPLocation(node.host, node.port)] = node
        self.__slots.clear()
        self.__slots.update(slots)

    def node_for_request(
        self,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        prefer_replica: bool = False,
        execution_parameters: ExecutionParameters = {},
    ) -> ClusterNodeLocation:
        """
        Maps a request to a single node if possible.
        """
        nodes = self.nodes_for_request(
            command,
            arguments,
            prefer_replica=prefer_replica,
            execution_parameters=execution_parameters,
        )
        if not nodes or len(nodes) > 1:
            raise RedisClusterError(
                f"Could not map {nativestr(command)} request to a single node in the cluster"
            )
        return list(nodes.keys()).pop()

    def nodes_for_request(
        self,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        prefer_replica: bool = False,
        allow_cross_slot: bool = False,
        execution_parameters: ExecutionParameters = {},
    ) -> dict[ClusterNodeLocation, list[tuple[RedisValueT, ...]]]:
        """
        Expands a request into the appropriate nodes it should be executed
        on. The returned mapping contains unique nodes for execution as keys
        mapped to a list of arguments to be used for execution. In most cases
        the list contains just one entry, the original arguments - but for
        commands that can be split over multiple nodes safely, the arguments will
        be separated into chunks per slot.
        """
        if not self.__nodes:
            raise RedisClusterError("No known nodes in cluster")

        nodes: dict[ClusterNodeLocation, list[tuple[RedisValueT, ...]]] = {}
        slots_to_keys = KeySpec.slots_to_keys(
            command, *arguments, readonly_command=command in READONLY_COMMANDS and prefer_replica
        )
        keys = KeySpec.extract_keys(command, *arguments)
        node_flag = ROUTE_FLAGS.get(command)

        # If the command can be split across multiple nodes in a non atomic
        # request, allow returning multiple nodes for the same request
        # but with the keys separated.
        split = False
        if command in SPLIT_FLAGS and allow_cross_slot:
            split = True
            node_flag = SPLIT_FLAGS[command]
            if keys:
                key_start: int = arguments.index(keys[0])
                key_end: int = arguments.index(keys[-1])
            assert arguments[key_start : 1 + key_end] == keys, (
                f"Unable to map {command.decode('latin-1')} by keys {keys}"
            )
        if slots_to_keys:
            if not split and len(slots_to_keys) > 1:
                raise ClusterCrossSlotError(command=command, keys=keys)

            for slot, slot_keys in slots_to_keys.items():
                node = self.node_for_slot(slot, not prefer_replica)
                nodes.setdefault(node, [])
                if split:
                    nodes[node].append(
                        (*arguments[:key_start], *slot_keys, *arguments[1 + key_end :])
                    )

                # Commands that contain keys can not be performed when they affect
                # multiple slots. Support for splitting only exists for certain commands
                # that have a stable key position and where the order of responses does
                # not matter.
                # if not split and len(nodes) > 1:
                #    raise ClusterCrossSlotError(command=command, keys=keys)

        # The remaining branches apply to non keyed commands
        elif node_flag == NodeFlag.RANDOM:
            nodes = {self.random_node(primary=not prefer_replica): []}
        elif node_flag == NodeFlag.PRIMARIES:
            nodes = {node: [] for node in self.primaries}
        elif node_flag == NodeFlag.ALL:
            nodes = {node: [] for node in self.nodes}
        elif node_flag == NodeFlag.SLOT_ID and (
            slot_arguments_range := execution_parameters.get("slot_arguments_range", None)
        ):
            slot_start, slot_end = slot_arguments_range
            arg_slots = arguments[slot_start:slot_end]
            all_slots = set(int(k) for k in arg_slots)
            for node, slots in self.nodes_for_slots(*all_slots).items():
                nodes[node] = [(*slots, *arguments[slot_end:])]
        if command in {
            CommandName.FCALL,
            CommandName.FCALL_RO,
            CommandName.EVAL,
            CommandName.EVAL_RO,
            CommandName.EVALSHA,
            CommandName.EVALSHA_RO,
        }:
            # If the scripting call doesn not contain any keys, pick a random
            # node
            if not nodes:
                nodes = {self.random_node(primary=not prefer_replica): []}
        # Populate arguments for all nodes if they haven't been populated
        for node in nodes:
            if not nodes[node]:
                nodes[node] = [arguments]
        return nodes

    def node_for_location(self, location: TCPLocation) -> ClusterNodeLocation | None:
        return self.__nodes.get(location)

    def node_for_slot(self, slot: int, primary: bool = True) -> ClusterNodeLocation:
        primary_node: ClusterNodeLocation | None = None
        replica_nodes: list[ClusterNodeLocation] = []
        for node in self.__slots[slot]:
            if node.server_type == "primary":
                primary_node = node
            else:
                replica_nodes.append(node)
        if primary and primary_node:
            return primary_node
        elif replica_nodes:
            return random.choice(replica_nodes)
        raise RedisClusterError(
            f"Unable to map slot {slot} to a {'primary' if primary else 'replica'} node"
        )

    def nodes_for_slots(
        self, *slots: int, primary: bool = True
    ) -> dict[ClusterNodeLocation, list[int]]:
        mapping: dict[ClusterNodeLocation, list[int]] = {}
        for slot in slots:
            if node := self.node_for_slot(slot, primary):
                mapping.setdefault(node, []).append(slot)
        return mapping

    @property
    def nodes(self) -> Iterator[ClusterNodeLocation]:
        yield from self.__nodes.values()

    @property
    def primaries(self) -> Iterator[ClusterNodeLocation]:
        for node in self.__nodes.values():
            if node.server_type == "primary":
                yield node

    @property
    def replicas(self) -> Iterator[ClusterNodeLocation]:
        for node in self.__nodes.values():
            if node.server_type == "replica":
                yield node

    def random_node(self, primary: bool = True) -> ClusterNodeLocation:
        if primary:
            return random.choice(list(self.primaries))
        else:
            return random.choice(list(self.nodes))

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
        self.__nodes[TCPLocation(node.host, node.port)] = node
        current_primary: int | None = None
        for idx, current in enumerate(self.__slots.get(slot, [])):
            if current.server_type == "primary" and (
                current.host != node.host or current.port != node.port
            ):
                current_primary = idx
                break
        if current_primary is not None:
            self.__slots[slot].pop(current_primary)
        self.__slots.setdefault(slot, []).append(node)
        return node

    def register_errors(self, *errors: Exception) -> None:
        pass
