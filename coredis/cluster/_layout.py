from __future__ import annotations

import math
import random
import time
from collections import Counter, defaultdict

from anyio import TASK_STATUS_IGNORED, WouldBlock, create_memory_object_stream
from anyio.abc import TaskStatus
from anyio.lowlevel import checkpoint

from coredis._utils import logger, nativestr
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.connection import TCPLocation
from coredis.exceptions import (
    ClusterCrossSlotError,
    ConnectionError,
    MovedError,
    RedisClusterError,
    RedisError,
    ResponseError,
)
from coredis.globals import READONLY_COMMANDS, ROUTE_FLAGS, SPLIT_FLAGS
from coredis.retry import ExponentialBackoffRetryPolicy
from coredis.typing import ExecutionParameters, RedisValueT

from ._discovery import DiscoveryService
from ._node import ClusterNodeLocation


class ClusterLayout:
    def __init__(
        self,
        discovery_service: DiscoveryService,
        error_threshold: int = 15,
        maximum_staleness: float = math.inf,
    ) -> None:
        """
        :param discovery_service: The discovery service to use to get the cluster
         layout
        :param error_threshold: Maximum number of errors to observe before forcing
         a refresh of the layout
        :param maximum_staleness: The maximum seconds to tolerate errors while trying
         to refresh the layout. After this threshold, this instance will give up
         trying to keep the layout fresh and the monitor task will raise an error."""
        self._slots: defaultdict[int, list[ClusterNodeLocation]] = defaultdict(list)
        self._nodes: dict[TCPLocation, ClusterNodeLocation] = {}
        self._discovery_service = discovery_service
        self._error_stream = create_memory_object_stream[
            tuple[ClusterNodeLocation | None, Exception]
        ](error_threshold * 2)
        self._error_threshold = error_threshold
        self._last_refresh: float = -math.inf
        self._maximum_staleness = maximum_staleness

    async def initialize(self) -> None:
        await self._refresh()

    def node_for_request(
        self,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        primary: bool = True,
        execution_parameters: ExecutionParameters = {},
    ) -> ClusterNodeLocation:
        """
        Maps a request to a single node if possible.
        """
        nodes = self.nodes_for_request(
            command,
            arguments,
            primary=primary,
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
        primary: bool = True,
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
        if not self._nodes:
            raise RedisClusterError("Local cluster layout cache is empty")

        nodes: dict[ClusterNodeLocation, list[tuple[RedisValueT, ...]]] = {}
        slots_to_keys = KeySpec.slots_to_keys(
            command, *arguments, readonly_command=command in READONLY_COMMANDS and not primary
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
            # Commands that contain keys can not be performed when they affect
            # multiple slots. Support for splitting only exists for certain commands
            # that have a stable key position and where the order of responses does
            # not matter.
            if not split and len(slots_to_keys) > 1:
                raise ClusterCrossSlotError(command=command, keys=keys)

            for slot, slot_keys in slots_to_keys.items():
                node = self.node_for_slot(slot, primary)
                nodes.setdefault(node, [])
                if split:
                    nodes[node].append(
                        (*arguments[:key_start], *slot_keys, *arguments[1 + key_end :])
                    )

        # The remaining branches apply to non keyed commands
        elif node_flag == NodeFlag.RANDOM:
            nodes = {self.random_node(primary=primary): [arguments]}
        elif node_flag == NodeFlag.PRIMARIES:
            nodes = {node: [arguments] for node in self.primaries}
        elif node_flag == NodeFlag.ALL:
            nodes = {node: [arguments] for node in self.nodes}
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
                nodes = {self.random_node(primary=primary): [arguments]}
        return nodes

    def node_for_location(self, location: TCPLocation) -> ClusterNodeLocation | None:
        return self._nodes.get(location)

    def node_for_slot(self, slot: int, primary: bool = True) -> ClusterNodeLocation:
        primary_node: ClusterNodeLocation | None = None
        replica_nodes: list[ClusterNodeLocation] = []
        for node in self._slots[slot]:
            if node.is_primary:
                primary_node = node
            else:
                replica_nodes.append(node)
        if primary and primary_node:
            return primary_node
        elif replica_nodes:
            return list(sorted(replica_nodes, key=lambda v: v.priority))[-1]
        # Last resort, if there is no replica, return the primary
        elif primary_node:
            return primary_node
        raise RedisClusterError(
            f"Unable to map slot {slot} to a {'primary' if primary else 'replica'} node"
        )

    def nodes_for_slots(
        self, *slots: int, primary: bool = True
    ) -> dict[ClusterNodeLocation, list[int]]:
        mapping: dict[ClusterNodeLocation, list[int]] = defaultdict(list)
        for slot in slots:
            if node := self.node_for_slot(slot, primary):
                mapping[node].append(slot)
        return mapping

    @property
    def nodes(self) -> list[ClusterNodeLocation]:
        return [node for node in self._nodes.values()]

    @property
    def primaries(self) -> list[ClusterNodeLocation]:
        return [node for node in self._nodes.values() if node.is_primary]

    @property
    def replicas(self) -> list[ClusterNodeLocation]:
        return [node for node in self._nodes.values() if node.server_type == "replica"]

    def random_node(self, primary: bool = True) -> ClusterNodeLocation:
        if primary:
            return random.choice(list(self.primaries))
        else:
            return random.choice(list(self.nodes))

    def _handle_moved_error(
        self,
        error: MovedError,
        errored_node: ClusterNodeLocation,
    ) -> None:
        """
        Updates the node mapping based on a Moved error
        """
        location = TCPLocation(error.host, error.port)
        redirect_node = self._nodes.setdefault(
            location, ClusterNodeLocation(location.host, location.port, server_type="primary")
        )
        slot_nodes = self._slots[error.slot_id]
        for idx, current in enumerate(slot_nodes):
            if (is_errored := (current == errored_node)) or current.is_primary:
                if redirect_node in slot_nodes and is_errored:
                    slot_nodes.pop(idx)
                    break
                else:
                    slot_nodes[idx] = redirect_node

    def report_errors(self, node: ClusterNodeLocation | None, *errors: Exception) -> None:
        for error in errors:
            if node and not isinstance(error, ResponseError):
                node.priority -= 1
            if node and isinstance(error, MovedError):
                self._handle_moved_error(error, node)
            try:
                self._error_stream[0].send_nowait((node, error))
            except WouldBlock:
                pass

    async def monitor(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        task_status.started()
        refresh_retry_policy = ExponentialBackoffRetryPolicy(
            (RedisError,),
            retries=None,
            base_delay=1,
            max_delay=max(0, min(30, self._maximum_staleness) - 1),
            deadline=self._maximum_staleness,
        )
        errors: dict[ClusterNodeLocation | None, Counter[type[Exception]]] = defaultdict(Counter)
        while True:
            node, error = await self._error_stream[1].receive()
            errors[node][type(error)] += 1
            total_errors = sum(
                count
                for counter in errors.values()
                for err_type, count in counter.items()
                if issubclass(err_type, (RedisClusterError, ConnectionError))
            )
            if total_errors >= self._error_threshold:
                logger.debug("Refreshing cluster layout due to error threshold exceeded")
                await refresh_retry_policy.call_with_retries(self._refresh)
                errors.clear()

            await checkpoint()

    async def _refresh(self) -> None:
        nodes, slots = await self._discovery_service.get_cluster_layout()
        nodes_by_location = {TCPLocation(node.host, node.port): node for node in nodes}
        self._nodes, self._slots = nodes_by_location, slots
        self._last_refresh = time.monotonic()
