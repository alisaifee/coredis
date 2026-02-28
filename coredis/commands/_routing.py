# coredis/commands/_routing.py
from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from coredis.exceptions import ResponseError
from coredis.response._callbacks import ClusterMultiNodeCallback, ClusterNoMerge
from coredis.typing import (
    ClassVar,
    ExecutionParameters,
    Generic,
    Mapping,
    RedisValueT,
    TypeVar,
)

from ._key_spec import KeySpec
from .constants import NodeFlag

if TYPE_CHECKING:
    from coredis.cluster._layout import ClusterLayout
    from coredis.cluster._node import ClusterNodeLocation

R = TypeVar("R")


@dataclasses.dataclass
class NodeExecution:
    node: ClusterNodeLocation
    command: bytes
    arguments: tuple[RedisValueT, ...]
    # Positions in the original argument list these map back to.
    # None means ordering doesn't matter (e.g. DEL, EXISTS)
    original_indices: list[int] | None = None
    slots: list[int] = dataclasses.field(default_factory=list)


NODE_FLAG_DOC_MAPPING = {
    NodeFlag.PRIMARIES: "all primaries",
    NodeFlag.REPLICAS: "all replicas",
    NodeFlag.RANDOM: "a random node",
    NodeFlag.ALL: "all nodes",
    NodeFlag.SLOT_ID: "one or more nodes based on the slots provided",
}


class RoutingStrategy(ABC, Generic[R]):
    cross_slot: ClassVar[bool] = True

    def __init__(
        self,
        route: NodeFlag | None,
        merge_callback: ClusterMultiNodeCallback[R],
    ) -> None:
        self.route = route
        self.merge_callback = merge_callback

    def combine(
        self,
        node_executions: list[NodeExecution],
        results: Mapping[str, R | ResponseError | TimeoutError],
    ) -> R:
        return self.merge_callback(results, executions=node_executions)

    @property
    @abstractmethod
    def description(
        self,
    ) -> str: ...

    @abstractmethod
    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]: ...


class RandomStrategy(RoutingStrategy[R]):
    cross_slot: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__(NodeFlag.RANDOM, merge_callback=ClusterNoMerge())

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        return [
            NodeExecution(
                node=cluster_layout.random_node(not readonly), command=command, arguments=arguments
            )
        ]

    @property
    def description(self) -> str:
        return "routed to a random node"


class FanoutStrategy(RoutingStrategy[R]):
    cross_slot: ClassVar[bool] = False
    fanout: NodeFlag

    def __init__(
        self,
        route: NodeFlag,
        merge_callback: ClusterMultiNodeCallback[R],
    ):
        super().__init__(route, merge_callback)
        self.fanout = route

    def nodes(
        self,
        cluster_layout: ClusterLayout,
        primary: bool,
        execution_parameters: ExecutionParameters = {},
    ) -> list[ClusterNodeLocation]:
        match self.fanout:
            case NodeFlag.PRIMARIES:
                return cluster_layout.primaries
            case NodeFlag.REPLICAS:
                return cluster_layout.replicas
            case NodeFlag.ALL:
                return cluster_layout.nodes
        raise RuntimeError(f"Invalid route {self.fanout} for fanout strategy")

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        return [
            NodeExecution(node=node, command=command, arguments=arguments)
            for node in self.nodes(cluster_layout, not readonly)
        ]

    @property
    def description(self) -> str:
        return f"""routed to {NODE_FLAG_DOC_MAPPING[self.fanout]}"""


class SlotRangeStrategy(RoutingStrategy[R]):
    """
    Only for cluster commands that deal with managing slots.
    Commands are routed to appropriate nodes by distributing slot ids in arguments
    to the appropriate nodes that handle them
    """

    def __init__(
        self,
        merge_callback: ClusterMultiNodeCallback[R] | None,
    ):
        super().__init__(NodeFlag.SLOT_ID, merge_callback or ClusterNoMerge())

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        if slot_arguments_range := execution_parameters.get("slot_arguments_range", None):
            slot_start, slot_end = slot_arguments_range
            arg_slots = arguments[slot_start : slot_end + 1]
            all_slots = list(int(k) for k in arg_slots)
            affected_nodes = cluster_layout.nodes_for_slots(*all_slots)
            node_slots: dict[ClusterNodeLocation, list[int]] = {node: [] for node in affected_nodes}
            for slot in all_slots:
                for node, slots in affected_nodes.items():
                    if slot in slots:
                        node_slots[node].append(slot)
            return [
                NodeExecution(
                    slots=slots,
                    node=node,
                    command=command,
                    arguments=arguments[:slot_start] + tuple(slots) + arguments[slot_end + 1 :],
                )
                for node, slots in node_slots.items()
            ]
        raise RuntimeError("Unable to route {command!r} with arguments {arguments}")

    @property
    def description(self) -> str:
        return f"""routed to {NODE_FLAG_DOC_MAPPING[NodeFlag.SLOT_ID]}"""


class SlotStrategy(RoutingStrategy[R]):
    """ """

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        _, key_range = KeySpec.extract_keys(command, *arguments, readonly_command=readonly)
        pre_arguments = arguments[: key_range[0]]
        post_arguments = arguments[key_range[1] + 1 :]

        slots_to_keys = KeySpec.slots_to_keys(command, *arguments, readonly_command=readonly)
        node_keys: dict[tuple[int, ClusterNodeLocation], list[RedisValueT]] = {}
        for slot, sub_keys in slots_to_keys.items():
            node = cluster_layout.node_for_slot(slot, primary=not readonly)
            node_keys.setdefault((slot, node), []).extend([k[1] for k in sub_keys])

        return [
            NodeExecution(
                slots=[slot_node[0]],
                node=slot_node[1],
                command=command,
                arguments=pre_arguments + tuple(keys) + post_arguments,
            )
            for slot_node, keys in node_keys.items()
        ]

    @property
    def description(self) -> str:
        return """routed to the nodes serving the keys in the commands"""


class KeyRangeStrategy(RoutingStrategy[R]):
    """
    For MGET where responses must be reassembled in the original key order.
    """

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        _, key_range = KeySpec.extract_keys(command, *arguments, readonly_command=readonly)
        pre_arguments = arguments[: key_range[0]]
        post_arguments = arguments[key_range[1] + 1 :]
        slots_to_keys = KeySpec.slots_to_keys(command, *arguments, readonly_command=readonly)
        # Track which original positions each node is responsible for
        node_keys: dict[tuple[int, ClusterNodeLocation], list[RedisValueT]] = {}
        node_indices: dict[tuple[int, ClusterNodeLocation], list[int]] = {}
        for slot, sub_keys in slots_to_keys.items():
            node = cluster_layout.node_for_slot(slot, primary=not readonly)
            for idx, key in sub_keys:
                node_keys.setdefault((slot, node), []).append(key)
                node_indices.setdefault((slot, node), []).append(idx)
        return [
            NodeExecution(
                slots=[slot_node[0]],
                node=slot_node[1],
                command=command,
                arguments=pre_arguments + tuple(node_keys[slot_node]) + post_arguments,
                original_indices=node_indices[slot_node],
            )
            for slot_node in node_keys
        ]

    @property
    def description(self) -> str:
        return """routed to the nodes serving the keys in the commands"""


class PairStrategy(RoutingStrategy[R]):
    """
    For commands such as MSET, MSETEX, MSETNX where key:value pairs are sent as part
    of the request
    """

    def __init__(
        self,
        route: NodeFlag | None,
        merge_callback: ClusterMultiNodeCallback[R],
        key_step: int = 2,
        add_count: bool = False,
    ):
        super().__init__(route, merge_callback)
        self.key_step = key_step
        self.add_count = add_count

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: bytes,
        arguments: tuple[RedisValueT, ...],
        readonly: bool,
        execution_parameters: ExecutionParameters,
    ) -> list[NodeExecution]:
        _, key_range = KeySpec.extract_keys(command, *arguments, readonly_command=readonly)
        slots_to_keys = KeySpec.slots_to_keys(command, *arguments, readonly_command=readonly)
        pre_arguments: tuple[RedisValueT, ...] = ()
        if not self.add_count:
            pre_arguments = arguments[: key_range[0]]
        post_arguments = arguments[key_range[1] * self.key_step + 2 :]
        keys = arguments[key_range[0] : key_range[1] * self.key_step + 1 : self.key_step]
        value_section = arguments[key_range[0] + 1 : key_range[1] * self.key_step + self.key_step]
        values = [
            value_section[chunk[0] : chunk[0] + self.key_step - 1]
            for chunk in list(enumerate(value_section))[:: self.key_step]
        ]
        pairs = dict(zip(keys, values))
        node_pairs: dict[tuple[int, ClusterNodeLocation], list[RedisValueT]] = {}
        node_indices: dict[tuple[int, ClusterNodeLocation], list[int]] = {}
        for slot, sub_keys in slots_to_keys.items():
            node = cluster_layout.node_for_slot(slot, primary=not readonly)
            for idx, key in sub_keys:
                node_pairs.setdefault((slot, node), []).extend([key, *pairs[key]])
                node_indices.setdefault((slot, node), []).append(idx)
            if self.add_count:
                node_pairs[(slot, node)].insert(0, len(sub_keys))
        return [
            NodeExecution(
                slots=[slot_node[0]],
                node=slot_node[1],
                command=command,
                arguments=pre_arguments + tuple(pairs) + post_arguments,
                original_indices=node_indices[slot_node],
            )
            for slot_node, pairs in node_pairs.items()
        ]

    @property
    def description(self) -> str:
        return """routed to the nodes serving the keys in the commands"""
