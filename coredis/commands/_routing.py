from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from coredis._utils import b, hash_slot, nativestr
from coredis.commands.request import CommandRequest, CommandResponseT
from coredis.exceptions import ResponseError
from coredis.response._callbacks import ClusterMultiNodeCallback, ClusterNoMerge
from coredis.typing import (
    ClassVar,
    ExecutionParameters,
    Generic,
    RedisValueT,
    StringT,
)

from .constants import NodeFlag

if TYPE_CHECKING:
    from coredis.cluster._layout import ClusterLayout
    from coredis.cluster._node import ClusterNodeLocation


class NodeExecution(CommandRequest[CommandResponseT]):
    _result: CommandResponseT | ResponseError | TimeoutError

    def __init__(
        self,
        node: ClusterNodeLocation,
        original_request: CommandRequest[CommandResponseT],
        node_arguments: tuple[RedisValueT, ...],
        key_positions: tuple[int, ...],
    ) -> None:
        super().__init__(
            original_request.client,
            original_request.name,
            *node_arguments,
            callback=original_request.callback,
            execution_parameters=original_request.execution_parameters,
        )
        self.node = node
        self.key_positions = key_positions

    @property
    def result(self) -> CommandResponseT | ResponseError | TimeoutError:
        if not hasattr(self, "_result"):
            raise ValueError("result for node execution has not been set yet")
        return self._result

    @result.setter
    def result(self, value: CommandResponseT | ResponseError | TimeoutError) -> None:
        self._result = value


class RoutingStrategy(ABC, Generic[CommandResponseT]):
    """
    Base routing strategy that decides how to take a
    request that is not tied to a single slot and
    distribute it to one or many nodes and then
    merge it back into a single response.
    """

    #: Whether this routing strategy supports multiple
    #: slots
    cross_slot: ClassVar[bool] = True

    def __init__(
        self,
        route: NodeFlag | None,
        merge_callback: ClusterMultiNodeCallback[CommandResponseT],
    ) -> None:
        """
        :param route: a hint to which nodes to route to
        :param merge_callback: callback to merge multiple results from
         different nodes into a consistent single response.
        """
        self.route = route
        self.merge_callback = merge_callback

    def combine(
        self,
        node_executions: list[NodeExecution[CommandResponseT]],
    ) -> CommandResponseT:
        return self.merge_callback(
            key_positions=[e.key_positions for e in node_executions],
            responses=[e.result for e in node_executions],
        )

    @property
    @abstractmethod
    def description(
        self,
    ) -> str:
        """
        Description of strategy. Purely for generating documentation
        """
        ...

    @abstractmethod
    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        """
        Split up the original command into the appropriate sub executions
        """
        ...


class RandomStrategy(RoutingStrategy[CommandResponseT]):
    cross_slot: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__(NodeFlag.RANDOM, merge_callback=ClusterNoMerge())

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        return [
            NodeExecution(
                node=cluster_layout.random_node(not readonly),
                original_request=command,
                node_arguments=command.serialized_arguments,
                key_positions=(),
            )
        ]

    @property
    def description(self) -> str:
        return NodeFlag.RANDOM.value


class FanoutStrategy(RoutingStrategy[CommandResponseT]):
    cross_slot: ClassVar[bool] = False
    fanout: NodeFlag

    def __init__(
        self,
        route: NodeFlag,
        merge_callback: ClusterMultiNodeCallback[CommandResponseT],
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
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        return [
            NodeExecution(
                node=node,
                original_request=command,
                node_arguments=command.serialized_arguments,
                key_positions=(),
            )
            for node in self.nodes(cluster_layout, not readonly)
        ]

    @property
    def description(self) -> str:
        return self.fanout.value


class SlotRangeStrategy(RoutingStrategy[CommandResponseT]):
    """
    Only for cluster commands that deal with managing slots.
    Commands are routed to appropriate nodes by distributing slot ids in arguments
    to the appropriate nodes that handle them
    """

    def __init__(
        self,
        merge_callback: ClusterMultiNodeCallback[CommandResponseT] = ClusterNoMerge(),
    ):
        super().__init__(NodeFlag.SLOT_ID, merge_callback)

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        if slot_arguments_range := command.execution_parameters.get("slot_arguments_range", None):
            slot_start, slot_end = slot_arguments_range
            arg_slots = command.serialized_arguments[slot_start : slot_end + 1]
            all_slots = list(int(k) for k in arg_slots)
            affected_nodes = cluster_layout.nodes_for_slots(*all_slots)
            node_slots: dict[ClusterNodeLocation, list[int]] = {node: [] for node in affected_nodes}
            for slot in all_slots:
                for node, slots in affected_nodes.items():
                    if slot in slots:
                        node_slots[node].append(slot)
            return [
                NodeExecution[CommandResponseT](
                    node=node,
                    original_request=command,
                    node_arguments=(
                        command.serialized_arguments[:slot_start]
                        + tuple(slots)
                        + command.serialized_arguments[slot_end + 1 :]
                    ),
                    key_positions=(),
                )
                for node, slots in node_slots.items()
            ]
        raise RuntimeError("Unable to route {command!r} with arguments {arguments}")

    @property
    def description(self) -> str:
        return NodeFlag.SLOT_ID.value


class KeyRangeStrategy(RoutingStrategy[CommandResponseT]):
    """
    For MGET where responses must be reassembled in the original key order.
    """

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        key_range = (
            (command.key_indices[0], command.key_indices[-1]) if command.key_indices else (0, 0)
        )
        pre_arguments = command.serialized_arguments[: key_range[0]]
        post_arguments = command.serialized_arguments[key_range[1] + 1 :]
        # Track which original positions each node is responsible for
        node_keys: dict[tuple[int, ClusterNodeLocation], list[RedisValueT]] = {}
        node_key_positions: dict[tuple[int, ClusterNodeLocation], list[int]] = {}
        for slot, sub_keys in command.slots_to_keys.items():
            node = cluster_layout.node_for_slot(slot, primary=not readonly)
            for idx, key in sub_keys:
                node_keys.setdefault((slot, node), []).append(key)
                node_key_positions.setdefault((slot, node), []).append(idx)
        return [
            NodeExecution(
                node=slot_node[1],
                original_request=command,
                node_arguments=pre_arguments + tuple(node_keys[slot_node]) + post_arguments,
                key_positions=tuple(node_key_positions[slot_node]),
            )
            for slot_node in node_keys
        ]

    @property
    def description(self) -> str:
        return """the nodes serving the keys in the command"""


class PairStrategy(RoutingStrategy[CommandResponseT]):
    """
    For commands such as MSET, MSETEX, MSETNX where key:value pairs are sent as part
    of the request
    """

    def __init__(
        self,
        route: NodeFlag | None,
        merge_callback: ClusterMultiNodeCallback[CommandResponseT],
        key_step: int = 2,
        add_count: bool = False,
    ):
        super().__init__(route, merge_callback)
        self.key_step = key_step
        self.add_count = add_count

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        key_range = (
            (command.key_indices[0], command.key_indices[-1]) if command.key_indices else (0, 0)
        )
        pre_arguments: tuple[RedisValueT, ...] = ()
        if not self.add_count:
            pre_arguments = command.serialized_arguments[: key_range[0]]
        post_arguments = command.serialized_arguments[key_range[1] * self.key_step + 2 :]
        keys = command.serialized_arguments[
            key_range[0] : key_range[1] * self.key_step + 1 : self.key_step
        ]
        value_section = command.serialized_arguments[
            key_range[0] + 1 : key_range[1] * self.key_step + self.key_step
        ]
        values = [
            value_section[chunk[0] : chunk[0] + self.key_step - 1]
            for chunk in list(enumerate(value_section))[:: self.key_step]
        ]
        pairs = dict(zip(keys, values))
        node_pairs: dict[tuple[int, ClusterNodeLocation], list[RedisValueT]] = {}
        node_key_positions: dict[tuple[int, ClusterNodeLocation], list[int]] = {}
        for slot, sub_keys in command.slots_to_keys.items():
            node = cluster_layout.node_for_slot(slot, primary=not readonly)
            for idx, key in sub_keys:
                node_pairs.setdefault((slot, node), []).extend([key, *pairs[key]])
                node_key_positions.setdefault((slot, node), []).append(idx)
            if self.add_count:
                node_pairs[(slot, node)].insert(0, len(sub_keys))
        return [
            NodeExecution(
                node=slot_node[1],
                original_request=command,
                node_arguments=pre_arguments + tuple(node_pairs) + post_arguments,
                key_positions=tuple(node_key_positions[slot_node]),
            )
            for slot_node, node_pairs in node_pairs.items()
        ]

    @property
    def description(self) -> str:
        return """the nodes serving the keys in the command"""


class UndefinedStrategy(RoutingStrategy[CommandResponseT]):
    cross_slot: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__(NodeFlag.SLOT_ID, merge_callback=ClusterNoMerge())

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        raise NotImplementedError(f"{nativestr(command.name)} must be routed to a slot explicitly!")

    @property
    def description(self) -> str:
        return """the explicitly passed slot when called with :meth:`~coredis.commands.CommandRequest.route`"""


class ExplicitSlotStrategy(RoutingStrategy[CommandResponseT]):
    cross_slot: ClassVar[bool] = False
    slot: int

    def __init__(self, key_or_slot: StringT | int) -> None:
        super().__init__(NodeFlag.SLOT_ID, merge_callback=ClusterNoMerge())
        self.slot = (
            hash_slot(b(key_or_slot)) if isinstance(key_or_slot, (str, bytes)) else key_or_slot
        )

    def distribute(
        self,
        cluster_layout: ClusterLayout,
        command: CommandRequest[CommandResponseT],
        readonly: bool,
    ) -> list[NodeExecution[CommandResponseT]]:
        node = cluster_layout.node_for_slot(self.slot)
        return [
            NodeExecution(
                node=node,
                original_request=command,
                node_arguments=command.serialized_arguments,
                key_positions=(),
            )
        ]

    @property
    def description(self) -> str:
        return """the explicitly passed slot when called with :meth:`~coredis.commands.CommandRequest.route`"""
