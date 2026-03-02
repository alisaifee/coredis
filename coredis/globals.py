from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from coredis.commands.constants import CommandFlag, NodeFlag
from coredis.typing import Callable, ResponseType

if TYPE_CHECKING:
    from coredis.modules.base import ModuleGroupRegistry, ModuleRegistry

#: Populated by the @redis_command wrapper
READONLY_COMMANDS: set[bytes] = set()
#: Populated by the @redis_command wrapper
COMMAND_FLAGS: dict[bytes, set[CommandFlag]] = defaultdict(set)

#: Populated by the @redis_command wrapper
ROUTE_FLAGS: dict[bytes, NodeFlag] = {}

#: Populated by the @redis_command wrapper
SPLIT_FLAGS: dict[bytes, NodeFlag] = {}

#: Populated by the @redis_command wrapper
MERGE_CALLBACKS: dict[bytes, Callable[..., ResponseType]] = {}

#: Populated by the @redis_command wrapper
CACHEABLE_COMMANDS: set[bytes] = set()

#: Populated by ModuleGroupRegistry
MODULE_GROUPS: set[ModuleGroupRegistry] = set()

#: Populated by ModuleRegistry
MODULES: dict[str, ModuleRegistry] = {}
