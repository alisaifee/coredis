from __future__ import annotations

from deprecated.sphinx import versionadded

from coredis.commands import CommandMixin
from coredis.typing import AnyStr

from .json import Json


class ModuleMixin(CommandMixin[AnyStr]):
    @property
    @versionadded(version="4.12.0")
    def json(self) -> Json[AnyStr]:
        """
        Property to access :class:`~coredis.modules.json.Json` commands.
        """
        return Json(self)
