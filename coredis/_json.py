from __future__ import annotations

from types import ModuleType

json: ModuleType
try:
    from orjson import json  # type: ignore
except ImportError:
    import json

__all__ = ["json"]
