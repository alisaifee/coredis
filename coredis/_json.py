from __future__ import annotations

from types import ModuleType

json: ModuleType
try:
    import orjson as json  # type: ignore
except ImportError:
    import json

__all__ = ["json"]
