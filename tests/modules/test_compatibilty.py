from __future__ import annotations

import pytest

from coredis import Redis
from coredis.exceptions import CommandSyntaxError, ModuleCommandNotSupportedError
from tests.conftest import module_targets


@module_targets()
class TestModuleCompatibility:
    @pytest.mark.max_module_version("bf", "2.4.0")
    async def test_module_version_too_low_for_command(self, client: Redis):
        with pytest.raises(ModuleCommandNotSupportedError):
            await client.tdigest.create("test")

    @pytest.mark.max_module_version("timeseries", "1.8.0")
    async def test_module_version_too_low_for_argument(self, client: Redis):
        await client.timeseries.create("ts")
        with pytest.raises(CommandSyntaxError):
            await client.timeseries.range("ts", 0, 1, latest=True)
