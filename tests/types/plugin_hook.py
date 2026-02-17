from __future__ import annotations

import textwrap

from pytest_mypy_plugins.item import YamlTestItem


def hook(item: YamlTestItem) -> None:
    parsed_test_data = item.parsed_test_data
    if "with_client" in parsed_test_data:
        with_client = parsed_test_data.get("with_client") or {}
        client_params = with_client.get("client_params", {})
        client_params = ", ".join(f"{k}={v}" for k, v in client_params.items())
        for file in item.files:
            if file.path.endswith("main.py"):
                current_content = file.content
                content = f"""
import coredis

async def test() -> None:
    async with coredis.Redis({client_params}) as client:
{textwrap.indent(current_content, " " * 8)}
"""
                file.content = content
