from __future__ import annotations

from unittest.mock import ANY

import pytest

from coredis import Redis
from coredis._concurrency import gather
from coredis.modules.response.types import AutocompleteSuggestion
from tests.conftest import module_targets


@module_targets()
class TestAutocomplete:
    async def test_add_suggestions(self, client: Redis):
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1)
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1, increment_score=True)
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)

        assert 2 == await client.autocomplete.suglen("suggest")

    async def test_delete_suggestion(self, client: Redis):
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1)
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)
        assert 2 == await client.autocomplete.suglen("suggest")
        assert await client.autocomplete.sugdel("suggest", "hello world")
        assert not await client.autocomplete.sugdel("suggest", "hello world")
        assert 1 == await client.autocomplete.suglen("suggest")

    async def test_suggestions(self, client: Redis, _s):
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1, payload="goodbye")
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)
        assert 3 == await client.autocomplete.sugadd(
            "suggest", "help", 1, payload="not just anybody"
        )

        assert 3 == len(await client.autocomplete.sugget("suggest", "hel"))
        assert 1 == len(await client.autocomplete.sugget("suggest", "hel", max_suggestions=1))
        assert _s("help") == (await client.autocomplete.sugget("suggest", "hel"))[0].string
        assert _s("hello") == (await client.autocomplete.sugget("suggest", "hell"))[0].string
        assert not (await client.autocomplete.sugget("suggest", "hall"))
        assert (
            _s("hello")
            == (await client.autocomplete.sugget("suggest", "hall", fuzzy=True))[0].string
        )
        assert (await client.autocomplete.sugget("suggest", "hel", withscores=True))[
            0
        ].score is not None
        assert (
            _s("not just anybody")
            == (
                await client.autocomplete.sugget(
                    "suggest", "hel", withscores=True, withpayloads=True
                )
            )[0].payload
        )

        assert 3 == await client.autocomplete.sugadd("suggest", "hello", 100, increment_score=True)
        assert (
            _s("goodbye")
            == (await client.autocomplete.sugget("suggest", "hel", withpayloads=True))[0].payload
        )

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool, _s):
        async with client.pipeline(transaction=transaction) as p:
            results = [
                p.autocomplete.sugadd("suggest", "hello", 1),
                p.autocomplete.sugadd("suggest", "hello world", 1),
                p.autocomplete.suglen("suggest"),
                p.autocomplete.sugget("suggest", "hel"),
                p.autocomplete.sugdel("suggest", "hello"),
                p.autocomplete.sugdel("suggest", "hello world"),
                p.autocomplete.suglen("suggest"),
            ]
        assert await gather(*results) == (
            1,
            2,
            2,
            (
                AutocompleteSuggestion(_s("hello"), ANY, ANY),
                AutocompleteSuggestion(_s("hello world"), ANY, ANY),
            ),
            1,
            1,
            0,
        )
