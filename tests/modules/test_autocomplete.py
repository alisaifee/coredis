from __future__ import annotations

from coredis import Redis
from tests.conftest import targets


@targets(
    "redis_stack",
    "redis_stack_cached",
    "redis_stack_cluster",
)
class TestAutocomplete:
    async def test_add_suggestions(self, client: Redis):
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1)
        assert 1 == await client.autocomplete.sugadd(
            "suggest", "hello", 1, increment_score=True
        )
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)

        assert 2 == await client.autocomplete.suglen("suggest")

    async def test_delete_suggestion(self, client: Redis):
        assert 1 == await client.autocomplete.sugadd("suggest", "hello", 1)
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)
        assert 2 == await client.autocomplete.suglen("suggest")
        assert await client.autocomplete.sugdel("suggest", "hello world")
        assert not await client.autocomplete.sugdel("suggest", "hello world")
        assert 1 == await client.autocomplete.suglen("suggest")

    async def test_suggestions(self, client: Redis):
        assert 1 == await client.autocomplete.sugadd(
            "suggest", "hello", 1, payload="goodbye"
        )
        assert 2 == await client.autocomplete.sugadd("suggest", "hello world", 1)
        assert 3 == await client.autocomplete.sugadd(
            "suggest", "help", 1, payload="not just anybody"
        )

        assert 3 == len(await client.autocomplete.sugget("suggest", "hel"))
        assert 1 == len(
            await client.autocomplete.sugget("suggest", "hel", max_suggestions=1)
        )
        assert "help" == (await client.autocomplete.sugget("suggest", "hel"))[0].string
        assert (
            "hello" == (await client.autocomplete.sugget("suggest", "hell"))[0].string
        )
        assert not (await client.autocomplete.sugget("suggest", "hall"))
        assert (
            "hello"
            == (await client.autocomplete.sugget("suggest", "hall", fuzzy=True))[
                0
            ].string
        )
        assert (await client.autocomplete.sugget("suggest", "hel", withscores=True))[
            0
        ].score is not None
        assert (
            "not just anybody"
            == (
                await client.autocomplete.sugget(
                    "suggest", "hel", withscores=True, withpayloads=True
                )
            )[0].payload
        )

        assert 3 == await client.autocomplete.sugadd(
            "suggest", "hello", 100, increment_score=True
        )
        assert (
            "goodbye"
            == (await client.autocomplete.sugget("suggest", "hel", withpayloads=True))[
                0
            ].payload
        )
