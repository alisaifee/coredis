from __future__ import annotations

from unittest.mock import ANY

import pytest

from coredis import PureToken, Redis
from coredis.exceptions import ResponseError
from coredis.modules.response.types import GraphNode, GraphQueryResult
from tests.conftest import module_targets


@module_targets()
@pytest.mark.max_server_version("7.0.0")
class TestGraph:
    async def test_create_graph(self, client: Redis):
        result = await client.graph.query(
            "graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})"
        )
        assert not result.result_set
        assert result.stats["Nodes created"] == 2
        assert result.stats["Labels added"] == 1
        assert result.stats["Relationships created"] == 1
        assert {"graph"} == await client.graph.list()

    async def test_delete_graph(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        assert await client.graph.delete("graph")
        assert not await client.graph.list()
        with pytest.raises(ResponseError):
            assert not await client.graph.delete("graph")

    async def test_readonly_query(self, client: Redis):
        await client.graph.query(
            "graph",
            "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})",
        )
        with pytest.raises(
            ResponseError,
            match="graph.RO_QUERY is to be executed only on read-only queries",
        ):
            await client.graph.ro_query(
                "graph",
                "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})",
            )

        assert 1 == len(
            (
                await client.graph.ro_query("graph", "MATCH (n)-[r]->(m) return n", timeout=100)
            ).result_set
        )

    async def test_query_nodes(self, client: Redis):
        await client.graph.query(
            "graph",
            "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})",
            timeout=100,
        )
        result = await client.graph.query("graph", "MATCH (n) RETURN n")
        assert result.result_set[0][0].labels == {"Node"}
        assert result.result_set[0][0].properties["name"] == "A"
        assert result.result_set[1][0].labels == {"Node"}
        assert result.result_set[1][0].properties["name"] == "B"

    async def test_query_relations(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        result = await client.graph.query("graph", "MATCH (n)-[r]->(m) RETURN n, r, m")
        assert result.result_set[0][0].labels == {"Node"}
        assert result.result_set[0][0].properties["name"] == "A"
        assert result.result_set[0][1].type == "EDGE"
        assert result.result_set[0][2].labels == {"Node"}
        assert result.result_set[0][2].properties["name"] == "B"
        await client.graph.query(
            "graph",
            """
            CREATE (:Node {
              name: 'α', language: 'greek'
            })
            -[:EDGE {
              distance: 1
            }]
            ->(:Node {
              name: 'β', language: 'greek'
            })
            """,
        )
        result = await client.graph.query(
            "graph", "MATCH (n {language: 'greek'})-[r]->(m) RETURN n, r, m"
        )
        assert result.result_set[0][0].labels == {"Node"}
        assert result.result_set[0][0].properties["name"] == "α"
        assert result.result_set[0][0].properties["language"] == "greek"
        assert result.result_set[0][1].properties["distance"] == 1

    async def test_query_paths(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        await client.graph.query("graph", "CREATE (:Node {name: 'B'})-[:EDGE]->(:Node {name: 'C'})")

        result = await client.graph.query("graph", "MATCH p = (n)-[r]->(m) return p")

        assert len(result.result_set) == 2

        p1 = result.result_set[0][0]
        p2 = result.result_set[1][0]

        assert p1.path == (p1.nodes[0], p1.relations[0], p1.nodes[1])
        assert ("A", "EDGE", "B") == (
            p1.nodes[0].properties["name"],
            p1.relations[0].type,
            p1.nodes[1].properties["name"],
        )

        assert p2.path == (p2.nodes[0], p2.relations[0], p2.nodes[1])
        assert ("B", "EDGE", "C") == (
            p2.nodes[0].properties["name"],
            p2.relations[0].type,
            p2.nodes[1].properties["name"],
        )

    @pytest.mark.parametrize(
        "query, expected",
        [
            ("RETURN 1 = 0", False),
            ("RETURN 1 = 1", True),
            ("RETURN 3.142", pytest.approx(3.142)),
            ("RETURN 1", 1),
            ("RETURN 'test'", "test"),
            ("RETURN ['test']", ["test"]),
            (
                "RETURN {fu: 1, bar: true, baz: [1,2,true]}",
                {"fu": 1, "bar": True, "baz": [1, 2, True]},
            ),
            (
                "RETURN point({latitude: 1.3, longitude: 2.3})",
                (pytest.approx(1.3), pytest.approx(2.3)),
            ),
        ],
    )
    async def test_response_types(self, client: Redis, query, expected):
        result = await client.graph.query("grap", query)
        assert result.result_set[0][0] == expected

    @pytest.mark.min_module_version("graph", "2.12")
    async def test_unique_constraint_node(self, client: Redis):
        with pytest.raises(ResponseError, match="missing supporting exact-match index"):
            assert await client.graph.constraint_create(
                "graph", PureToken.UNIQUE, node="Node", properties=["name"]
            )
        await client.graph.query("graph", "CREATE INDEX FOR (n:Node) ON (n.name)")

        assert await client.graph.constraint_create(
            "graph", PureToken.UNIQUE, node="Node", properties=["name"]
        )

        await client.graph.query("graph", "CREATE (:Node {name: 'test'})")
        with pytest.raises(ResponseError, match="unique constraint violation"):
            await client.graph.query("graph", "CREATE (:Node {name: 'test'})")

        assert await client.graph.constraint_drop(
            "graph", PureToken.UNIQUE, node="Node", properties=["name"]
        )

    @pytest.mark.min_module_version("graph", "2.12")
    async def test_mandatory_constraint_relationship(self, client: Redis):
        assert await client.graph.constraint_create(
            "graph", PureToken.MANDATORY, relationship="EDGE", properties=["type"]
        )

        with pytest.raises(ResponseError, match="mandatory constraint violation"):
            await client.graph.query(
                "graph", "CREATE (:Node {name: 'fu'})-[:EDGE]->(:Node {name: 'bar'})"
            )
        await client.graph.query(
            "graph",
            "CREATE (:Node {name: 'fu'})-[:EDGE {type: 1}]->(:Node {name: 'bar'})",
        )
        assert await client.graph.constraint_drop(
            "graph", PureToken.MANDATORY, relationship="EDGE", properties=["type"]
        )

    async def test_config(self, client: Redis):
        all_config = await client.graph.config_get("*")
        assert all_config["TIMEOUT"] == 1000
        assert await client.graph.config_set("TIMEOUT", 2000)
        assert 2000 == await client.graph.config_get("TIMEOUT")
        assert await client.graph.config_set("TIMEOUT", 1000)

    async def test_profile(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        profile = await client.graph.profile(
            "graph", "MATCH (n)-[r]->(m) RETURN n, r, m", timeout=100
        )
        assert "All Node Scan" in profile[-1]
        assert "Records produced: 1" in profile[0]
        assert "Records produced: 2" in profile[-1]

    async def test_explain(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        explain = await client.graph.explain("graph", "MATCH (n)-[r]->(m) RETURN n, r, m")
        assert "All Node Scan" in explain[-1]

    async def test_slowlog(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        logs = await client.graph.slowlog("graph")
        assert len(logs) == 1
        await client.graph.query("graph", "MATCH (n)-[r]->(m) RETURN n, r, m")
        logs = await client.graph.slowlog("graph")
        assert len(logs) == 5  # 1, create, 1, match, 3 procedure calls

    @pytest.mark.min_module_version("graph", "2.12.0")
    async def test_slowlog_reset(self, client: Redis):
        await client.graph.query("graph", "CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        logs = await client.graph.slowlog("graph")
        assert len(logs) == 1

        assert await client.graph.slowlog("graph", reset=True)

        logs = await client.graph.slowlog("graph")
        assert len(logs) == 0

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction):
        p = await client.pipeline(transaction=transaction)
        p.graph.query("graph", "CREATE (:Node {name: 'A'})")
        p.graph.query("graph", "MATCH (n) return n")
        assert (
            GraphQueryResult((), (), stats=ANY),
            GraphQueryResult(
                ("n",),
                ([GraphNode(id=0, labels={"Node"}, properties={"name": "A"})],),
                stats=ANY,
            ),
        ) == await p.execute()
