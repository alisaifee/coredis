from __future__ import annotations

from datetime import timedelta

from ..commands._utils import normalized_milliseconds
from ..commands._wrappers import ClusterCommandConfig
from ..commands.constants import CommandGroup, CommandName, NodeFlag
from ..response._callbacks import (
    ClusterEnsureConsistent,
    ClusterMergeSets,
    ListCallback,
    SetCallback,
    SimpleStringCallback,
)
from ..tokens import PrefixToken, PureToken
from ..typing import (
    AnyStr,
    CommandArgList,
    Dict,
    KeyT,
    List,
    Literal,
    Optional,
    Parameters,
    ResponsePrimitive,
    Set,
    StringT,
    Tuple,
    Union,
    ValueT,
)
from .base import Module, ModuleGroup, module_command
from .response._callbacks.graph import (
    ConfigGetCallback,
    GraphSlowLogCallback,
    QueryCallback,
)
from .response.types import GraphQueryResult, GraphSlowLogInfo


class RedisGraph(Module[AnyStr]):
    NAME = "graph"
    FULL_NAME = "RedisGraph"
    DESCRIPTION = """RedisGraph is a queryable Property Graph database that uses sparse matrices 
to represent the adjacency matrix in graphs and linear algebra to query the graph.
    """
    DOCUMENTATION_URL = "https://redis.io/docs/stack/graph/"


class Graph(ModuleGroup[AnyStr]):
    MODULE = RedisGraph
    COMMAND_GROUP = CommandGroup.GRAPH

    @module_command(
        CommandName.GRAPH_QUERY,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def query(
        self,
        graph: KeyT,
        query: StringT,
        timeout: Optional[Union[int, timedelta]] = None,
    ) -> GraphQueryResult[AnyStr]:
        """
        Executes the given query against a specified graph

        :param graph: The name of the graph to query.
        :param query: The query to execute.
        :param timeout: The maximum amount of time (milliseconds) to wait for the query to complete
        :return: The result set of the executed query.
        """
        pieces: CommandArgList = [graph, query]

        if timeout is not None:
            pieces.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])

        pieces.append(b"--compact")
        return await self.execute_module_command(
            CommandName.GRAPH_QUERY,
            *pieces,
            callback=QueryCallback[AnyStr](graph),
            query=query,
        )

    @module_command(
        CommandName.GRAPH_RO_QUERY,
        module=MODULE,
        version_introduced="2.2.8",
        group=COMMAND_GROUP,
    )
    async def ro_query(
        self,
        graph: KeyT,
        query: StringT,
        timeout: Optional[Union[int, timedelta]] = None,
    ) -> GraphQueryResult[AnyStr]:
        """
        Executes a given read only query against a specified graph

        :param graph: The name of the graph to query.
        :param query: The query to execute.
        :param timeout: The maximum amount of time (milliseconds) to wait for the query to complete.
        :return: The result set for the read-only query or an error if a write query was given.
        """
        pieces: CommandArgList = [graph, query]
        if timeout is not None:
            pieces.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        pieces.append(b"--compact")

        return await self.execute_module_command(
            CommandName.GRAPH_RO_QUERY,
            *pieces,
            callback=QueryCallback[AnyStr](graph),
            query=query,
        )

    @module_command(
        CommandName.GRAPH_DELETE,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def delete(self, graph: KeyT) -> bool:
        """
        Completely removes the graph and all of its entities

        Deletes the entire graph and all of its entities.

        :param graph: The name of the graph to be deleted.
        """

        return await self.execute_module_command(
            CommandName.GRAPH_DELETE,
            graph,
            callback=SimpleStringCallback(
                prefix_match=True, ok_values={"Graph removed"}
            ),
        )

    @module_command(
        CommandName.GRAPH_EXPLAIN,
        module=MODULE,
        version_introduced="2.0.0",
        group=COMMAND_GROUP,
    )
    async def explain(self, graph: KeyT, query: StringT) -> List[AnyStr]:
        """

        Constructs a query execution plan for the given :paramref:`graph` and
        :paramref:`query`, but does not execute it.

        :param graph: The name of the graph to execute the query on.
        :param query: The query to construct the execution plan for.

        :return: A list of strings representing the query execution plan.
        """

        return await self.execute_module_command(
            CommandName.GRAPH_EXPLAIN, graph, query, callback=ListCallback[AnyStr]()
        )

    @module_command(
        CommandName.GRAPH_PROFILE,
        module=MODULE,
        version_introduced="2.0.0",
        group=COMMAND_GROUP,
    )
    async def profile(
        self,
        graph: KeyT,
        query: StringT,
        timeout: Optional[Union[int, timedelta]] = None,
    ) -> List[AnyStr]:
        """
        Executes a query and returns an execution plan augmented with metrics for each
        operation's execution

        :param graph: The name of the graph to execute the query on.
        :param query: The query to execute and return a profile for.
        :param timeout: Optional timeout for the query execution in milliseconds.
        :return: A string representation of a query execution plan, with details on results produced
         by and time spent in each operation.
        """
        pieces: CommandArgList = [graph, query]
        if timeout is not None:
            pieces.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        return await self.execute_module_command(
            CommandName.GRAPH_PROFILE, *pieces, callback=ListCallback[AnyStr]()
        )

    @module_command(
        CommandName.GRAPH_SLOWLOG,
        module=MODULE,
        version_introduced="2.0.12",
        arguments={"reset": {"version_introduced": "2.12.0"}},
        group=COMMAND_GROUP,
    )
    async def slowlog(
        self, graph: KeyT, reset: bool = False
    ) -> Union[Tuple[GraphSlowLogInfo, ...], bool]:
        """
        Returns a list containing up to 10 of the slowest queries issued against the given graph

        :param graph: The name of the graph
        :param reset: If ``True``, the slowlog will be reset
        :return: The slowlog for the given graph or ``True`` if the slowlog was reset
        """
        pieces: CommandArgList = [graph]
        if reset:
            pieces.append(PureToken.RESET)
            return await self.execute_module_command(
                CommandName.GRAPH_SLOWLOG,
                *pieces,
                callback=SimpleStringCallback(),
            )
        else:
            return await self.execute_module_command(
                CommandName.GRAPH_SLOWLOG,
                *pieces,
                callback=GraphSlowLogCallback(),
            )

    @module_command(
        CommandName.GRAPH_CONFIG_GET,
        module=MODULE,
        version_introduced="2.2.11",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    async def config_get(
        self, name: StringT
    ) -> Union[Dict[AnyStr, ResponsePrimitive], ResponsePrimitive]:
        """
        Retrieves a RedisGraph configuration

        :param name: The name of the configuration parameter to retrieve.
        :return: The value of the configuration parameter. If :paramref:`name`
         is ``*``, a mapping of all configuration parameters to their values
        """
        return await self.execute_module_command(
            CommandName.GRAPH_CONFIG_GET,
            name,
            callback=ConfigGetCallback[AnyStr](),
        )

    @module_command(
        CommandName.GRAPH_CONFIG_SET,
        module=MODULE,
        version_introduced="2.2.11",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent[AnyStr](),
        ),
    )
    async def config_set(self, name: StringT, value: ValueT) -> bool:
        """
        Updates a RedisGraph configuration

        :param name: The name of the configuration parameter to set.
        :param value: The value to set the configuration parameter to.
        :return: True if the configuration parameter was set successfully, False otherwise.
        """
        return await self.execute_module_command(
            CommandName.GRAPH_CONFIG_SET, name, value, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.GRAPH_LIST,
        module=MODULE,
        version_introduced="2.4.3",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets[AnyStr](),
        ),
    )
    async def list(self) -> Set[AnyStr]:
        """
        Lists all graph keys in the keyspace

        :return: A list of graph keys in the keyspace.
        """

        return await self.execute_module_command(
            CommandName.GRAPH_LIST, callback=SetCallback[AnyStr]()
        )

    @module_command(
        CommandName.GRAPH_CONSTRAINT_DROP,
        module=MODULE,
        version_introduced="2.12.0",
        group=COMMAND_GROUP,
    )
    async def constraint_drop(
        self,
        graph: KeyT,
        type: Literal[PureToken.MANDATORY, PureToken.UNIQUE],
        node: Optional[StringT] = None,
        relationship: Optional[StringT] = None,
        properties: Optional[Parameters[StringT]] = None,
    ) -> bool:
        """
        Deletes a constraint from specified graph

        :param graph: The name of the RedisGraph.
        :param type: The type of constraint to drop.
        :param node: The name of the node to drop the constraint from
        :param relationship: The name of the relationship to drop the constraint from
        :param properties: The properties to drop the constraint from

        :return: True if the constraint was successfully dropped, False otherwise.
        """
        pieces: CommandArgList = [graph, type]
        if node is not None:
            pieces.extend([PrefixToken.NODE, node])
        if relationship is not None:
            pieces.extend([PrefixToken.RELATIONSHIP, relationship])
        if properties:
            _props: List[StringT] = list(properties)
            pieces.extend([PrefixToken.PROPERTIES, len(_props), *_props])

        return await self.execute_module_command(
            CommandName.GRAPH_CONSTRAINT_DROP,
            *pieces,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.GRAPH_CONSTRAINT_CREATE,
        module=MODULE,
        version_introduced="2.12.0",
        group=COMMAND_GROUP,
    )
    async def constraint_create(
        self,
        graph: KeyT,
        type: Literal[PureToken.MANDATORY, PureToken.UNIQUE],
        node: Optional[StringT] = None,
        relationship: Optional[StringT] = None,
        properties: Optional[Parameters[StringT]] = None,
    ) -> bool:
        """
        Creates a constraint on specified graph


        :param graph: The name of the graph.
        :param type: The type of constraint to create.
        :param node: The label of the node to apply the constraint to.
        :param relationship: The type of relationship to apply the constraint to.
        :param properties: The properties to apply the constraint to.
        :return: True if the constraint was created successfully, False otherwise.
        """
        pieces: CommandArgList = [graph, type]

        if node is not None:
            pieces.extend([PrefixToken.NODE, node])
        if relationship is not None:
            pieces.extend([PrefixToken.RELATIONSHIP, relationship])
        if properties:
            _props: List[StringT] = list(properties)
            pieces.extend([PrefixToken.PROPERTIES, len(_props), *_props])
        return await self.execute_module_command(
            CommandName.GRAPH_CONSTRAINT_CREATE,
            *pieces,
            callback=SimpleStringCallback(ok_values={"PENDING"}),
        )
