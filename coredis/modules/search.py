from __future__ import annotations

import dataclasses
import itertools
from datetime import timedelta

from deprecated.sphinx import versionadded

from ..commands._utils import normalized_milliseconds, normalized_seconds
from ..commands._wrappers import ClusterCommandConfig
from ..commands.constants import CommandGroup, CommandName, NodeFlag
from ..response._callbacks import (
    AnyStrCallback,
    ClusterEnsureConsistent,
    ClusterMergeSets,
    DictCallback,
    IntCallback,
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
    Mapping,
    Optional,
    Parameters,
    ResponsePrimitive,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Union,
    ValueT,
)
from .base import Module, ModuleGroup, module_command
from .response._callbacks.search import (
    AggregationResultCallback,
    SearchConfigCallback,
    SearchResultCallback,
    SpellCheckCallback,
    SpellCheckResult,
)
from .response.types import SearchAggregationResult, SearchResult


class RediSearch(Module[AnyStr]):
    NAME = "search"
    FULL_NAME = "RediSearch"
    DESCRIPTION = """RedisSearch is a Redis module that enables querying, secondary 
indexing, and full-text search for Redis. These features enable multi-field queries, 
aggregation, exact phrase matching, numeric filtering, geo filtering and vector 
similarity semantic search on top of text queries."""
    DOCUMENTATION_URL = "https://redis.io/docs/stack/search"


@dataclasses.dataclass
class Field:
    """
    Field definition to be used in :meth:`~coredis.modules.Search.create` &
    :meth:`~coredis.modules.Search.alter`

    For more details refer to the documentation for
    `FT.CREATE <https://redis.io/commands/ft.create/>`__
    """

    #: Name of the field. For hashes, this is a field name within the hash.
    #:  For JSON, this is a JSON Path expression.
    name: StringT
    #: Type of field
    type: Literal[
        PureToken.TEXT,
        PureToken.TAG,
        PureToken.NUMERIC,
        PureToken.GEO,
        PureToken.VECTOR,
    ]

    #: Defines the alias associated to :paramref:`name`.
    #: For example, you can use this feature to alias a complex
    #: JSONPath expression with more memorable (and easier to type) name.
    alias: Optional[StringT] = None
    #: Whether to optimize for sorting.
    sortable: Optional[bool] = None
    #: Whether to use the unnormalized form of the field for sorting.
    unf: Optional[bool] = None
    #: Whether to disable stemming for this field.
    nostem: Optional[bool] = None
    #: Skip indexing this field
    noindex: Optional[bool] = None
    #: Phonetic algorithm to use for this field.
    phonetic: Optional[StringT] = None
    #: Weight of this field in the document's ranking. The default is 1.0.
    weight: Optional[Union[int, float]] = None
    #: Separator to use for splitting tags if the field is of
    #: type :attr:`~coredis.PureToken.TAG`.
    separator: Optional[StringT] = None
    #: For fields of type :attr:`~coredis.PureToken.TAG`,
    #: keeps the original letter cases of the tags. If not specified,
    #: the characters are converted to lowercase.
    casesensitive: Optional[bool] = None
    #: For fields of type :attr:`~coredis.PureToken.TAG` &
    #: :attr:`~coredis.PureToken.TEXT`, keeps a suffix trie with all
    #: terms which match the suffix. It is used to optimize contains ``(foo)``
    #: and suffix ``(*foo)`` queries. Otherwise, a brute-force search on the trie
    #: is performed. If suffix trie exists for some fields, these queries will
    #: be disabled for other fields
    withsuffixtrie: Optional[bool] = None
    #: The algorithm to use for indexing if the field is of type
    #: :attr:`~coredis.PureToken.VECTOR`.
    #: For more details refer to the
    #: `Vector similarity <https://redis.io/docs/stack/search/reference/vectors/>`__
    #: section of the RediSearch documentation.
    algorithm: Optional[Literal["FLAT", "HSNW"]] = None
    #: A dictionary of attributes to be used with the :paramref:`algorithm` specified.
    #: For more details refer to the
    #: `Creation attributes per algorithm <https://redis.io/docs/stack/search/reference/vectors/#creation-attributes-per-algorithm>`__
    #: section of the RediSearch documentation.
    attributes: Optional[Dict[StringT, ValueT]] = None

    @property
    def args(self) -> Tuple[ValueT, ...]:
        args: CommandArgList = [self.name]
        if self.alias:
            args += [PrefixToken.AS, self.alias]
        args += [self.type]
        if self.type == PureToken.VECTOR:
            assert self.algorithm

            args += [self.algorithm]
            if self.attributes:
                _attributes: List[ValueT] = list(
                    itertools.chain(*self.attributes.items())
                )
                args += [len(_attributes)]
                args += _attributes

        if self.sortable:
            args += [PureToken.SORTABLE]
        if self.unf:
            args += [PureToken.UNF]
        if self.nostem:
            args += [b"NOSTEM"]
        if self.noindex:
            args += [PureToken.NOINDEX]
        if self.phonetic:
            args += [b"PHONETIC", self.phonetic]
        if self.weight:
            args += [b"WEIGHT", self.weight]
        if self.separator:
            args += [PrefixToken.SEPARATOR, self.separator]
        if self.casesensitive:
            args += [b"CASESENSITIVE"]
        if self.withsuffixtrie:
            args += [PureToken.WITHSUFFIXTRIE]
        return tuple(args)


@dataclasses.dataclass
class Reduce:
    """
    Reduce definition to be used with :paramref:`~coredis.modules.Search.aggregate.transformations`
    to define ``REDUCE`` steps in :meth:`~coredis.modules.Search.aggregate`

    For more details refer to `GroupBy Reducers <https://redis.io/docs/stack/search/reference/aggregations/#groupby-reducers>`__
    in the RediSearch documentation.
    """

    #: The name of the reducer function
    function: StringT
    #: The arguments to the reducer function
    parameters: Optional[Parameters[ValueT]] = None
    #: The alias to assign to the result of the reducer function
    alias: Optional[StringT] = None

    @property
    def args(self) -> CommandArgList:
        args: CommandArgList = [self.function]
        if self.parameters:
            args.extend(self.parameters)
        if self.alias:
            args.extend([PrefixToken.AS, self.alias])
        return args


@dataclasses.dataclass
class Group:
    """
    Group definition to be used with :paramref:`~coredis.modules.Search.aggregate.transformations`
    to specify ``GROUPBY`` steps in :meth:`~coredis.modules.Search.aggregate`

    For more details refer to
    `Aggregations <https://redis.io/docs/stack/search/reference/aggregations>`__
    in the RediSearch documentation.
    """

    #: The field to group by
    by: Union[StringT, Parameters[StringT]]
    #: The reducers to apply to each group
    reducers: Optional[Parameters[Reduce]] = None

    @property
    def args(self) -> CommandArgList:
        args: CommandArgList = [PrefixToken.GROUPBY]
        if isinstance(self.by, (bytes, str)):
            args.extend([1, self.by])
        else:
            bies: List[StringT] = list(self.by)
            args.extend([len(bies), *bies])
        for reducer in self.reducers or []:
            args.append(PrefixToken.REDUCE)
            args.extend(reducer.args)

        return args


@dataclasses.dataclass
class Apply:
    """
    Apply definition to be used with :paramref:`~coredis.modules.Search.aggregate.transformations`
    to specify ``APPLY`` steps in :meth:`~coredis.modules.Search.aggregate`

    For more details refer to
    `APPLY expressions <https://redis.io/docs/stack/search/reference/aggregations/#apply-expressions>`__
    in the RediSearch documentation.
    """

    #: The expression to apply
    function: StringT
    #: The alias to assign to the result of the expression
    alias: StringT

    @property
    def args(self) -> CommandArgList:
        return [PrefixToken.APPLY, self.function, PrefixToken.AS, self.alias]


@dataclasses.dataclass
class Filter:
    """
    Filter definition to be used with :paramref:`~coredis.modules.Search.aggregate.transformations`
    to specify ``FILTER`` steps in :meth:`~coredis.modules.Search.aggregate`

    For more details refer to
    `FILTER expressions <https://redis.io/docs/stack/search/reference/aggregations/#filter-expressions>`__
    in the RediSearch documentation.
    """

    #: The filter expression
    expression: StringT

    @property
    def args(self) -> CommandArgList:
        return [PrefixToken.FILTER, self.expression]


@versionadded(version="4.12")
class Search(ModuleGroup[AnyStr]):
    MODULE = RediSearch
    COMMAND_GROUP = CommandGroup.SEARCH

    @module_command(
        CommandName.FT_CREATE,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def create(
        self,
        index: KeyT,
        schema: Parameters[Field],
        *,
        on: Optional[Literal[PureToken.HASH, PureToken.JSON]] = None,
        prefixes: Optional[Parameters[StringT]] = None,
        filter_expression: Optional[StringT] = None,
        language: Optional[StringT] = None,
        language_field: Optional[StringT] = None,
        score: Optional[Union[int, float]] = None,
        score_field: Optional[StringT] = None,
        payload_field: Optional[StringT] = None,
        maxtextfields: Optional[bool] = None,
        temporary: Optional[Union[int, timedelta]] = None,
        nooffsets: Optional[bool] = None,
        nohl: Optional[bool] = None,
        nofields: Optional[bool] = None,
        nofreqs: Optional[bool] = None,
        stopwords: Optional[Parameters[StringT]] = None,
        skipinitialscan: Optional[bool] = None,
    ) -> bool:
        """
        Creates an index with the given spec


        :param index: The name of the index to create.
        :param schema: A list of schema fields
        :param on: The type of Redis key to index.
        :param prefixes: A list of key prefixes to index.
        :param filter_expression: A filter expression to apply to the index.
        :param language: The default language to use for text fields.
        :param language_field: The name of the field to use for language detection.
        :param score: The default score to use for documents.
        :param score_field: The name of the field to use for document scoring.
        :param payload_field: The name of the field to use for document payloads.
        :param maxtextfields: If ``True``, the maximum number of text fields will be used.
        :param temporary: If specified, the index will be temporary and
         will expire after the given number of seconds.
        :param nooffsets: If ``True``, term offsets will not be stored.
        :param nohl: If ``True``, search results will not include highlighted snippets.
        :param nofields: If ``True``, search results will not include field values.
        :param nofreqs: If ``True``, term frequencies will not be stored.
        :param stopwords: A list of stopwords to ignore.
        :param skipinitialscan: If ``True``, the initial scan of the index will be skipped.
        """
        pieces: CommandArgList = [index]
        if on:
            pieces.extend([PrefixToken.ON, on])

        if prefixes:
            _prefixes: List[StringT] = list(prefixes)
            pieces.extend([PrefixToken.PREFIX, len(_prefixes), *_prefixes])
        if filter_expression:
            pieces.extend([PrefixToken.FILTER, filter_expression])
        if language:
            pieces.extend([PrefixToken.LANGUAGE, language])
        if language_field:
            pieces.extend([PrefixToken.LANGUAGE_FIELD, language_field])
        if score:
            pieces.extend([PrefixToken.SCORE, score])
        if score_field:
            pieces.extend([PrefixToken.SCORE_FIELD, score_field])
        if payload_field:
            pieces.extend([PrefixToken.PAYLOAD_FIELD, payload_field])
        if maxtextfields:
            pieces.append(PureToken.MAXTEXTFIELDS)
        if temporary:
            pieces.extend([PrefixToken.TEMPORARY, normalized_seconds(temporary)])
        if nooffsets:
            pieces.append(PureToken.NOOFFSETS)
        if nohl:
            pieces.append(PureToken.NOHL)
        if nofields:
            pieces.append(PureToken.NOFIELDS)
        if nofreqs:
            pieces.append(PureToken.NOFREQS)
        if stopwords:
            _stop: List[StringT] = list(stopwords)
            pieces.extend([PrefixToken.STOPWORDS, len(_stop), *_stop])
        if skipinitialscan:
            pieces.append(PureToken.SKIPINITIALSCAN)

        field_args: CommandArgList = [PureToken.SCHEMA]
        for field in schema:
            field_args.extend(field.args)
        pieces.extend(field_args)

        return await self.execute_module_command(
            CommandName.FT_CREATE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_INFO,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def info(self, index: KeyT) -> Dict[AnyStr, ResponseType]:
        """
        Returns information and statistics on the index

        :param index: The name of the index.
        """
        return await self.execute_module_command(
            CommandName.FT_INFO,
            index,
            callback=DictCallback[AnyStr, ResponseType](
                recursive=[
                    "attributes",
                    "index_definition",
                    "gc_stats",
                    "cursor_stats",
                    "dialect_stats",
                ]
            ),
        )

    @module_command(
        CommandName.FT_EXPLAIN,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    async def explain(
        self, index: KeyT, query: StringT, dialect: Optional[int] = None
    ) -> AnyStr:
        """
        Returns the execution plan for a complex query

        :param index: The name of the index to query.
        :param query: The query to explain.
        :param dialect: Query dialect to use.
        """
        pieces: CommandArgList = [index, query]
        if dialect:
            pieces.extend([PrefixToken.DIALECT, dialect])
        return await self.execute_module_command(
            CommandName.FT_EXPLAIN, *pieces, callback=AnyStrCallback[AnyStr]()
        )

    @module_command(
        CommandName.FT_ALTER,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def alter(
        self,
        index: KeyT,
        field: Field,
        skipinitialscan: Optional[bool] = None,
    ) -> bool:
        """
        Adds a new field to the index

        :param index: The name of the index to alter.
        :param field: The new field to add
        :param skipinitialscan: If ``True``, skip the initial scan and indexing.

        """
        pieces: CommandArgList = [index]
        if skipinitialscan:
            pieces.append(PureToken.SKIPINITIALSCAN)
        pieces.extend([PureToken.SCHEMA, PureToken.ADD, *field.args])

        return await self.execute_module_command(
            CommandName.FT_ALTER, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_DROPINDEX,
        module=MODULE,
        version_introduced="2.0.0",
        group=COMMAND_GROUP,
    )
    async def dropindex(self, index: KeyT, delete_docs: Optional[bool] = None) -> bool:
        """
        Deletes the index

        :param index: The name of the index to delete.
        :param delete_docs: If ``True``, delete the documents associated with the index.
        """
        pieces: CommandArgList = [index]
        if delete_docs:
            pieces.append(PureToken.DELETE_DOCS)

        return await self.execute_module_command(
            CommandName.FT_DROPINDEX, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_ALIASADD,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def aliasadd(self, alias: StringT, index: KeyT) -> bool:
        """
        Adds an alias to the index

        :param alias: The alias to be added to the index.
        :param index: The index to which the alias will be added.
        """

        return await self.execute_module_command(
            CommandName.FT_ALIASADD, alias, index, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_ALIASUPDATE,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def aliasupdate(self, alias: StringT, index: KeyT) -> bool:
        """
        Adds or updates an alias to the index

        :param alias: The alias to be added to an index.
        :param index: The index to which the alias will be added.
        """

        return await self.execute_module_command(
            CommandName.FT_ALIASUPDATE, alias, index, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_ALIASDEL,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def aliasdel(self, alias: StringT) -> bool:
        """
        Deletes an alias from the index

        :param alias: The index alias to be removed.
        """

        return await self.execute_module_command(
            CommandName.FT_ALIASDEL, alias, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_TAGVALS,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def tagvals(self, index: KeyT, field_name: StringT) -> Set[AnyStr]:
        """
        Returns the distinct tags indexed in a Tag field

        :param index: The name of the index.
        :param field_name: Name of a Tag field defined in the schema.
        """

        return await self.execute_module_command(
            CommandName.FT_TAGVALS, index, field_name, callback=SetCallback[AnyStr]()
        )

    @module_command(
        CommandName.FT_SYNUPDATE,
        module=MODULE,
        version_introduced="1.2.0",
        group=COMMAND_GROUP,
    )
    async def synupdate(
        self,
        index: KeyT,
        synonym_group: StringT,
        terms: Parameters[StringT],
        skipinitialscan: Optional[bool] = None,
    ) -> bool:
        """
        Creates or updates a synonym group with additional terms

        :param index: The name of the index.
        :param synonym_group: The ID of the synonym group to update.
        :param terms: A list of terms to add to the synonym group.
        :param skipinitialscan: If ``True``, only documents indexed after the
         update will be affected.

        """
        pieces: CommandArgList = [index, synonym_group]
        if skipinitialscan:
            pieces.append(PureToken.SKIPINITIALSCAN)
        pieces.extend(terms)
        return await self.execute_module_command(
            CommandName.FT_SYNUPDATE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_SYNDUMP,
        module=MODULE,
        version_introduced="1.2.0",
        group=COMMAND_GROUP,
    )
    async def syndump(self, index: KeyT) -> Dict[AnyStr, List[AnyStr]]:
        """
        Dumps the contents of a synonym group

        :param index: The name of the index.
        """

        return await self.execute_module_command(
            CommandName.FT_SYNDUMP, index, callback=DictCallback[AnyStr, List[AnyStr]]()
        )

    @module_command(
        CommandName.FT_SPELLCHECK,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    async def spellcheck(
        self,
        index: KeyT,
        query: StringT,
        distance: Optional[int] = None,
        include: Optional[StringT] = None,
        exclude: Optional[StringT] = None,
        dialect: Optional[int] = None,
    ) -> SpellCheckResult:
        """
        Performs spelling correction on a query, returning suggestions for misspelled terms

        :param index: The name of the index with the indexed terms.
        :param query: The search query.
        :param distance: Maximum Levenshtein distance for spelling suggestions
        :param include: Specifies an inclusion of a custom dictionary
        :param exclude: Specifies an exclusion of a custom dictionary
        :param dialect: The query dialect to use.
        """
        pieces: CommandArgList = [index, query]
        if distance:
            pieces.extend([PrefixToken.DISTANCE, distance])
        if exclude:
            pieces.extend([PrefixToken.TERMS, PureToken.EXCLUDE, exclude])
        if include:
            pieces.extend([PrefixToken.TERMS, PureToken.INCLUDE, include])
        if dialect:
            pieces.extend([PrefixToken.DIALECT, dialect])
        return await self.execute_module_command(
            CommandName.FT_SPELLCHECK, *pieces, callback=SpellCheckCallback()
        )

    @module_command(
        CommandName.FT_DICTADD,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    async def dictadd(
        self,
        name: StringT,
        terms: Parameters[StringT],
    ) -> int:
        """
        Adds terms to a dictionary

        :param name: The name of the dictionary.
        :param terms: The terms to add to the dictionary.
        """
        pieces: CommandArgList = [name, *terms]

        return await self.execute_module_command(
            CommandName.FT_DICTADD, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_DICTDEL,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    async def dictdel(
        self,
        name: StringT,
        terms: Parameters[StringT],
    ) -> int:
        """
        Deletes terms from a dictionary

        :param name: The name of the dictionary.
        :param terms: The terms to delete from the dictionary.
        """
        pieces: CommandArgList = [name, *terms]

        return await self.execute_module_command(
            CommandName.FT_DICTDEL, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_DICTDUMP,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    async def dictdump(self, name: StringT) -> Set[AnyStr]:
        """
        Dumps all terms in the given dictionary

        :param name: The name of the dictionary to dump.
        """

        return await self.execute_module_command(
            CommandName.FT_DICTDUMP, name, callback=SetCallback[AnyStr]()
        )

    @module_command(
        CommandName.FT__LIST,
        module=MODULE,
        version_introduced="2.0.0",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets[AnyStr](),
        ),
    )
    async def list(self) -> Set[AnyStr]:
        """
        Returns a list of all existing indexes
        """
        return await self.execute_module_command(
            CommandName.FT__LIST, callback=SetCallback[AnyStr]()
        )

    @module_command(
        CommandName.FT_CONFIG_SET,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent[bool](),
        ),
    )
    async def config_set(self, option: StringT, value: ValueT) -> bool:
        """
        Sets runtime configuration options
        """

        return await self.execute_module_command(
            CommandName.FT_CONFIG_SET, option, value, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_CONFIG_GET,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    async def config_get(self, option: StringT) -> Dict[AnyStr, ResponsePrimitive]:
        """
        Retrieves runtime configuration options
        """

        return await self.execute_module_command(
            CommandName.FT_CONFIG_GET,
            option,
            callback=SearchConfigCallback[AnyStr](),
        )

    @module_command(
        CommandName.FT_SEARCH,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    async def search(
        self,
        index: KeyT,
        query: StringT,
        *,
        nocontent: Optional[bool] = None,
        verbatim: Optional[bool] = None,
        nostopwords: Optional[bool] = None,
        withscores: Optional[bool] = None,
        withpayloads: Optional[bool] = None,
        withsortkeys: Optional[bool] = None,
        numeric_filters: Optional[
            Mapping[
                StringT, Tuple[Union[int, float, StringT], Union[int, float, StringT]]
            ]
        ] = None,
        geo_filters: Optional[
            Mapping[
                StringT,
                Tuple[
                    Tuple[Union[int, float], Union[int, float]],
                    Union[int, float],
                    Literal[PureToken.KM, PureToken.M, PureToken.MI, PureToken.FT],
                ],
            ]
        ] = None,
        in_keys: Optional[Parameters[StringT]] = None,
        in_fields: Optional[Parameters[StringT]] = None,
        returns: Optional[Mapping[StringT, Optional[StringT]]] = None,
        summarize_fields: Optional[Parameters[StringT]] = None,
        summarize_frags: Optional[int] = None,
        summarize_length: Optional[int] = None,
        summarize_separator: Optional[StringT] = None,
        highlight_fields: Optional[Parameters[StringT]] = None,
        highlight_tags: Optional[Tuple[StringT, StringT]] = None,
        slop: Optional[int] = None,
        timeout: Optional[Union[int, timedelta]] = None,
        inorder: Optional[bool] = None,
        language: Optional[StringT] = None,
        expander: Optional[StringT] = None,
        scorer: Optional[StringT] = None,
        explainscore: Optional[bool] = None,
        payload: Optional[StringT] = None,
        sortby: Optional[StringT] = None,
        sort_order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        offset: Optional[int] = 0,
        limit: Optional[int] = None,
        parameters: Optional[Mapping[StringT, ValueT]] = None,
        dialect: Optional[int] = None,
    ) -> SearchResult[AnyStr]:
        """
        Searches the index with a textual query, returning either documents or just ids

        :param index: The name of the index to search.
        :param query: The text query to search.
        :param nocontent: If ``True``, returns the document ids and not the content.
        :param verbatim: If ``True``, does not try to use stemming for query expansion
         but searches the query terms verbatim.
        :param nostopwords: If ``True``, disables stopword filtering.
        :param withscores: If ``True``, also returns the relative internal score of each document.
        :param withpayloads: If ``True``, retrieves optional document payloads.
        :param withsortkeys: If ``True``, returns the value of the sorting key
        :param numeric_filters: A dictionary of numeric filters.
         Limits results to those having numeric values ranging between min and max,
        :param geo_filters: A dictionary of geo filters.
         Filters the results to a given radius from lon and lat.
         Radius is given as a number and units.
        :param in_keys: A list of keys to limit the result to.
         Non-existent keys are ignored, unless all the keys are non-existent.
        :param in_fields: Filters the results to those appearing only in specific
         attributes of the document, like title or URL.
        :param returns: A dictionary of attributes to return from the document. The values
         in the dictionary are used as an alias for the attribute name and if ``None``,
         the attribute will be returned as is.
        :param summarize_fields: A list of fields to summarize. If not ``None``,
         returns a summary of the fields.
        :param summarize_frags: The number of fragments to return in the summary.
        :param summarize_length: The length of each fragment in the summary.
        :param summarize_separator: The separator to use between fragments in the summary.
        :param highlight_fields: A list of fields to highlight.
        :param highlight_tags: A tuple of opening and closing tags to use for highlighting.
        :param slop: The number of words allowed between query terms.
        :param timeout: The timeout for the search query.
        :param inorder: If ``True``, requires that the query terms appear in the same order
         as in the query.
        :param language: The language to use for stemming.
        :param expander: The query expander to use.
        :param scorer: The scorer to use.
        :param explainscore: If ``True``, returns an explanation of the score.
        :param payload: The payload to return.
        :param sortby: The field to sort by.
        :param sort_order: The order to sort by.
        :param offset: The offset to start returning results from.
        :param limit: The maximum number of results to return.
        :param parameters: A dictionary of parameters to pass to the query.
        :param dialect: The query dialect to use.

        """
        pieces: CommandArgList = [index, query]
        if nocontent:
            pieces.append(PureToken.NOCONTENT)
        if verbatim:
            pieces.append(PureToken.VERBATIM)
        if nostopwords:
            pieces.append(PureToken.NOSTOPWORDS)
        if withscores:
            pieces.append(PureToken.WITHSCORES)
        if withpayloads:
            pieces.append(PureToken.WITHPAYLOADS)
        if withsortkeys:
            pieces.append(PureToken.WITHSORTKEYS)
        if numeric_filters:
            for field, numeric_filter in numeric_filters.items():
                pieces.extend(
                    [PrefixToken.FILTER, field, numeric_filter[0], numeric_filter[1]]
                )
        if geo_filters:
            for field, gfilter in geo_filters.items():
                pieces.extend(
                    [
                        PrefixToken.GEOFILTER,
                        field,
                        gfilter[0][0],
                        gfilter[0][1],
                        gfilter[1],
                        gfilter[2],
                    ]
                )
        if in_keys:
            _in_keys: List[StringT] = list(in_keys)
            pieces.extend([PrefixToken.INKEYS, len(_in_keys), *_in_keys])
        if in_fields:
            _in_fields: List[StringT] = list(in_fields)
            pieces.extend([PrefixToken.INFIELDS, len(_in_fields), *_in_fields])
        if returns:
            return_items: CommandArgList = []
            for identifier, property in returns.items():
                return_items.append(identifier)
                if property:
                    return_items.extend([PrefixToken.AS, property])
            pieces.extend([PrefixToken.RETURN, len(return_items), *return_items])
        if sortby:
            pieces.extend([PrefixToken.SORTBY, sortby])
            if sort_order:
                pieces.append(sort_order)
        if (
            summarize_fields
            or summarize_frags
            or summarize_length
            or summarize_separator
        ):
            pieces.append(PureToken.SUMMARIZE)
            if summarize_fields:
                _fields: List[StringT] = list(summarize_fields)
                pieces.extend([PrefixToken.FIELDS, len(_fields), *_fields])
            if summarize_frags:
                pieces.extend([PrefixToken.FRAGS, summarize_frags])
            if summarize_length:
                pieces.extend([PrefixToken.LEN, summarize_length])
            if summarize_separator:
                pieces.extend([PrefixToken.SEPARATOR, summarize_separator])
        if highlight_fields or highlight_tags:
            pieces.append(PureToken.HIGHLIGHT)
            if highlight_fields:
                _fields = list(highlight_fields)
                pieces.extend([PrefixToken.FIELDS, len(_fields), *_fields])
            if highlight_tags:
                pieces.extend([PureToken.TAGS, highlight_tags[0], highlight_tags[1]])
        if slop is not None:
            pieces.extend([PrefixToken.SLOP, slop])
        if timeout:
            pieces.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        if inorder:
            pieces.append(PureToken.INORDER)
        if language:
            pieces.extend([PrefixToken.LANGUAGE, language])
        if expander:  # noqa
            pieces.extend([PrefixToken.EXPANDER, expander])
        if scorer:  # noqa
            pieces.extend([PrefixToken.SCORER, scorer])
        if explainscore:
            pieces.append(PureToken.EXPLAINSCORE)
        if payload:
            pieces.extend([PrefixToken.PAYLOAD, payload])
        if limit is not None:
            pieces.extend([PrefixToken.LIMIT, offset or 0, limit])
        if parameters:
            _parameters: List[ValueT] = list(itertools.chain(*parameters.items()))
            pieces.extend([PureToken.PARAMS, len(_parameters), *_parameters])
        if dialect:
            pieces.extend([PrefixToken.DIALECT, dialect])

        return await self.execute_module_command(
            CommandName.FT_SEARCH,
            *pieces,
            callback=SearchResultCallback[AnyStr](),
            withscores=withscores,
            withpayloads=withpayloads,
            withsortkeys=withsortkeys,
            explainscore=explainscore,
            nocontent=nocontent,
        )

    @module_command(
        CommandName.FT_AGGREGATE,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    async def aggregate(
        self,
        index: KeyT,
        query: StringT,
        *,
        verbatim: Optional[bool] = None,
        load: Optional[
            Union[Literal["*"], Parameters[Union[StringT, Tuple[StringT, StringT]]]]
        ] = None,
        timeout: Optional[Union[int, timedelta]] = None,
        transforms: Optional[Parameters[Union[Group, Apply, Filter]]] = None,
        sortby: Optional[
            Mapping[StringT, Literal[PureToken.ASC, PureToken.DESC]]
        ] = None,
        sortby_max: Optional[int] = None,
        offset: Optional[int] = 0,
        limit: Optional[int] = None,
        with_cursor: Optional[bool] = None,
        cursor_read_size: Optional[int] = None,
        cursor_maxidle: Optional[Union[int, timedelta]] = None,
        parameters: Optional[Mapping[StringT, StringT]] = None,
        dialect: Optional[int] = None,
    ) -> SearchAggregationResult[AnyStr]:
        """
        Perform aggregate transformations on search results from a Redis index.

        :param index: Name of the Redis index to search.
        :param query: Base filtering query to retrieve documents.
        :param verbatim: If ``True``, search query terms verbatim.
        :param load: Load document attributes from the source document.
        :param timeout: Maximum time to wait for the query to complete.
        :param transforms: List of transformations to apply to the results.
        :param sortby: Sort the pipeline up to the point of SORTBY.
        :param sortby_max: Optimize sorting by sorting only for the n-largest elements.
        :param offset: Number of results to skip.
        :param limit: Maximum number of results to return.
        :param with_cursor: If ``True``, return a cursor for large result sets.
        :param cursor_read_size: Number of results to read from the cursor at a time.
        :param cursor_maxidle: Maximum idle time for the cursor.
        :param parameters: Additional parameters to pass to the query.
        :param dialect: Query dialect to use.

        :return: Aggregated search results from the Redis index.

        """
        pieces: CommandArgList = [index, query]
        if verbatim:
            pieces.append(PureToken.VERBATIM)
        if timeout:
            pieces.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        if load:
            pieces.append(PrefixToken.LOAD)
            if isinstance(load, (bytes, str)):
                pieces.append(load)
            else:
                _load_fields: List[StringT] = []
                for field in load:
                    if isinstance(field, (bytes, str)):
                        _load_fields.append(field)
                    else:
                        _load_fields.extend([field[0], PrefixToken.AS, field[1]])

                pieces.extend([len(_load_fields), *_load_fields])

        if transforms:
            for step in transforms:
                pieces.extend(step.args)

        if sortby:
            pieces.append(PrefixToken.SORTBY)
            pieces.append(len(sortby) * 2)
            for field, order in sortby.items():
                pieces.extend([field, order])
            if sortby_max:
                pieces.extend([PrefixToken.MAX, sortby_max])

        if limit is not None:
            pieces.extend([PrefixToken.LIMIT, offset or 0, limit])

        if with_cursor:
            pieces.append(PureToken.WITHCURSOR)
            if cursor_read_size:
                pieces.extend([PrefixToken.COUNT, cursor_read_size])
            if cursor_maxidle:
                pieces.extend(
                    [PrefixToken.MAXIDLE, normalized_milliseconds(cursor_maxidle)]
                )
        if parameters:
            _parameters: List[StringT] = list(itertools.chain(*parameters.items()))
            pieces.extend([PureToken.PARAMS, len(_parameters), *_parameters])
        if dialect:
            pieces.extend([PrefixToken.DIALECT, dialect])
        return await self.execute_module_command(
            CommandName.FT_AGGREGATE,
            *pieces,
            callback=AggregationResultCallback[AnyStr](),
            with_cursor=with_cursor,
            dialect=dialect,
        )

    @module_command(
        CommandName.FT_CURSOR_READ,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
    )
    async def cursor_read(
        self, index: KeyT, cursor_id: int, count: Optional[int] = None
    ) -> SearchAggregationResult[AnyStr]:
        """
        Reads from a cursor
        """
        pieces: CommandArgList = [index, cursor_id]
        if count:
            pieces.extend([PrefixToken.COUNT, count])

        return await self.execute_module_command(
            CommandName.FT_CURSOR_READ,
            *pieces,
            callback=AggregationResultCallback[AnyStr](),
            with_cursor=True,
        )

    @module_command(
        CommandName.FT_CURSOR_DEL,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
    )
    async def cursor_del(self, index: KeyT, cursor_id: int) -> bool:
        """
        Deletes a cursor

        """
        pieces: CommandArgList = [index, cursor_id]

        return await self.execute_module_command(
            CommandName.FT_CURSOR_DEL, *pieces, callback=SimpleStringCallback()
        )
