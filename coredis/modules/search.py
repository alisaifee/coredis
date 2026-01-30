from __future__ import annotations

import dataclasses
import itertools
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import timedelta

from deprecated.sphinx import versionadded

from ..commands._utils import normalized_milliseconds, normalized_seconds
from ..commands._validators import mutually_exclusive_parameters, mutually_inclusive_parameters
from ..commands._wrappers import ClusterCommandConfig
from ..commands.constants import CommandGroup, CommandName, NodeFlag
from ..commands.request import CommandRequest
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
    KeyT,
    Literal,
    Mapping,
    Parameters,
    ResponsePrimitive,
    ResponseType,
    StringT,
    ValueT,
)
from .base import Module, ModuleGroup, module_command
from .response._callbacks.search import (
    AggregationResultCallback,
    HybridSearchCallback,
    SearchConfigCallback,
    SearchResultCallback,
    SpellCheckCallback,
)
from .response.types import HybridResult, SearchAggregationResult, SearchResult


class RediSearch(Module[AnyStr]):
    NAME = "search"
    FULL_NAME = "RediSearch"
    DESCRIPTION = """RedisSearch is a Redis module that enables querying, secondary
indexing, and full-text search for Redis. These features enable multi-field queries,
aggregation, exact phrase matching, numeric filtering, geo filtering and vector
similarity semantic search on top of text queries."""
    DOCUMENTATION_URL = "https://redis.io/docs/develop/ai/search-and-query"


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
    alias: StringT | None = None
    #: Whether to optimize for sorting.
    sortable: bool | None = None
    #: Whether to use the unnormalized form of the field for sorting.
    unf: bool | None = None
    #: Whether to disable stemming for this field.
    nostem: bool | None = None
    #: Skip indexing this field
    noindex: bool | None = None
    #: Phonetic algorithm to use for this field.
    phonetic: StringT | None = None
    #: Weight of this field in the document's ranking. The default is 1.0.
    weight: int | float | None = None
    #: Separator to use for splitting tags if the field is of
    #: type :attr:`~coredis.PureToken.TAG`.
    separator: StringT | None = None
    #: For fields of type :attr:`~coredis.PureToken.TAG`,
    #: keeps the original letter cases of the tags. If not specified,
    #: the characters are converted to lowercase.
    casesensitive: bool | None = None
    #: For fields of type :attr:`~coredis.PureToken.TAG` &
    #: :attr:`~coredis.PureToken.TEXT`, keeps a suffix trie with all
    #: terms which match the suffix. It is used to optimize contains ``(foo)``
    #: and suffix ``(*foo)`` queries. Otherwise, a brute-force search on the trie
    #: is performed. If suffix trie exists for some fields, these queries will
    #: be disabled for other fields
    withsuffixtrie: bool | None = None
    #: The algorithm to use for indexing if the field is of type
    #: :attr:`~coredis.PureToken.VECTOR`.
    #: For more details refer to the
    #: `Vector search concepts <https://redis.io/docs/develop/ai/search-and-query/vectors>`__
    #: section of the RediSearch documentation.
    algorithm: Literal["FLAT", "HSNW"] | None = None
    #: A dictionary of attributes to be used with the :paramref:`algorithm` specified.
    #: For more details refer to the
    #: `Vector index <https://redis.io/docs/develop/ai/search-and-query/vectors/#create-a-vector-index>`__
    #: section of the RediSearch documentation.
    attributes: dict[StringT, ValueT] | None = None

    @property
    def args(self) -> tuple[ValueT, ...]:
        args: CommandArgList = [self.name]
        if self.alias:
            args += [PrefixToken.AS, self.alias]
        args += [self.type]
        if self.type == PureToken.VECTOR:
            assert self.algorithm

            args += [self.algorithm]
            if self.attributes:
                _attributes: list[ValueT] = list(itertools.chain(*self.attributes.items()))
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

    For more details refer to `GroupBy Reducers <https://redis.io/docs/develop/ai/search-and-query/advanced-concepts/aggregations/#groupby-reducers>`__
    in the RediSearch documentation.
    """

    #: The name of the reducer function
    function: StringT
    #: The arguments to the reducer function
    parameters: Parameters[ValueT] | None = None
    #: The alias to assign to the result of the reducer function
    alias: StringT | None = None

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
    `Aggregations <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/aggregations/>`__
    in the RediSearch documentation.
    """

    #: The field to group by
    by: StringT | Parameters[StringT]
    #: The reducers to apply to each group
    reducers: Parameters[Reduce] | None = None

    @property
    def args(self) -> CommandArgList:
        args: CommandArgList = [PrefixToken.GROUPBY]
        if isinstance(self.by, (bytes, str)):
            args.extend([1, self.by])
        else:
            bies: list[StringT] = list(self.by)
            args.extend([len(bies), *bies])
        for reducer in self.reducers or []:
            args.append(PureToken.REDUCE)
            args.extend(reducer.args)

        return args


@dataclasses.dataclass
class Apply:
    """
    Apply definition to be used with :paramref:`~coredis.modules.Search.aggregate.transformations`
    to specify ``APPLY`` steps in :meth:`~coredis.modules.Search.aggregate`

    For more details refer to
    `APPLY expressions <https://redis.io/docs/develop/ai/search-and-query/advanced-concepts/aggregations/#apply-expressions>`__
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
    `FILTER expressions <https://redis.io/docs/develop/ai/search-and-query/advanced-concepts/aggregations/#filter-expressions>`__
    in the RediSearch documentation.
    """

    #: The filter expression
    expression: StringT

    @property
    def args(self) -> CommandArgList:
        return [PrefixToken.FILTER, self.expression]


@dataclasses.dataclass
class Combine(ABC):
    """
    Method definition for fusing text & vector results to be used with
    :paramref:`~coredis.modules.Search.hybrid.combine`
    to specify the ``COMBINE`` method for :meth:`~coredis.modules.Search.hybrid`
    """

    method: Literal["RRF", "LINEAR"] = dataclasses.field(init=False)
    window: int | None = None
    score_alias: StringT | None = None

    @property
    def args(self) -> CommandArgList:
        args = [PureToken.COMBINE, self.method]
        sub_args = self.sub_args
        if self.window is not None:
            sub_args.extend([PrefixToken.WINDOW, self.window])
        if self.score_alias:
            sub_args.extend([PrefixToken.YIELD_SCORE_AS, self.score_alias])
        return args + [len(sub_args), *sub_args]

    @property
    @abstractmethod
    def sub_args(self) -> CommandArgList: ...


@dataclasses.dataclass
class RRFCombine(Combine):
    """
    RRF (Reciprocal Rank Fusion) method
    to be used in :meth:`~coredis.modules.Search.hybrid`
    with :paramref:`~coredis.modules.Search.hybrid.combine`
    """

    method: Literal["RRF"] = dataclasses.field(init=False, default="RRF")
    constant: int | None = None

    @property
    def sub_args(self) -> CommandArgList:
        args: CommandArgList = []
        if self.constant is not None:
            args.extend([PrefixToken.CONSTANT, self.constant])
        return args


@dataclasses.dataclass
class LinearCombine(Combine):
    """
    Linear combination with ALPHA and BETA weights
    to be used in :meth:`~coredis.modules.Search.hybrid`
    with :paramref:`~coredis.modules.Search.hybrid.combine`
    """

    method: Literal["LINEAR"] = dataclasses.field(init=False, default="LINEAR")
    alpha: int | None = None
    beta: int | None = None

    @property
    def sub_args(self) -> CommandArgList:
        args: CommandArgList = []
        if self.alpha is not None:
            args.extend([PrefixToken.ALPHA, self.alpha])
        if self.beta is not None:
            args.extend([PrefixToken.BETA, self.beta])
        return args


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
    def create(
        self,
        index: KeyT,
        schema: Parameters[Field],
        *,
        on: Literal[PureToken.HASH, PureToken.JSON] | None = None,
        prefixes: Parameters[StringT] | None = None,
        filter_expression: StringT | None = None,
        language: StringT | None = None,
        language_field: StringT | None = None,
        score: int | float | None = None,
        score_field: StringT | None = None,
        payload_field: StringT | None = None,
        maxtextfields: bool | None = None,
        temporary: int | timedelta | None = None,
        nooffsets: bool | None = None,
        nohl: bool | None = None,
        nofields: bool | None = None,
        nofreqs: bool | None = None,
        stopwords: Parameters[StringT] | None = None,
        skipinitialscan: bool | None = None,
    ) -> CommandRequest[bool]:
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
        command_arguments: CommandArgList = [index]
        if on:
            command_arguments.extend([PrefixToken.ON, on])

        if prefixes:
            _prefixes: list[StringT] = list(prefixes)
            command_arguments.extend([PrefixToken.PREFIX, len(_prefixes), *_prefixes])
        if filter_expression:
            command_arguments.extend([PrefixToken.FILTER, filter_expression])
        if language:
            command_arguments.extend([PrefixToken.LANGUAGE, language])
        if language_field:
            command_arguments.extend([PrefixToken.LANGUAGE_FIELD, language_field])
        if score:
            command_arguments.extend([PrefixToken.SCORE, score])
        if score_field:
            command_arguments.extend([PrefixToken.SCORE_FIELD, score_field])
        if payload_field:
            command_arguments.extend([PrefixToken.PAYLOAD_FIELD, payload_field])
        if maxtextfields:
            command_arguments.append(PureToken.MAXTEXTFIELDS)
        if temporary:
            command_arguments.extend([PrefixToken.TEMPORARY, normalized_seconds(temporary)])
        if nooffsets:
            command_arguments.append(PureToken.NOOFFSETS)
        if nohl:
            command_arguments.append(PureToken.NOHL)
        if nofields:
            command_arguments.append(PureToken.NOFIELDS)
        if nofreqs:
            command_arguments.append(PureToken.NOFREQS)
        if stopwords:
            _stop: list[StringT] = list(stopwords)
            command_arguments.extend([PrefixToken.STOPWORDS, len(_stop), *_stop])
        if skipinitialscan:
            command_arguments.append(PureToken.SKIPINITIALSCAN)

        field_args: CommandArgList = [PureToken.SCHEMA]
        for field in schema:
            field_args.extend(field.args)
        command_arguments.extend(field_args)

        return self.client.create_request(
            CommandName.FT_CREATE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_INFO,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def info(self, index: KeyT) -> CommandRequest[dict[AnyStr, ResponseType]]:
        """
        Returns information and statistics on the index

        :param index: The name of the index.
        """
        return self.client.create_request(
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
    def explain(
        self, index: KeyT, query: StringT, dialect: int | None = None
    ) -> CommandRequest[AnyStr]:
        """
        Returns the execution plan for a complex query

        :param index: The name of the index to query.
        :param query: The query to explain.
        :param dialect: Query dialect to use.
        """
        command_arguments: CommandArgList = [index, query]
        if dialect:
            command_arguments.extend([PrefixToken.DIALECT, dialect])
        return self.client.create_request(
            CommandName.FT_EXPLAIN,
            *command_arguments,
            callback=AnyStrCallback[AnyStr](),
        )

    @module_command(
        CommandName.FT_ALTER,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def alter(
        self,
        index: KeyT,
        field: Field,
        skipinitialscan: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Adds a new field to the index

        :param index: The name of the index to alter.
        :param field: The new field to add
        :param skipinitialscan: If ``True``, skip the initial scan and indexing.

        """
        command_arguments: CommandArgList = [index]
        if skipinitialscan:
            command_arguments.append(PureToken.SKIPINITIALSCAN)
        command_arguments.extend([PureToken.SCHEMA, PureToken.ADD, *field.args])

        return self.client.create_request(
            CommandName.FT_ALTER, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_DROPINDEX,
        module=MODULE,
        version_introduced="2.0.0",
        group=COMMAND_GROUP,
    )
    def dropindex(self, index: KeyT, delete_docs: bool | None = None) -> CommandRequest[bool]:
        """
        Deletes the index

        :param index: The name of the index to delete.
        :param delete_docs: If ``True``, delete the documents associated with the index.
        """
        command_arguments: CommandArgList = [index]
        if delete_docs:
            command_arguments.append(PureToken.DELETE_DOCS)

        return self.client.create_request(
            CommandName.FT_DROPINDEX,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.FT_ALIASADD,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def aliasadd(self, alias: StringT, index: KeyT) -> CommandRequest[bool]:
        """
        Adds an alias to the index

        :param alias: The alias to be added to the index.
        :param index: The index to which the alias will be added.
        """

        return self.client.create_request(
            CommandName.FT_ALIASADD, alias, index, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_ALIASUPDATE,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def aliasupdate(self, alias: StringT, index: KeyT) -> CommandRequest[bool]:
        """
        Adds or updates an alias to the index

        :param alias: The alias to be added to an index.
        :param index: The index to which the alias will be added.
        """

        return self.client.create_request(
            CommandName.FT_ALIASUPDATE, alias, index, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_ALIASDEL,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def aliasdel(self, alias: StringT) -> CommandRequest[bool]:
        """
        Deletes an alias from the index

        :param alias: The index alias to be removed.
        """

        return self.client.create_request(
            CommandName.FT_ALIASDEL, alias, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_TAGVALS,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def tagvals(self, index: KeyT, field_name: StringT) -> CommandRequest[set[AnyStr]]:
        """
        Returns the distinct tags indexed in a Tag field

        :param index: The name of the index.
        :param field_name: Name of a Tag field defined in the schema.
        """

        return self.client.create_request(
            CommandName.FT_TAGVALS, index, field_name, callback=SetCallback[AnyStr]()
        )

    @module_command(
        CommandName.FT_SYNUPDATE,
        module=MODULE,
        version_introduced="1.2.0",
        group=COMMAND_GROUP,
    )
    def synupdate(
        self,
        index: KeyT,
        synonym_group: StringT,
        terms: Parameters[StringT],
        skipinitialscan: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Creates or updates a synonym group with additional terms

        :param index: The name of the index.
        :param synonym_group: The ID of the synonym group to update.
        :param terms: A list of terms to add to the synonym group.
        :param skipinitialscan: If ``True``, only documents indexed after the
         update will be affected.

        """
        command_arguments: CommandArgList = [index, synonym_group]
        if skipinitialscan:
            command_arguments.append(PureToken.SKIPINITIALSCAN)
        command_arguments.extend(terms)
        return self.client.create_request(
            CommandName.FT_SYNUPDATE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.FT_SYNDUMP,
        module=MODULE,
        version_introduced="1.2.0",
        group=COMMAND_GROUP,
    )
    def syndump(self, index: KeyT) -> CommandRequest[dict[AnyStr, list[AnyStr]]]:
        """
        Dumps the contents of a synonym group

        :param index: The name of the index.
        """

        return self.client.create_request(
            CommandName.FT_SYNDUMP,
            index,
            callback=DictCallback[AnyStr, list[AnyStr]](),
        )

    @module_command(
        CommandName.FT_SPELLCHECK,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    def spellcheck(
        self,
        index: KeyT,
        query: StringT,
        distance: int | None = None,
        include: StringT | None = None,
        exclude: StringT | None = None,
        dialect: int | None = None,
    ) -> CommandRequest[dict[AnyStr, OrderedDict[AnyStr, float]]]:
        """
        Performs spelling correction on a query, returning suggestions for misspelled terms

        :param index: The name of the index with the indexed terms.
        :param query: The search query.
        :param distance: Maximum Levenshtein distance for spelling suggestions
        :param include: Specifies an inclusion of a custom dictionary
        :param exclude: Specifies an exclusion of a custom dictionary
        :param dialect: The query dialect to use.
        """
        command_arguments: CommandArgList = [index, query]
        if distance:
            command_arguments.extend([PrefixToken.DISTANCE, distance])
        if exclude:
            command_arguments.extend([PrefixToken.TERMS, PureToken.EXCLUDE, exclude])
        if include:
            command_arguments.extend([PrefixToken.TERMS, PureToken.INCLUDE, include])
        if dialect:
            command_arguments.extend([PrefixToken.DIALECT, dialect])
        return self.client.create_request(
            CommandName.FT_SPELLCHECK,
            *command_arguments,
            callback=SpellCheckCallback[AnyStr](),
        )

    @module_command(
        CommandName.FT_DICTADD,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    def dictadd(
        self,
        name: StringT,
        terms: Parameters[StringT],
    ) -> CommandRequest[int]:
        """
        Adds terms to a dictionary

        :param name: The name of the dictionary.
        :param terms: The terms to add to the dictionary.
        """
        command_arguments: CommandArgList = [name, *terms]

        return self.client.create_request(
            CommandName.FT_DICTADD, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_DICTDEL,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    def dictdel(
        self,
        name: StringT,
        terms: Parameters[StringT],
    ) -> CommandRequest[int]:
        """
        Deletes terms from a dictionary

        :param name: The name of the dictionary.
        :param terms: The terms to delete from the dictionary.
        """
        command_arguments: CommandArgList = [name, *terms]

        return self.client.create_request(
            CommandName.FT_DICTDEL, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_DICTDUMP,
        module=MODULE,
        version_introduced="1.4.0",
        group=COMMAND_GROUP,
    )
    def dictdump(self, name: StringT) -> CommandRequest[set[AnyStr]]:
        """
        Dumps all terms in the given dictionary

        :param name: The name of the dictionary to dump.
        """

        return self.client.create_request(
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
    def list(self) -> CommandRequest[set[AnyStr]]:
        """
        Returns a list of all existing indexes
        """
        return self.client.create_request(CommandName.FT__LIST, callback=SetCallback[AnyStr]())

    @module_command(
        CommandName.FT_CONFIG_SET,
        module=MODULE,
        version_introduced="1.0.0",
        version_deprecated="8.0.0",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent[bool](),
        ),
    )
    def config_set(self, option: StringT, value: ValueT) -> CommandRequest[bool]:
        """
        Sets runtime configuration options
        """

        return self.client.create_request(
            CommandName.FT_CONFIG_SET, option, value, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.FT_CONFIG_GET,
        module=MODULE,
        version_introduced="1.0.0",
        version_deprecated="8.0.0",
        group=COMMAND_GROUP,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    def config_get(self, option: StringT) -> CommandRequest[dict[AnyStr, ResponsePrimitive]]:
        """
        Retrieves runtime configuration options
        """

        return self.client.create_request(
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
    def search(
        self,
        index: KeyT,
        query: StringT,
        *,
        nocontent: bool | None = None,
        verbatim: bool | None = None,
        nostopwords: bool | None = None,
        withscores: bool | None = None,
        withpayloads: bool | None = None,
        withsortkeys: bool | None = None,
        numeric_filters: None
        | (Mapping[StringT, tuple[int | float | StringT, int | float | StringT]]) = None,
        geo_filters: None
        | (
            Mapping[
                StringT,
                tuple[
                    tuple[int | float, int | float],
                    int | float,
                    Literal[PureToken.KM, PureToken.M, PureToken.MI, PureToken.FT],
                ],
            ]
        ) = None,
        in_keys: Parameters[StringT] | None = None,
        in_fields: Parameters[StringT] | None = None,
        returns: Mapping[StringT, StringT | None] | None = None,
        summarize_fields: Parameters[StringT] | None = None,
        summarize_frags: int | None = None,
        summarize_length: int | None = None,
        summarize_separator: StringT | None = None,
        highlight_fields: Parameters[StringT] | None = None,
        highlight_tags: tuple[StringT, StringT] | None = None,
        slop: int | None = None,
        timeout: int | timedelta | None = None,
        inorder: bool | None = None,
        language: StringT | None = None,
        expander: StringT | None = None,
        scorer: StringT | None = None,
        explainscore: bool | None = None,
        payload: StringT | None = None,
        sortby: StringT | None = None,
        sort_order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        offset: int | None = 0,
        limit: int | None = None,
        parameters: Mapping[StringT, ValueT] | None = None,
        dialect: int | None = None,
    ) -> CommandRequest[SearchResult[AnyStr]]:
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
        command_arguments: CommandArgList = [index, query]
        if nocontent:
            command_arguments.append(PureToken.NOCONTENT)
        if verbatim:
            command_arguments.append(PureToken.VERBATIM)
        if nostopwords:
            command_arguments.append(PureToken.NOSTOPWORDS)
        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
        if withpayloads:
            command_arguments.append(PureToken.WITHPAYLOADS)
        if withsortkeys:
            command_arguments.append(PureToken.WITHSORTKEYS)
        if numeric_filters:
            for field, numeric_filter in numeric_filters.items():
                command_arguments.extend(
                    [PrefixToken.FILTER, field, numeric_filter[0], numeric_filter[1]]
                )
        if geo_filters:
            for field, gfilter in geo_filters.items():
                command_arguments.extend(
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
            _in_keys: list[StringT] = list(in_keys)
            command_arguments.extend([PrefixToken.INKEYS, len(_in_keys), *_in_keys])
        if in_fields:
            _in_fields: list[StringT] = list(in_fields)
            command_arguments.extend([PrefixToken.INFIELDS, len(_in_fields), *_in_fields])
        if returns:
            return_items: CommandArgList = []
            for identifier, property in returns.items():
                return_items.append(identifier)
                if property:
                    return_items.extend([PrefixToken.AS, property])
            command_arguments.extend([PrefixToken.RETURN, len(return_items), *return_items])
        if sortby:
            command_arguments.extend([PrefixToken.SORTBY, sortby])
            if sort_order:
                command_arguments.append(sort_order)
        if summarize_fields or summarize_frags or summarize_length or summarize_separator:
            command_arguments.append(PureToken.SUMMARIZE)
            if summarize_fields:
                _fields: list[StringT] = list(summarize_fields)
                command_arguments.extend([PrefixToken.FIELDS, len(_fields), *_fields])
            if summarize_frags:
                command_arguments.extend([PrefixToken.FRAGS, summarize_frags])
            if summarize_length:
                command_arguments.extend([PrefixToken.LEN, summarize_length])
            if summarize_separator:
                command_arguments.extend([PrefixToken.SEPARATOR, summarize_separator])
        if highlight_fields or highlight_tags:
            command_arguments.append(PureToken.HIGHLIGHT)
            if highlight_fields:
                _fields = list(highlight_fields)
                command_arguments.extend([PrefixToken.FIELDS, len(_fields), *_fields])
            if highlight_tags:
                command_arguments.extend([PureToken.TAGS, highlight_tags[0], highlight_tags[1]])
        if slop is not None:
            command_arguments.extend([PrefixToken.SLOP, slop])
        if timeout:
            command_arguments.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        if inorder:
            command_arguments.append(PureToken.INORDER)
        if language:
            command_arguments.extend([PrefixToken.LANGUAGE, language])
        if expander:  # noqa
            command_arguments.extend([PrefixToken.EXPANDER, expander])
        if scorer:  # noqa
            command_arguments.extend([PrefixToken.SCORER, scorer])
        if explainscore:
            command_arguments.append(PureToken.EXPLAINSCORE)
        if payload:
            command_arguments.extend([PrefixToken.PAYLOAD, payload])
        if limit is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset or 0, limit])
        if parameters:
            _parameters: list[ValueT] = list(itertools.chain(*parameters.items()))
            command_arguments.extend([PureToken.PARAMS, len(_parameters), *_parameters])
        if dialect:
            command_arguments.extend([PrefixToken.DIALECT, dialect])

        return self.client.create_request(
            CommandName.FT_SEARCH,
            *command_arguments,
            callback=SearchResultCallback[AnyStr](
                withscores=withscores,
                withpayloads=withpayloads,
                withsortkeys=withsortkeys,
                explainscore=explainscore,
                nocontent=nocontent,
            ),
        )

    @module_command(
        CommandName.FT_AGGREGATE,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
        arguments={"dialect": {"version_introduced": "2.4.3"}},
    )
    def aggregate(
        self,
        index: KeyT,
        query: StringT,
        *,
        verbatim: bool | None = None,
        load: Literal["*"] | Parameters[StringT | tuple[StringT, StringT]] | None = None,
        timeout: int | timedelta | None = None,
        transforms: Parameters[Group | Apply | Filter] | None = None,
        sortby: Mapping[StringT, Literal[PureToken.ASC, PureToken.DESC]] | None = None,
        sortby_max: int | None = None,
        offset: int | None = 0,
        limit: int | None = None,
        with_cursor: bool | None = None,
        cursor_read_size: int | None = None,
        cursor_maxidle: int | timedelta | None = None,
        parameters: Mapping[StringT, StringT] | None = None,
        dialect: int | None = None,
    ) -> CommandRequest[SearchAggregationResult[AnyStr]]:
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
        command_arguments: CommandArgList = [index, query]
        if verbatim:
            command_arguments.append(PureToken.VERBATIM)
        if timeout:
            command_arguments.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])
        if load:
            command_arguments.append(PrefixToken.LOAD)
            if isinstance(load, (bytes, str)):
                command_arguments.append(load)
            else:
                _load_fields: list[StringT] = []
                for field in load:
                    if isinstance(field, (bytes, str)):
                        _load_fields.append(field)
                    else:
                        _load_fields.extend([field[0], PrefixToken.AS, field[1]])

                command_arguments.extend([len(_load_fields), *_load_fields])

        if transforms:
            for step in transforms:
                command_arguments.extend(step.args)

        if sortby:
            command_arguments.append(PrefixToken.SORTBY)
            command_arguments.append(len(sortby) * 2)
            for field, order in sortby.items():
                command_arguments.extend([field, order])
            if sortby_max:
                command_arguments.extend([PrefixToken.MAX, sortby_max])

        if limit is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset or 0, limit])

        if with_cursor:
            command_arguments.append(PureToken.WITHCURSOR)
            if cursor_read_size:
                command_arguments.extend([PrefixToken.COUNT, cursor_read_size])
            if cursor_maxidle:
                command_arguments.extend(
                    [PrefixToken.MAXIDLE, normalized_milliseconds(cursor_maxidle)]
                )
        if parameters:
            _parameters: list[StringT] = list(itertools.chain(*parameters.items()))
            command_arguments.extend([PureToken.PARAMS, len(_parameters), *_parameters])
        if dialect:
            command_arguments.extend([PrefixToken.DIALECT, dialect])
        return self.client.create_request(
            CommandName.FT_AGGREGATE,
            *command_arguments,
            callback=AggregationResultCallback[AnyStr](with_cursor=with_cursor, dialect=dialect),
        )

    @mutually_exclusive_parameters("k", "radius")
    @mutually_inclusive_parameters("ef_runtime", leaders=["k"])
    @mutually_inclusive_parameters("epsilon", leaders=["radius"])
    @module_command(
        CommandName.FT_HYBRID,
        module=MODULE,
        version_introduced="8.4.0",
        group=COMMAND_GROUP,
    )
    def hybrid(
        self,
        index: KeyT,
        search: StringT,
        vector_field: StringT,
        vector_data: bytes,
        *,
        scorer: StringT | None = None,
        search_score_alias: StringT | None = None,
        k: int | None = None,
        ef_runtime: int | None = None,
        radius: int | None = None,
        epsilon: float | None = None,
        vector_score_alias: StringT | None = None,
        vector_filter: Filter | None = None,
        combine: RRFCombine | LinearCombine | None = None,
        transforms: Parameters[Group | Apply | Filter] | None = None,
        sortby: Mapping[StringT, Literal[PureToken.ASC, PureToken.DESC]] | None = None,
        offset: int | None = 0,
        limit: int | None = None,
        load: Literal["*"] | Parameters[StringT | tuple[StringT, StringT]] | None = None,
        timeout: int | timedelta | None = None,
        parameters: Mapping[StringT, StringT] | None = None,
    ) -> CommandRequest[HybridResult[AnyStr]]:
        """
        Performs hybrid search combining text search and vector similarity with
        configurable fusion methods.

        :param index: Name of the Redis index to search.
        :param search: Base filtering query to retrieve documents.
        :param vector_field: The vector field in the index to search against
        :param vector_data: The vector data to use for similarity comparison
        :param scorer: Scoring algorithm to use
        :param search_score_alias: Alias for the search score
        :param k: K value for performing K-nearest neighbour search
        :param ef_runtime: controls the range search accuracy vs. speed tradeoff.
        :param radius: maximum distance for range matches
        :param epsilon: precision controls for range matches
        :param vector_score_alias: Alias for the vector search score
        :param vector_filter: Filter to use to select which documents are considered
         for vector similarity.
        :param combine: How to fuse the text search and vector similarity results.
        :param transforms: List of transformations to apply to the results.
        :param sortby: The fields to sort by and the direction to sort.
        :param offset: Number of results to skip.
        :param limit: Maximum number of results to return.
        :param load: Load document attributes from the source document.
        :param timeout: Maximum time to wait for the query to complete.
        :param parameters: Additional parameters to pass to the query.
        """
        command_arguments: CommandArgList = [index]

        # SEARCH
        command_arguments.extend([PureToken.SEARCH, search])

        # SCORER
        if scorer is not None:
            command_arguments.extend([PrefixToken.SCORER, scorer])
        if search_score_alias is not None:
            command_arguments.extend([PrefixToken.YIELD_SCORE_AS, search_score_alias])

        # VSIM
        command_arguments.extend([PureToken.VSIM, vector_field, "$query_vector"])
        if k is not None:
            _knn_args: CommandArgList = [PrefixToken.K, k]
            if ef_runtime:
                _knn_args.extend([PrefixToken.EF_RUNTIME, ef_runtime])

            command_arguments.extend([PureToken.KNN, len(_knn_args), *_knn_args])
        if radius is not None:
            _range_args: CommandArgList = [PrefixToken.RADIUS, radius]
            if epsilon:
                _range_args.extend([PrefixToken.EPSILON, epsilon])
            command_arguments.extend([PureToken.RANGE, len(_range_args), *_range_args])
        if vector_filter is not None:
            command_arguments.extend(vector_filter.args)
        if vector_score_alias is not None:
            command_arguments.extend([PrefixToken.YIELD_SCORE_AS, vector_score_alias])

        # COMBINE
        if combine:
            command_arguments.extend(combine.args)

        # LIMIT
        if limit is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset or 0, limit])
        # SORT
        if sortby:
            command_arguments.append(PrefixToken.SORTBY)
            command_arguments.append(len(sortby) * 2)
            for sort_field, sort_order in sortby.items():
                command_arguments.extend([sort_field, sort_order])
        # LOAD
        if load:
            command_arguments.append(PrefixToken.LOAD)
            if isinstance(load, (bytes, str)):
                command_arguments.append(load)
            else:
                _load_fields: list[StringT] = []
                for load_field in load:
                    if isinstance(load_field, (bytes, str)):
                        _load_fields.append(load_field)
                    else:
                        _load_fields.extend([load_field[0], PrefixToken.AS, load_field[1]])

                command_arguments.extend([len(_load_fields), *_load_fields])

        # GROUPBY/REDUCE/APPLY/FILTER
        if transforms:
            for step in transforms:
                command_arguments.extend(step.args)

        # PARAMS
        _parameters: list[StringT] = list(
            itertools.chain(*[("query_vector", vector_data)] + list((parameters or {}).items()))
        )
        command_arguments.extend([PureToken.PARAMS, len(_parameters), *_parameters])

        if timeout:
            command_arguments.extend([PrefixToken.TIMEOUT, normalized_milliseconds(timeout)])

        return self.client.create_request(
            CommandName.FT_HYBRID,
            *command_arguments,
            callback=HybridSearchCallback(),
        )

    @module_command(
        CommandName.FT_CURSOR_READ,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
    )
    def cursor_read(
        self, index: KeyT, cursor_id: int, count: int | None = None
    ) -> CommandRequest[SearchAggregationResult[AnyStr]]:
        """
        Reads from a cursor
        """
        command_arguments: CommandArgList = [index, cursor_id]
        if count:
            command_arguments.extend([PrefixToken.COUNT, count])

        return self.client.create_request(
            CommandName.FT_CURSOR_READ,
            *command_arguments,
            callback=AggregationResultCallback[AnyStr](with_cursor=True),
        )

    @module_command(
        CommandName.FT_CURSOR_DEL,
        module=MODULE,
        version_introduced="1.1.0",
        group=COMMAND_GROUP,
    )
    def cursor_del(self, index: KeyT, cursor_id: int) -> CommandRequest[bool]:
        """
        Deletes a cursor

        """
        command_arguments: CommandArgList = [index, cursor_id]

        return self.client.create_request(
            CommandName.FT_CURSOR_DEL,
            *command_arguments,
            callback=SimpleStringCallback(),
        )
