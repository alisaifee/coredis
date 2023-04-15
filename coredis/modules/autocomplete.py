from __future__ import annotations

from deprecated.sphinx import versionadded

from ..commands._wrappers import CacheConfig
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..response._callbacks import BoolCallback, IntCallback
from ..tokens import PrefixToken, PureToken
from ..typing import AnyStr, CommandArgList, KeyT, Optional, StringT, Tuple, Union
from .base import ModuleGroup, module_command
from .response._callbacks.autocomplete import AutocompleteCallback
from .response.types import AutocompleteSuggestion
from .search import RediSearch


@versionadded(version="4.12")
class Autocomplete(ModuleGroup[AnyStr]):
    MODULE = RediSearch
    COMMAND_GROUP = CommandGroup.SUGGESTION

    @module_command(
        CommandName.FT_SUGADD,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def sugadd(
        self,
        key: KeyT,
        string: StringT,
        score: Union[int, float],
        increment_score: Optional[bool] = None,
        payload: Optional[StringT] = None,
    ) -> int:
        """
        Adds a suggestion string to an auto-complete suggestion dictionary

        :param key: The suggestion dictionary key.
        :param string: The suggestion string to index.
        :param score: The floating point number of the suggestion string's weight.
        :param increment_score: Increments the existing entry of the suggestion by
         the given score, instead of replacing the score.
        :param payload: Saves an extra payload with the suggestion, that can be
         fetched when calling :meth:`sugget` by using :paramref:`sugget.withpayloads`
        """
        pieces: CommandArgList = [key, string, score]
        if increment_score:
            pieces.append(PureToken.INCREMENT)
        if payload:
            pieces.extend([PrefixToken.PAYLOAD, payload])

        return await self.execute_module_command(
            CommandName.FT_SUGADD, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_SUGGET,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
        flags={CommandFlag.READONLY},
    )
    async def sugget(
        self,
        key: KeyT,
        prefix: StringT,
        *,
        fuzzy: Optional[bool] = None,
        withscores: Optional[bool] = None,
        withpayloads: Optional[bool] = None,
        max_suggestions: Optional[int] = None,
    ) -> Union[Tuple[AutocompleteSuggestion[AnyStr], ...], Tuple[()]]:
        """
        Gets completion suggestions for a prefix

        :param key: The suggestion dictionary key.
        :param prefix: The prefix to complete on.
        :param fuzzy: If ``True``, performs a fuzzy prefix search, including prefixes at
         Levenshtein distance of 1 from the prefix sent.
        :param withscores: If ``True``, also returns the score of each suggestion.
        :param withpayloads: If True, returns optional payloads saved along with the suggestions.
        :param max_suggestions: Limits the results to a maximum of ``max_suggestions``
        """
        pieces: CommandArgList = [key, prefix]
        if fuzzy:
            pieces.append(PureToken.FUZZY)
        if withscores:
            pieces.append(PureToken.WITHSCORES)
        if withpayloads:
            pieces.append(PureToken.WITHPAYLOADS)
        if max_suggestions is not None:
            pieces.append(PureToken.MAX)
            pieces.append(max_suggestions)

        return await self.execute_module_command(
            CommandName.FT_SUGGET,
            *pieces,
            callback=AutocompleteCallback[AnyStr](),
            withscores=withscores,
            withpayloads=withpayloads,
        )

    @module_command(
        CommandName.FT_SUGDEL,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def sugdel(self, key: KeyT, string: StringT) -> bool:
        """
        Deletes a string from a suggestion index

        :param key: The suggestion dictionary key.
        :param string: The suggestion string to index.

        """
        pieces: CommandArgList = [key, string]

        return await self.execute_module_command(
            CommandName.FT_SUGDEL, *pieces, callback=BoolCallback()
        )

    @module_command(
        CommandName.FT_SUGLEN,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    async def suglen(self, key: KeyT) -> int:
        """
        Gets the size of an auto-complete suggestion dictionary

        :param key: The key of the suggestion dictionary.
        """
        pieces: CommandArgList = [key]

        return await self.execute_module_command(
            CommandName.FT_SUGLEN, *pieces, callback=IntCallback()
        )
