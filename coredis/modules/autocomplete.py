from __future__ import annotations

from deprecated.sphinx import versionadded

from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..commands.request import CommandRequest
from ..response._callbacks import BoolCallback, IntCallback
from ..tokens import PrefixToken, PureToken
from ..typing import AnyStr, CommandArgList, KeyT, StringT
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
    def sugadd(
        self,
        key: KeyT,
        string: StringT,
        score: int | float,
        increment_score: bool | None = None,
        payload: StringT | None = None,
    ) -> CommandRequest[int]:
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
        command_arguments: CommandArgList = [key, string, score]
        if increment_score:
            command_arguments.append(PureToken.INCREMENT)
        if payload:
            command_arguments.extend([PrefixToken.PAYLOAD, payload])

        return self.client.create_request(
            CommandName.FT_SUGADD, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.FT_SUGGET,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def sugget(
        self,
        key: KeyT,
        prefix: StringT,
        *,
        fuzzy: bool | None = None,
        withscores: bool | None = None,
        withpayloads: bool | None = None,
        max_suggestions: int | None = None,
    ) -> CommandRequest[tuple[AutocompleteSuggestion[AnyStr], ...] | tuple[()]]:
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
        command_arguments: CommandArgList = [key, prefix]
        if fuzzy:
            command_arguments.append(PureToken.FUZZY)
        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
        if withpayloads:
            command_arguments.append(PureToken.WITHPAYLOADS)
        if max_suggestions is not None:
            command_arguments.append(PureToken.MAX)
            command_arguments.append(max_suggestions)

        return self.client.create_request(
            CommandName.FT_SUGGET,
            *command_arguments,
            callback=AutocompleteCallback[AnyStr](withscores=withscores, withpayloads=withpayloads),
        )

    @module_command(
        CommandName.FT_SUGDEL,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def sugdel(self, key: KeyT, string: StringT) -> CommandRequest[bool]:
        """
        Deletes a string from a suggestion index

        :param key: The suggestion dictionary key.
        :param string: The suggestion string to index.

        """
        command_arguments: CommandArgList = [key, string]

        return self.client.create_request(
            CommandName.FT_SUGDEL, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.FT_SUGLEN,
        module=MODULE,
        version_introduced="1.0.0",
        group=COMMAND_GROUP,
    )
    def suglen(self, key: KeyT) -> CommandRequest[int]:
        """
        Gets the size of an auto-complete suggestion dictionary

        :param key: The key of the suggestion dictionary.
        """
        command_arguments: CommandArgList = [key]

        return self.client.create_request(
            CommandName.FT_SUGLEN, *command_arguments, callback=IntCallback()
        )
