from __future__ import annotations

from coredis.modules.response.types import AutocompleteSuggestion
from coredis.response._callbacks import ResponseCallback
from coredis.typing import AnyStr, List, Optional, ResponseType, Tuple, Union, ValueT


class AutocompleteCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        Union[Tuple[AutocompleteSuggestion[AnyStr], ...], Tuple[()]],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Union[Tuple[AutocompleteSuggestion[AnyStr], ...], Tuple[()]]:
        if not response:
            return ()
        step = 1
        results = []
        score_idx = payload_idx = 0
        if options.get("withscores"):
            score_idx = 1
            step += 1
        if options.get("withpayloads"):
            payload_idx = score_idx + 1
            step += 1

        for k in range(0, len(response), step):
            section = response[k : k + step]
            score = section[score_idx] if score_idx else None
            results.append(
                AutocompleteSuggestion(
                    section[0],
                    float(score) if score else None,
                    section[payload_idx] if payload_idx else None,
                )
            )

        return tuple(results)
