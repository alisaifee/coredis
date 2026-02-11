from __future__ import annotations

from coredis._enum import CaseAndEncodingInsensitiveEnum


class PubSubMessageTypes(CaseAndEncodingInsensitiveEnum):
    MESSAGE = b"message"
    PMESSAGE = b"pmessage"
    SMESSAGE = b"smessage"
    SUBSCRIBE = b"subscribe"
    UNSUBSCRIBE = b"unsubscribe"
    PSUBSCRIBE = b"psubscribe"
    PUNSUBSCRIBE = b"punsubscribe"
    SSUBSCRIBE = b"ssubscribe"
    SUNSUBSCRIBE = b"sunsubscribe"


PUBLISH_MESSAGE_TYPES = {
    PubSubMessageTypes.MESSAGE.value,
    PubSubMessageTypes.PMESSAGE.value,
    PubSubMessageTypes.SMESSAGE.value,
}
SUBSCRIBE_MESSAGE_TYPES = {
    PubSubMessageTypes.SUBSCRIBE.value,
    PubSubMessageTypes.PSUBSCRIBE.value,
    PubSubMessageTypes.SSUBSCRIBE.value,
}
UNSUBSCRIBE_MESSAGE_TYPES = {
    PubSubMessageTypes.UNSUBSCRIBE.value,
    PubSubMessageTypes.PUNSUBSCRIBE.value,
    PubSubMessageTypes.SUNSUBSCRIBE.value,
}

SUBUNSUB_MESSAGE_TYPES = SUBSCRIBE_MESSAGE_TYPES | UNSUBSCRIBE_MESSAGE_TYPES
