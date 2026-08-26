from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage
    from ravendb.documents.operations.ai.agents.ai_conversation_message import (
        AiConversationMessage,
    )


def _materialize_value(value: Any) -> Any:
    """Recursively materialize a parameter value from JSON to proper Python types.

    json.loads already produces native Python types (str, int, float, bool, None, list, dict).
    RavenDB internal types like LazyNumberValue never appear in HTTP JSON responses.
    This function only handles homogeneous list type coercion and recursive dict traversals.
    """
    if value is None:
        return None
    if isinstance(value, list):
        items = [_materialize_value(v) for v in value]
        return _to_typed_list(items)
    if isinstance(value, dict):
        return {k: _materialize_value(v) for k, v in value.items()}
    return value


def _to_typed_list(items: List[Any]) -> Any:
    """Convert a list of values to a typed homogeneous list if possible.

    Mirrors the C# ToTypedList method. Returns the list as-is when already
    homogeneous; no-op copies are avoided.
    """
    if not items:
        return items

    all_string = all(isinstance(item, str) for item in items)
    if all_string:
        return items

    all_bool = all(isinstance(item, bool) for item in items)
    if all_bool:
        return items

    all_number = all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in items)
    if all_number:
        has_double = any(isinstance(item, float) for item in items)
        if has_double:
            return [float(item) for item in items]
        return [int(item) for item in items]

    return items


def _materialize_parameters(parameters_dict: Any) -> Dict[str, Any]:
    """Convert raw Parameters from JSON into properly typed values."""
    if not parameters_dict:
        return {}
    result: Dict[str, Any] = {}
    for key, value in parameters_dict.items():
        result[key] = _materialize_value(value)
    return result


@dataclass
class AiConversationMessagesResult:
    """
    The result of fetching conversation messages.

    Attributes:
        conversation_id: The conversation document ID.
        agent: The identifier of the AI agent this conversation belongs to.
        parameters: The conversation parameters as a name -> value map,
            normalized from the stored format.
        total_usage: Cumulative token usage across all turns of this conversation.
        last_message_at: When the last message was added to the conversation.
        messages: Messages in chronological order (oldest first).
        has_more_messages: True if there are more messages beyond the returned page.
        sub_conversation_ids: IDs of sub-agent conversations spawned during this
            conversation.
        attachments: All attachments referenced across the conversation.
    """

    conversation_id: Optional[str] = None
    agent: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    total_usage: Optional[Any] = None  # AiUsage when resolved
    last_message_at: Optional[datetime] = None
    messages: List[Any] = field(default_factory=list)  # List[AiConversationMessage] when resolved
    has_more_messages: bool = False
    sub_conversation_ids: List[str] = field(default_factory=list)
    attachments: List[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiConversationMessagesResult:
        from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage
        from ravendb.documents.operations.ai.agents.ai_conversation_message import (
            AiConversationMessage,
            _parse_timestamp,
        )

        total_usage = None
        raw_usage = json_dict.get("TotalUsage")
        if raw_usage:
            total_usage = AiUsage.from_json(raw_usage)

        last_message_at = _parse_timestamp(json_dict.get("LastMessageAt"))

        messages = []
        raw_messages = json_dict.get("Messages")
        if raw_messages:
            messages = [AiConversationMessage.from_json(msg) for msg in raw_messages]

        return cls(
            conversation_id=json_dict.get("ConversationId"),
            agent=json_dict.get("Agent"),
            parameters=_materialize_parameters(json_dict.get("Parameters")),
            total_usage=total_usage,
            last_message_at=last_message_at,
            messages=messages,
            has_more_messages=json_dict.get("HasMoreMessages", False),
            sub_conversation_ids=json_dict.get("SubConversationIds") or [],
            attachments=json_dict.get("Attachments") or [],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConversationId": self.conversation_id,
            "Agent": self.agent,
            "Parameters": self.parameters if self.parameters is not None else None,
            "TotalUsage": self.total_usage.to_json() if self.total_usage else None,
            "LastMessageAt": self.last_message_at.isoformat() if self.last_message_at else None,
            "HasMoreMessages": self.has_more_messages,
            "SubConversationIds": self.sub_conversation_ids if self.sub_conversation_ids is not None else None,
            "Attachments": self.attachments if self.attachments is not None else None,
            "Messages": [msg.to_json() for msg in self.messages] if self.messages is not None else None,
        }

    def __repr__(self) -> str:
        return (
            f"AiConversationMessagesResult(conversation_id={self.conversation_id!r}, "
            f"agent={self.agent!r}, messages={len(self.messages)}, "
            f"has_more_messages={self.has_more_messages})"
        )
