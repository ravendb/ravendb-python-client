from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage


class AiMessageRole(Enum):
    """Role of a message in an AI conversation."""

    SYSTEM = "System"
    USER = "User"
    ASSISTANT = "Assistant"
    SUMMARY = "Summary"
    INTERNAL = "Internal"


def _parse_timestamp(ts_value: Any) -> Optional[datetime]:
    """Parse a RavenDB timestamp string into a datetime.

    Handles Z suffix and 7-digit fractional seconds (truncated to 6 for Python 3.9).
    """
    if not ts_value:
        return None
    if isinstance(ts_value, datetime):
        return ts_value
    if isinstance(ts_value, str):
        # Replace Z suffix with +00:00 for Python fromisoformat
        ts_str = ts_value.replace("Z", "+00:00")
        # Truncate fractional seconds to 6 digits (Python 3.9 limit)
        if "." in ts_str:
            dot_idx = ts_str.index(".")
            remaining = ts_str[dot_idx + 1 :]
            frac_end = len(remaining)
            for i, c in enumerate(remaining):
                if c in ("+", "-") and i > 0:
                    frac_end = i
                    break
            frac = remaining[:frac_end][:6]
            rest = remaining[frac_end:]
            ts_str = ts_str[: dot_idx + 1] + frac + rest
        try:
            return datetime.fromisoformat(ts_str)
        except (ValueError, AttributeError):
            return None
    return None


@dataclass
class AiToolCallResult:
    """
    Represents a tool call that originated in an assistant message, with its
    eventual response (result) and optional sub-conversation ID merged in.
    """

    id: Optional[str] = None
    """The tool call ID from the model."""

    name: Optional[str] = None
    """Tool name."""

    arguments: Optional[str] = None
    """Arguments the model passed, as JSON string."""

    result: Optional[str] = None
    """The tool's response content. None if still pending (ActionRequired)."""

    sub_conversation_id: Optional[str] = None
    """If this tool call was a sub-agent invocation, the ID of the spawned sub-conversation."""

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiToolCallResult:
        return cls(
            id=json_dict.get("Id"),
            name=json_dict.get("Name"),
            arguments=json_dict.get("Arguments"),
            result=json_dict.get("Result"),
            sub_conversation_id=json_dict.get("SubConversationId"),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.id,
            "Name": self.name,
            "Arguments": self.arguments,
            "Result": self.result,
            "SubConversationId": self.sub_conversation_id,
        }


@dataclass
class AiConversationMessage:
    """
    A single message in an AI agent conversation.

    Attributes:
        role: The role of the message sender.
        content: Text content. When the stored message has multiple text parts,
            they are joined with line breaks. None for assistant messages that
            only initiated tool calls.
        attachments: Attachment file names associated with this message, if any.
        timestamp: When this message was recorded (UTC). Guaranteed unique and
            monotonic within a conversation — safe to use as a paging cursor.
        tool_calls: Tool calls initiated by this assistant message, with their
            responses inlined.
        usage: Token usage for this message (typically on assistant messages).
        sub_conversation_id: For Internal role messages: the ID of the
            sub-conversation this message relates to.
    """

    role: Optional[AiMessageRole] = None
    content: Optional[str] = None
    attachments: Optional[List[str]] = None
    timestamp: Optional[datetime] = None
    tool_calls: Optional[List[AiToolCallResult]] = None
    usage: Optional[Any] = None  # AiUsage when resolved
    sub_conversation_id: Optional[str] = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiConversationMessage:
        from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage

        role_str = json_dict.get("Role")
        if role_str is None:
            raise ValueError("Message is missing 'Role' field")
        try:
            role = AiMessageRole(role_str)
        except ValueError:
            raise ValueError(f"Unknown AiMessageRole: '{role_str}'")

        content = json_dict.get("Content")
        raw_attachments = json_dict.get("Attachments")
        attachments = raw_attachments if raw_attachments is not None else None
        timestamp = _parse_timestamp(json_dict.get("Timestamp"))

        tool_calls = None
        raw_tool_calls = json_dict.get("ToolCalls")
        if raw_tool_calls:
            tool_calls = [AiToolCallResult.from_json(tc) for tc in raw_tool_calls]

        usage = None
        raw_usage = json_dict.get("Usage")
        if raw_usage:
            usage = AiUsage.from_json(raw_usage)

        sub_conversation_id = json_dict.get("SubConversationId")

        return cls(
            role=role,
            content=content,
            attachments=attachments,
            timestamp=timestamp,
            tool_calls=tool_calls,
            usage=usage,
            sub_conversation_id=sub_conversation_id,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Role": self.role.value if self.role else None,
            "Content": self.content,
            "Attachments": self.attachments if self.attachments is not None else None,
            "Timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "ToolCalls": [tc.to_json() for tc in self.tool_calls] if self.tool_calls else None,
            "Usage": self.usage.to_json() if self.usage else None,
            "SubConversationId": self.sub_conversation_id,
        }

    def __repr__(self) -> str:
        return (
            f"AiConversationMessage(role={self.role}, content={self.content!r}, "
            f"timestamp={self.timestamp}, tool_calls={len(self.tool_calls) if self.tool_calls else 0})"
        )
