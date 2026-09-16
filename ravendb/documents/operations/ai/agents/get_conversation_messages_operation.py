from __future__ import annotations

import enum
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import requests

from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.primitives import constants
from ravendb.tools.utils import Utils


class AiConversationDetailLevel(enum.Enum):
    """Controls how much of a conversation the server returns."""

    # User messages and assistant messages that have content. System prompts, tool calls,
    # summaries and internal messages are excluded.
    SIMPLE = "Simple"
    # Adds system messages, tool calls with their results, and per-message usage.
    DETAILED = "Detailed"
    # No filtering at all, summaries and internal messages included.
    FULL = "Full"

    def __str__(self) -> str:
        return self.value


class AiMessageRole(enum.Enum):
    SYSTEM = "System"
    USER = "User"
    ASSISTANT = "Assistant"
    SUMMARY = "Summary"
    INTERNAL = "Internal"

    def __str__(self) -> str:
        return self.value


class AiToolCallResult:
    """A tool call the model initiated, with the tool's response inlined."""

    def __init__(
        self,
        id_: Optional[str] = None,
        name: Optional[str] = None,
        arguments: Optional[str] = None,
        result: Optional[str] = None,
        sub_conversation_id: Optional[str] = None,
    ):
        self.id_ = id_
        self.name = name
        # Arguments the model passed, as a JSON string.
        self.arguments = arguments
        # None while the call is still pending (ActionRequired).
        self.result = result
        # Set when the call was a sub-agent invocation - the spawned conversation can be
        # read on its own with GetConversationMessagesOperation.
        self.sub_conversation_id = sub_conversation_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.id_,
            "Name": self.name,
            "Arguments": self.arguments,
            "Result": self.result,
            "SubConversationId": self.sub_conversation_id,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiToolCallResult:
        return cls(
            id_=json_dict.get("Id"),
            name=json_dict.get("Name"),
            arguments=json_dict.get("Arguments"),
            result=json_dict.get("Result"),
            sub_conversation_id=json_dict.get("SubConversationId"),
        )


class AiConversationMessage:
    """A single message in an AI agent conversation."""

    def __init__(
        self,
        role: Optional[AiMessageRole] = None,
        content: Optional[str] = None,
        attachments: Optional[List[str]] = None,
        timestamp: Optional[datetime] = None,
        tool_calls: Optional[List[AiToolCallResult]] = None,
        usage: Optional[AiUsage] = None,
        sub_conversation_id: Optional[str] = None,
    ):
        self.role = role
        # Multiple stored text parts arrive joined with line breaks. None for assistant
        # messages that only initiated tool calls.
        self.content = content
        self.attachments = attachments
        # Unique and monotonic within a conversation, so it is safe as a paging cursor.
        self.timestamp = timestamp
        self.tool_calls = tool_calls
        self.usage = usage
        # For Internal messages: the sub-conversation this message relates to.
        self.sub_conversation_id = sub_conversation_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Role": self.role.value if self.role else None,
            "Content": self.content,
            "Attachments": self.attachments,
            "Timestamp": Utils.datetime_to_string(self.timestamp),
            "ToolCalls": [tool_call.to_json() for tool_call in self.tool_calls] if self.tool_calls else None,
            "Usage": self.usage.to_json() if self.usage else None,
            "SubConversationId": self.sub_conversation_id,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiConversationMessage:
        role = json_dict.get("Role")
        tool_calls = json_dict.get("ToolCalls")
        usage = json_dict.get("Usage")
        return cls(
            role=AiMessageRole(role) if role else None,
            content=json_dict.get("Content"),
            attachments=json_dict.get("Attachments"),
            timestamp=Utils.string_to_datetime(json_dict.get("Timestamp")),
            tool_calls=[AiToolCallResult.from_json(tool_call) for tool_call in tool_calls] if tool_calls else None,
            usage=AiUsage.from_json(usage) if usage else None,
            sub_conversation_id=json_dict.get("SubConversationId"),
        )


class AiConversationMessagesResult:
    """The result of reading an AI agent conversation."""

    def __init__(
        self,
        conversation_id: Optional[str] = None,
        agent: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        total_usage: Optional[AiUsage] = None,
        last_message_at: Optional[datetime] = None,
        messages: Optional[List[AiConversationMessage]] = None,
        has_more_messages: bool = False,
        sub_conversation_ids: Optional[List[str]] = None,
        attachments: Optional[List[str]] = None,
    ):
        self.conversation_id = conversation_id
        self.agent = agent
        # Conversation parameters as a name -> value map. Values are heterogeneous
        # (primitives and arrays), so they are handed back as they arrive.
        self.parameters = parameters
        # Cumulative token usage across every turn of the conversation.
        self.total_usage = total_usage
        self.last_message_at = last_message_at
        # Chronological, oldest first.
        self.messages = messages
        # Older messages exist for backward/default paging, newer ones for `after` paging.
        self.has_more_messages = has_more_messages
        self.sub_conversation_ids = sub_conversation_ids
        self.attachments = attachments

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConversationId": self.conversation_id,
            "Agent": self.agent,
            "Parameters": self.parameters,
            "TotalUsage": self.total_usage.to_json() if self.total_usage else None,
            "LastMessageAt": Utils.datetime_to_string(self.last_message_at),
            "HasMoreMessages": self.has_more_messages,
            "SubConversationIds": self.sub_conversation_ids,
            "Attachments": self.attachments,
            "Messages": [message.to_json() for message in self.messages] if self.messages else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiConversationMessagesResult:
        total_usage = json_dict.get("TotalUsage")
        messages = json_dict.get("Messages")
        return cls(
            conversation_id=json_dict.get("ConversationId"),
            agent=json_dict.get("Agent"),
            parameters=json_dict.get("Parameters"),
            total_usage=AiUsage.from_json(total_usage) if total_usage else None,
            last_message_at=Utils.string_to_datetime(json_dict.get("LastMessageAt")),
            messages=[AiConversationMessage.from_json(message) for message in messages] if messages else None,
            has_more_messages=json_dict.get("HasMoreMessages", False),
            sub_conversation_ids=json_dict.get("SubConversationIds"),
            attachments=json_dict.get("Attachments"),
        )


class GetConversationMessagesOptions:
    """Paging and filtering for GetConversationMessagesOperation."""

    def __init__(
        self,
        conversation_id: Optional[str] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        page_size: int = constants.int_max,
        detail_level: AiConversationDetailLevel = AiConversationDetailLevel.SIMPLE,
    ):
        self.conversation_id = conversation_id
        # Messages older than this timestamp - backward paging, scrolling up in a chat UI.
        self.before = before
        # Messages newer than this timestamp - catching up after a Changes() notification.
        self.after = after
        self.page_size = page_size
        self.detail_level = detail_level

    def validate(self) -> None:
        if not self.conversation_id:
            raise ValueError("conversation_id cannot be None or empty")

        if self.before is not None and self.after is not None:
            raise ValueError("before and after cannot both be specified.")

        if self.page_size is None or self.page_size <= 0:
            raise ValueError("page_size must be greater than 0.")


class GetConversationMessagesOperation(MaintenanceOperation[AiConversationMessagesResult]):
    """
    Reads messages from an AI agent conversation, with optional timestamp-based paging
    and view filtering. Returns the most recent messages by default.
    """

    def __init__(self, conversation_id_or_parameters: Union[str, GetConversationMessagesOptions]):
        if conversation_id_or_parameters is None:
            raise ValueError("conversation_id_or_parameters cannot be None")

        if isinstance(conversation_id_or_parameters, GetConversationMessagesOptions):
            self._parameters = conversation_id_or_parameters
        else:
            self._parameters = GetConversationMessagesOptions(conversation_id=conversation_id_or_parameters)

        self._parameters.validate()

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AiConversationMessagesResult]:
        return self._GetConversationMessagesCommand(self._parameters)

    class _GetConversationMessagesCommand(RavenCommand[AiConversationMessagesResult]):
        def __init__(self, parameters: GetConversationMessagesOptions):
            super().__init__(AiConversationMessagesResult)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return True

        @staticmethod
        def _to_utc_string(value: datetime) -> str:
            # Naive datetimes are taken as UTC, aware ones are converted, matching the
            # server's EnsureUtc before it formats the cursor.
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return Utils.datetime_to_string(value)

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/ai/agent/conversation/messages"
                f"?conversationId={Utils.quote_key(self._parameters.conversation_id)}"
            )

            if self._parameters.before is not None:
                url += f"&before={Utils.quote_key(self._to_utc_string(self._parameters.before))}"
            if self._parameters.after is not None:
                url += f"&after={Utils.quote_key(self._to_utc_string(self._parameters.after))}"

            url += f"&pageSize={self._parameters.page_size}"
            url += f"&detailLevel={self._parameters.detail_level.value}"

            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return  # 404 - conversation not found

            self.result = AiConversationMessagesResult.from_json(json.loads(response))
