from __future__ import annotations

import enum
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import requests

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage


class AiMessageRole(enum.Enum):
    SYSTEM = "System"
    USER = "User"
    ASSISTANT = "Assistant"
    SUMMARY = "Summary"
    INTERNAL = "Internal"


class AiConversationDetailLevel(enum.Enum):
    SIMPLE = "Simple"
    DETAILED = "Detailed"
    FULL = "Full"


class AiToolCallResult:
    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        arguments: Optional[str] = None,
        result: Optional[str] = None,
        sub_conversation_id: Optional[str] = None,
    ):
        self.id = id
        self.name = name
        self.arguments = arguments
        self.result = result
        self.sub_conversation_id = sub_conversation_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.id,
            "Name": self.name,
            "Arguments": self.arguments,
            "Result": self.result,
            "SubConversationId": self.sub_conversation_id,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AiToolCallResult":
        return cls(
            id=json_dict.get("Id"),
            name=json_dict.get("Name"),
            arguments=json_dict.get("Arguments"),
            result=json_dict.get("Result"),
            sub_conversation_id=json_dict.get("SubConversationId"),
        )


class AiConversationMessage:
    def __init__(
        self,
        role: Optional[AiMessageRole] = None,
        content: Optional[str] = None,
        attachments: Optional[List[str]] = None,
        timestamp: Optional[datetime] = None,
        tool_calls: Optional[List[AiToolCallResult]] = None,
        usage: Optional["AiUsage"] = None,
        sub_conversation_id: Optional[str] = None,
    ):
        self.role = role
        self.content = content
        self.attachments = attachments
        self.timestamp = timestamp
        self.tool_calls = tool_calls
        self.usage = usage
        self.sub_conversation_id = sub_conversation_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Role": self.role.value if self.role else None,
            "Content": self.content,
            "Attachments": self.attachments,
            "Timestamp": Utils.datetime_to_string(self.timestamp) if self.timestamp is not None else None,
            "ToolCalls": (
                [tool_call.to_json() for tool_call in self.tool_calls] if self.tool_calls is not None else None
            ),
            "Usage": self.usage.to_json() if self.usage else None,
            "SubConversationId": self.sub_conversation_id,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AiConversationMessage":
        from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage

        role_str = json_dict.get("Role")
        return cls(
            role=AiMessageRole(role_str) if role_str else None,
            content=json_dict.get("Content"),
            attachments=json_dict.get("Attachments") or [],
            timestamp=Utils.string_to_datetime(json_dict.get("Timestamp")),
            tool_calls=(
                [AiToolCallResult.from_json(tool_call) for tool_call in json_dict["ToolCalls"]]
                if json_dict.get("ToolCalls")
                else []
            ),
            usage=AiUsage.from_json(json_dict["Usage"]) if json_dict.get("Usage") else None,
            sub_conversation_id=json_dict.get("SubConversationId"),
        )


class AiConversationMessagesResult:
    def __init__(
        self,
        conversation_id: Optional[str] = None,
        agent: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        total_usage: Optional["AiUsage"] = None,
        last_message_at: Optional[datetime] = None,
        messages: Optional[List[AiConversationMessage]] = None,
        has_more_messages: bool = False,
        sub_conversation_ids: Optional[List[str]] = None,
        attachments: Optional[List[str]] = None,
    ):
        self.conversation_id = conversation_id
        self.agent = agent
        self.parameters = parameters
        self.total_usage = total_usage
        self.last_message_at = last_message_at
        self.messages = messages
        self.has_more_messages = has_more_messages
        self.sub_conversation_ids = sub_conversation_ids
        self.attachments = attachments

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConversationId": self.conversation_id,
            "Agent": self.agent,
            "Parameters": self.parameters,
            "TotalUsage": self.total_usage.to_json() if self.total_usage else None,
            "LastMessageAt": (
                Utils.datetime_to_string(self.last_message_at) if self.last_message_at is not None else None
            ),
            "HasMoreMessages": self.has_more_messages,
            "SubConversationIds": self.sub_conversation_ids,
            "Attachments": self.attachments,
            "Messages": [message.to_json() for message in self.messages] if self.messages is not None else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AiConversationMessagesResult":
        from ravendb.documents.operations.ai.agents.run_conversation_operation import AiUsage

        return cls(
            conversation_id=json_dict.get("ConversationId"),
            agent=json_dict.get("Agent"),
            parameters=json_dict.get("Parameters"),
            total_usage=AiUsage.from_json(json_dict["TotalUsage"]) if json_dict.get("TotalUsage") else None,
            last_message_at=Utils.string_to_datetime(json_dict.get("LastMessageAt")),
            messages=(
                [AiConversationMessage.from_json(message) for message in json_dict["Messages"]]
                if json_dict.get("Messages")
                else None
            ),
            has_more_messages=json_dict.get("HasMoreMessages", False),
            sub_conversation_ids=json_dict.get("SubConversationIds"),
            attachments=json_dict.get("Attachments"),
        )


class GetConversationMessagesOptions:
    def __init__(
        self,
        conversation_id: Optional[str] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        page_size: int = 2147483647,
        detail_level: AiConversationDetailLevel = AiConversationDetailLevel.SIMPLE,
    ):
        self.conversation_id = conversation_id
        self.before = before
        self.after = after
        self.page_size = page_size
        self.detail_level = detail_level

    def validate(self) -> None:
        if not self.conversation_id:
            raise ValueError("conversation_id cannot be None or empty")

        if self.before is not None and self.after is not None:
            raise ValueError("before and after cannot both be specified.")

        if self.page_size <= 0:
            raise ValueError("page_size must be greater than 0.")


class GetConversationMessagesOperation(MaintenanceOperation[AiConversationMessagesResult]):
    def __init__(self, conversation_id_or_options: Union[str, GetConversationMessagesOptions]):
        if isinstance(conversation_id_or_options, GetConversationMessagesOptions):
            parameters = conversation_id_or_options
        elif isinstance(conversation_id_or_options, str):
            parameters = GetConversationMessagesOptions(conversation_id=conversation_id_or_options)
        else:
            raise TypeError("conversation_id_or_options must be a conversation id or GetConversationMessagesOptions")

        parameters.validate()
        self._parameters = parameters

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[AiConversationMessagesResult]:
        return GetConversationMessagesOperation._GetConversationMessagesCommand(self._parameters)

    class _GetConversationMessagesCommand(RavenCommand[AiConversationMessagesResult]):
        def __init__(self, parameters: GetConversationMessagesOptions):
            super().__init__(AiConversationMessagesResult)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/ai/agent/conversation/messages"
                f"?conversationId={Utils.quote_key(self._parameters.conversation_id, True)}"
            )

            if self._parameters.before is not None:
                url += f"&before={Utils.quote_key(Utils.datetime_to_string(self._parameters.before))}"
            if self._parameters.after is not None:
                url += f"&after={Utils.quote_key(Utils.datetime_to_string(self._parameters.after))}"

            url += f"&pageSize={self._parameters.page_size}"
            url += f"&detailLevel={self._parameters.detail_level.value}"

            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            # A 404 for an unknown conversation yields a null result, not an exception.
            if response is None:
                return
            self.result = AiConversationMessagesResult.from_json(json.loads(response))
