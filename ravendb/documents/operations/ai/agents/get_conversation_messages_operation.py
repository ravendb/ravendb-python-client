from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import requests

from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.documents.operations.ai.agents.ai_conversation_detail_level import AiConversationDetailLevel
from ravendb.documents.operations.ai.agents.ai_conversation_messages_result import AiConversationMessagesResult

# RavenDB requires '/' in conversation IDs to be encoded (Uri.EscapeDataString)
_QUOTE_SAFE = ""


def _format_datetime_for_url(dt: datetime) -> str:
    """Format a datetime for use as a RavenDB query parameter.

    Produces the RavenDB-standard format: yyyy-MM-ddTHH:mm:ss.fffffffZ
    (7-digit fractional seconds, Z suffix for UTC).
    """
    # Ensure UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    elif dt.tzinfo != timezone.utc:
        dt = dt.astimezone(timezone.utc)
    # strftime %f gives 6 digits; pad to 7 by appending '0', then append Z
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "0Z"


class GetConversationMessagesOptions:
    """
    Parameters for reading messages from an AI agent conversation.

    Attributes:
        conversation_id: The conversation document ID. Required.
        before: Return messages older than this timestamp (exclusive upper bound).
            Used for backward paging (scrolling up in a chatbot UI).
        after: Return messages newer than this timestamp (exclusive lower bound).
            Used for catching up on new messages (e.g., after a Changes() notification).
        page_size: Maximum number of messages to return. Default: effectively unlimited
            (int.MaxValue = 2147483647).
        detail_level: Controls the level of detail in returned messages.
            Default: Simple.
    """

    DEFAULT_PAGE_SIZE: int = 2147483647  # int.MaxValue

    def __init__(
        self,
        conversation_id: Optional[str] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        detail_level: AiConversationDetailLevel = AiConversationDetailLevel.SIMPLE,
    ):
        self.conversation_id = conversation_id
        self.before = before
        self.after = after
        self.page_size = page_size
        self.detail_level = detail_level

    def validate(self) -> None:
        """Validates the options and raises appropriate exceptions."""
        if not self.conversation_id or not self.conversation_id.strip():
            raise ValueError("ConversationId cannot be None or empty")

        if self.before is not None and self.after is not None:
            raise ValueError("Before and After cannot both be specified.")

        if self.page_size <= 0:
            raise ValueError("PageSize must be greater than 0.")


class GetConversationMessagesOperation(MaintenanceOperation[AiConversationMessagesResult]):
    """
    Reads messages from an AI agent conversation, with optional timestamp-based
    paging and view filtering.
    """

    def __init__(self, conversation_id_or_options):
        """
        Initialize with either a conversation_id string or a GetConversationMessagesOptions instance.

        Args:
            conversation_id_or_options: A conversation ID string or GetConversationMessagesOptions instance.
        """
        if isinstance(conversation_id_or_options, GetConversationMessagesOptions):
            self._parameters = conversation_id_or_options
        elif isinstance(conversation_id_or_options, str):
            self._parameters = GetConversationMessagesOptions(conversation_id=conversation_id_or_options)
        else:
            raise TypeError(
                "Expected str or GetConversationMessagesOptions, got " f"{type(conversation_id_or_options).__name__}"
            )
        self._parameters.validate()

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AiConversationMessagesResult]:
        return GetConversationMessagesCommand(self._parameters)


class GetConversationMessagesCommand(RavenCommand[AiConversationMessagesResult]):
    def __init__(self, parameters: GetConversationMessagesOptions):
        super().__init__(AiConversationMessagesResult)
        self._parameters = parameters

    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> requests.Request:
        url = (
            f"{node.url}/databases/{node.database}/ai/agent/conversation/messages"
            f"?conversationId={quote(self._parameters.conversation_id, safe=_QUOTE_SAFE)}"
        )

        if self._parameters.before is not None:
            url += f"&before={quote(_format_datetime_for_url(self._parameters.before), safe=_QUOTE_SAFE)}"
        if self._parameters.after is not None:
            url += f"&after={quote(_format_datetime_for_url(self._parameters.after), safe=_QUOTE_SAFE)}"

        url += f"&pageSize={self._parameters.page_size}"
        url += f"&detailLevel={self._parameters.detail_level.value}"

        request = requests.Request("GET", url)
        return request

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self.result = None  # 404 — conversation not found
            return

        response_json = json.loads(response)
        self.result = AiConversationMessagesResult.from_json(response_json)
