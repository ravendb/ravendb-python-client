from __future__ import annotations
from typing import Optional, List, TypeVar, Generic, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents import AiAgentActionRequest, AiUsage

TResponse = TypeVar("TResponse")


class AiConversationResult(Generic[TResponse]):
    """
    Represents the result of an AI conversation turn, containing the agent's response,
    usage statistics, and any action requests that need to be fulfilled.
    """

    def __init__(self):
        self.conversation_id: Optional[str] = None
        self.change_vector: Optional[str] = None
        self.response: Optional[TResponse] = None
        self.usage: Optional[AiUsage] = None
        self.action_requests: List[AiAgentActionRequest] = []

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AiConversationResult(conversation_id={self.conversation_id!r}, "
            f"has_response={self.response is not None}, "
            f"action_requests={len(self.action_requests)})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"AiConversationResult(conversation_id={self.conversation_id!r}, "
            f"change_vector={self.change_vector!r}, "
            f"response={self.response!r}, "
            f"usage={self.usage!r}, "
            f"action_requests={self.action_requests!r})"
        )

    @property
    def has_action_requests(self) -> bool:
        """
        Returns True if there are action requests that need to be fulfilled.
        """
        return bool(self.action_requests)

    def get_action_request_by_id(self, action_id: str) -> Optional[AiAgentActionRequest]:
        """
        Gets an action request by its tool ID.

        Args:
            action_id: The tool ID of the action request

        Returns:
            The action request if found, None otherwise
        """
        return next((request for request in self.action_requests if request.tool_id == action_id), None)
