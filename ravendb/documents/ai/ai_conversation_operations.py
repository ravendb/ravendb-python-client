from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, TypeVar, Generic, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents import AiAgentActionRequest
    from ravendb.documents.ai.ai_conversation_result import AiConversationResult

TResponse = TypeVar("TResponse")


class IAiConversationOperations(ABC, Generic[TResponse]):
    """
    Interface for AI conversation operations, providing methods to manage
    conversations with AI agents including sending prompts, handling actions,
    and running conversation turns.
    """

    @property
    @abstractmethod
    def required_actions(self) -> List[AiAgentActionRequest]:
        """
        Gets the list of action requests that need to be fulfilled before
        the conversation can continue.

        Returns:
            List of action requests that require responses
        """
        pass

    @abstractmethod
    def add_action_response(self, action_id: str, action_response: Union[str, TResponse]) -> None:
        """
        Adds a response for a given action request.

        Args:
            action_id: The ID of the action to respond to
            action_response: The response content (string or typed response object)
        """
        pass

    @abstractmethod
    def run(self) -> AiConversationResult[TResponse]:
        """
        Executes one "turn" of the conversation:
        sends the current prompt, processes any required actions,
        and awaits the agent's reply.

        Returns:
            The result of the conversation turn
        """
        pass

    @abstractmethod
    def set_user_prompt(self, user_prompt: str) -> None:
        """
        Sets the next user prompt to send to the AI agent.

        Args:
            user_prompt: The prompt text to send to the agent
        """
        pass
