from __future__ import annotations
import json
from typing import List, Dict, Any, Optional, Union, TypeVar, TYPE_CHECKING

from ravendb.documents.ai.ai_conversation_operations import IAiConversationOperations
from ravendb.documents.ai.ai_conversation_result import AiConversationResult

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.operations.ai.agents import (
        AiAgentActionRequest,
        AiAgentActionResponse,
        ConversationResult,
    )

TResponse = TypeVar("TResponse")


class AiConversation(IAiConversationOperations[TResponse]):
    """
    Implementation of AI conversation operations for managing conversations with AI agents.

    Can be used as a context manager for automatic cleanup:
        with store.ai.conversation(agent_id) as conversation:
            conversation.set_user_prompt("Hello!")
            result = conversation.run()
    """

    def __init__(
        self,
        store: DocumentStore,
        agent_id: str = None,
        parameters: Dict[str, Any] = None,
        conversation_id: str = None,
        change_vector: str = None,
    ):
        self._store = store
        self._agent_id = agent_id
        self._parameters = parameters or {}
        self._conversation_id = conversation_id
        self._change_vector = change_vector
        self._user_prompt: Optional[str] = None
        self._action_responses: List[AiAgentActionResponse] = []
        self._last_result: Optional[ConversationResult[TResponse]] = None

    @classmethod
    def with_conversation_id(
        cls, store: DocumentStore, conversation_id: str, change_vector: str = None
    ) -> AiConversation[TResponse]:
        """
        Creates a conversation instance for continuing an existing conversation.

        Args:
            store: The document store
            conversation_id: The ID of the existing conversation
            change_vector: Optional change vector for optimistic concurrency

        Returns:
            A new conversation instance
        """
        return cls(
            store=store,
            conversation_id=conversation_id,
            change_vector=change_vector,
        )

    @property
    def required_actions(self) -> List[AiAgentActionRequest]:
        """
        Gets the list of action requests that need to be fulfilled before
        the conversation can continue.
        """
        if self._last_result and self._last_result.action_requests:
            return self._last_result.action_requests
        return []

    def add_action_response(self, action_id: str, action_response: Union[str, TResponse]) -> None:
        """
        Adds a response for a given action request.

        Args:
            action_id: The ID of the action to respond to
            action_response: The response content (string or typed response object)
        """
        from ravendb.documents.operations.ai.agents import AiAgentActionResponse

        response = AiAgentActionResponse(tool_id=action_id)

        if isinstance(action_response, str):
            response.content = action_response
        else:
            # More robust JSON serialization
            try:
                response.content = json.dumps(
                    action_response.__dict__ if hasattr(action_response, "__dict__") else action_response, default=str
                )
            except (TypeError, ValueError) as e:
                response.content = str(action_response)

        self._action_responses.append(response)

    def run(self) -> AiConversationResult[TResponse]:
        """
        Executes one "turn" of the conversation:
        sends the current prompt, processes any required actions,
        and awaits the agent's reply.
        """
        from ravendb.documents.operations.ai.agents import RunConversationOperation

        if self._conversation_id:
            # Continue existing conversation
            if not self._agent_id:
                raise ValueError("Agent ID is required for conversation continuation")

            operation = RunConversationOperation(
                self._conversation_id,
                self._user_prompt,
                self._action_responses,
                self._change_vector,
            )
            # Set agent ID for conversation continuation
            operation._agent_id = self._agent_id
        else:
            # Start new conversation
            if not self._agent_id:
                raise ValueError("Agent ID is required for new conversations")

            operation = RunConversationOperation(
                self._agent_id,
                self._user_prompt,
                self._parameters,
            )

        # Execute the operation
        result = self._store.maintenance.send(operation)
        self._last_result = result

        # Update conversation state for future calls
        if result.conversation_id:
            self._conversation_id = result.conversation_id
        if result.change_vector:
            self._change_vector = result.change_vector

        # Preserve agent ID for future conversation turns
        if not self._agent_id and hasattr(operation, "_agent_id"):
            self._agent_id = operation._agent_id

        # Clear processed data for next turn
        self._user_prompt = None
        self._action_responses.clear()

        # Convert to AiConversationResult
        conversation_result = AiConversationResult[TResponse]()
        conversation_result.conversation_id = result.conversation_id
        conversation_result.change_vector = result.change_vector
        conversation_result.response = result.response
        conversation_result.usage = result.usage
        conversation_result.action_requests = result.action_requests or []

        return conversation_result

    def set_user_prompt(self, user_prompt: str) -> None:
        """
        Sets the next user prompt to send to the AI agent.

        Args:
            user_prompt: The prompt text to send to the agent

        Raises:
            ValueError: If user_prompt is empty or whitespace-only
        """
        if not user_prompt or user_prompt.isspace():
            raise ValueError("User prompt cannot be empty or whitespace-only")
        self._user_prompt = user_prompt

    def __enter__(self) -> AiConversation[TResponse]:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup resources."""
        # Clear any pending data
        self._user_prompt = None
        self._action_responses.clear()
