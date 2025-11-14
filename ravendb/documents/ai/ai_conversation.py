from __future__ import annotations

import json
import traceback
from typing import List, Dict, Any, Optional, TypeVar, TYPE_CHECKING, Callable

from ravendb.documents.ai.ai_conversation_result import AiConversationResult

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.operations.ai.agents import (
        AiAgentActionRequest,
        AiAgentActionResponse,
        ConversationResult,
    )

TResponse = TypeVar("TResponse")


class AiHandleErrorStrategy:
    SEND_ERRORS_TO_MODEL = "SendErrorsToModel"
    RAISE_IMMEDIATELY = "RaiseImmediately"


class AiConversation:
    """
    Implementation of AI conversation operations for managing conversations with AI agents.

    Can be used as a context manager for automatic cleanup:
        with store.ai.conversation(agent_id) as conversation:
            conversation.set_user_prompt("Hello!")
            result = conversation.run()
    """

    _invocations: Dict[str, Callable[[AiAgentActionRequest], None]] = {}

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
        self._last_result: Optional[ConversationResult] = None

    def __enter__(self) -> AiConversation:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup resources."""
        pass

    @classmethod
    def with_conversation_id(
        cls, store: DocumentStore, conversation_id: str, change_vector: str = None
    ) -> AiConversation:
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

    def add_action_response(self, action_id: str, action_response: str) -> None:
        """
        Adds a response for a given action request.

        Args:
            action_id: The ID of the action to respond to
            action_response: The response content
        """
        from ravendb.documents.operations.ai.agents import AiAgentActionResponse

        response = AiAgentActionResponse(tool_id=action_id)

        if isinstance(action_response, str):
            response.content = action_response

        self._action_responses.append(response)

    def run(self) -> AiConversationResult:
        """
        Executes one "turn" of the conversation:
        sends the current prompt or replies to any required actions,
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
        conversation_result = AiConversationResult()
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

    def handle(
        self,
        action_name: str,
        action: Callable[[dict], Any],
        ai_handle_error: AiHandleErrorStrategy,
    ) -> None:
        self.handle_ai_agent_action_request(action_name, lambda _, args: action(args), ai_handle_error)

    def handle_ai_agent_action_request(
        self,
        action_name: str,
        action: Callable[[AiAgentActionRequest, dict], Any],
        ai_handle_error: AiHandleErrorStrategy = AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL,
    ) -> None:
        def wrapped_no_return(request: AiAgentActionRequest, args: dict) -> Any:
            result = action(request, args)
            self.add_action_response(request.tool_id, result)

        self.receive(action_name, wrapped_no_return, ai_handle_error)

    def receive(
        self,
        action_name: str,
        action: Callable[[AiAgentActionRequest, dict], None],
        ai_handle_error: AiHandleErrorStrategy = AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL,
    ):
        t = self.AiActionContext(self, lambda request, args: action(request, args), ai_handle_error)
        self._add_action(action_name, t.execute)

    def _add_action(self, action_name: str, action: Callable[[AiAgentActionRequest], Any]):
        if action_name in self._invocations:
            raise ValueError(f"Action '{action_name}' already exists")

        self._invocations[action_name] = action

    class AiActionContext:
        def __init__(
            self,
            conversation: AiConversation,
            action: Callable[[AiAgentActionRequest, dict], Any],
            ai_handle_error: AiHandleErrorStrategy,
        ):
            self._conversation = conversation
            self._action = action
            self._ai_handle_error = ai_handle_error

        def execute(self, action_request: AiAgentActionRequest):
            args = json.loads(action_request.arguments)
            self.invoke(action_request, args)

        def invoke(self, action_request: AiAgentActionRequest, args: dict):
            try:
                self._action(action_request, args)
            except Exception as e:
                if self._ai_handle_error == AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL:
                    self._conversation.add_action_response(action_request.tool_id, self.create_error_message_for_llm(e))
                else:
                    raise e

        @staticmethod
        def create_error_message_for_llm(exc: Exception) -> str:
            parts = []

            current = exc
            indent = 0

            while current is not None:
                prefix = "  " * indent
                header = f"{prefix}{current.__class__.__name__}: {current}"
                parts.append(header)

                tb = "".join(traceback.format_exception(type(current), current, current.__traceback__))
                tb_lines = tb.strip().splitlines()

                # indent the traceback block
                indented_tb = "\n".join(prefix + "  " + line for line in tb_lines)
                parts.append(indented_tb)

                # Move to next exception in the chain
                if current.__cause__:
                    current = current.__cause__
                elif current.__context__ and not current.__suppress_context__:
                    current = current.__context__
                else:
                    current = None

                indent += 1

            return "\n".join(parts)
