from __future__ import annotations

import json
import traceback
from typing import List, Dict, Any, Optional, TypeVar, TYPE_CHECKING, Callable
from datetime import timedelta

from ravendb.documents.ai.ai_answer import AiAnswer, AiConversationStatus
from ravendb.documents.ai.content_part import ContentPart, TextPart
from ravendb.documents.operations.ai.agents import (
    AiAgentActionRequest,
    AiAgentActionResponse,
    AiConversationCreationOptions,
)
from ravendb.documents.operations.ai.agents.run_conversation_operation import AiAgentArtificialActionResponse

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore

TResponse = TypeVar("TResponse")


class AiHandleErrorStrategy:
    SEND_ERRORS_TO_MODEL = "SendErrorsToModel"
    RAISE_IMMEDIATELY = "RaiseImmediately"


class UnhandledActionEventArgs:
    def __init__(self, sender: AiConversation, action: AiAgentActionRequest):
        self.sender = sender
        self.action = action


class AiConversation:
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
        options: AiConversationCreationOptions = None,
        conversation_id: str = None,
        change_vector: str = None,
    ):
        self._store = store
        self._agent_id = agent_id
        self._options = options or AiConversationCreationOptions()
        self._conversation_id = conversation_id
        self._change_vector = change_vector

        self._prompt_parts: List[ContentPart] = []
        self._action_responses: List[AiAgentActionResponse] = []
        self._artificial_actions: List[AiAgentArtificialActionResponse] = []
        self._action_requests: Optional[List[AiAgentActionRequest]] = None

        # Action handlers
        self._invocations: Dict[str, Callable[[AiAgentActionRequest], None]] = {}

        self.on_unhandled_action: Optional[Callable[[UnhandledActionEventArgs], None]] = None

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

        Raises:
            RuntimeError: If run() hasn't been called yet
        """
        if self._action_requests is None:
            raise RuntimeError("You have to call run() first")
        return self._action_requests

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

    def add_artificial_action_with_response(self, tool_id: str, action_response) -> None:
        """
        Injects an artificial action (tool call) and a response into the model's conversation context.
        This is an advanced mechanism to programmatically prompt the agent, causing it to "believe"
        it successfully executed a tool and received the specified action_response.

        Args:
            tool_id: The name of the tool to simulate the agent called.
            action_response: The response to supply to the agent as the result of the simulated action.
                            Can be a string or any object that will be serialized to JSON.
        """
        if not tool_id or (isinstance(tool_id, str) and tool_id.isspace()):
            raise ValueError("tool_id cannot be None or empty")
        if action_response is None:
            raise ValueError(f"Action response for '{tool_id}' cannot be None.")

        if isinstance(action_response, str):
            content = action_response
        else:
            content = json.dumps(action_response)

        self._artificial_actions.append(
            AiAgentArtificialActionResponse(tool_id=tool_id, content=content)
        )

    def run(self) -> AiAnswer:
        """
        Executes the conversation loop, automatically handling action requests
        until the conversation is complete or no handlers are available.

        Returns:
            AiAnswer with the final response, status, usage, and elapsed time
        """
        while True:
            r = self._run_internal()
            if self._handle_server_reply(r):
                return r

    def stream(self, stream_property_path: str = None, on_chunk: Optional[Callable[[str], None]] = None) -> AiAnswer:
        """
        Stream the LLM response for the given property and return the final AiAnswer when done.
        """
        while True:
            r = self._run_internal(stream_property_path=stream_property_path, streamed_chunks_callback=on_chunk)
            if self._handle_server_reply(r):
                return r

    def _run_internal(
        self,
        stream_property_path: Optional[str] = None,
        streamed_chunks_callback: Optional[Callable[[str], None]] = None,
    ) -> AiAnswer:
        """
        Internal method that executes a single server call.

        Returns:
            AiAnswer from this single turn
        """
        from ravendb.documents.operations.ai.agents import RunConversationOperation
        import time

        # If we already went to the server and have nothing new to tell it, we're done
        if (
            self._action_requests is not None
            and len(self._prompt_parts) == 0
            and len(self._action_responses) == 0
            and len(self._artificial_actions) == 0
        ):
            return AiAnswer(
                answer=None,
                status=AiConversationStatus.DONE,
                usage=None,
                elapsed=None,
            )

        # Build the operation
        if not self._agent_id:
            raise ValueError("Agent ID is required")

        # If we don't have a conversation ID yet, generate one with the prefix
        # The server will complete it with a unique ID
        if not self._conversation_id:
            self._conversation_id = "conversations/"

        # Create operation with all required parameters
        operation = RunConversationOperation(
            agent_id=self._agent_id,
            conversation_id=self._conversation_id,
            prompt_parts=self._prompt_parts,  # Always send list, even if empty
            action_responses=self._action_responses,  # Always send list, even if empty
            artificial_actions=self._artificial_actions,  # Always send list, even if empty
            options=self._options,
            change_vector=self._change_vector,
            stream_property_path=stream_property_path,
            streamed_chunks_callback=streamed_chunks_callback,
        )

        try:
            # Track elapsed time
            start_time = time.time()
            result = self._store.maintenance.send(operation)
            elapsed = timedelta(seconds=time.time() - start_time)

            # Update conversation state
            self._change_vector = result.change_vector
            self._conversation_id = result.conversation_id
            self._action_requests = result.action_requests or []

            # Build AiAnswer
            return AiAnswer(
                answer=result.response,
                status=(
                    AiConversationStatus.ACTION_REQUIRED
                    if len(self._action_requests) > 0
                    else AiConversationStatus.DONE
                ),
                usage=result.usage,
                elapsed=elapsed,
            )
        # except ConcurrencyException as e:
        #     self._change_vector = e.actual_change_vector
        #     raise
        finally:
            # Clear the user prompt and tool responses after running the conversation
            self._prompt_parts.clear()
            self._action_responses.clear()
            self._artificial_actions.clear()

    def _handle_server_reply(self, answer: AiAnswer) -> bool:
        """
        Handles the server reply by invoking registered action handlers.

        Args:
            answer: The answer from the server

        Returns:
            True if the conversation is done, False if it should continue
        """
        if answer.status == AiConversationStatus.DONE:
            return True

        if len(self._action_requests) == 0:
            raise RuntimeError(
                f"There are no action requests to process, but Status was {answer.status}, should not be possible."
            )

        # Process each action request
        for action in self._action_requests:
            if action.name in self._invocations:
                # Invoke the registered handler
                # Error handling is done by the invocation based on the error strategy
                self._invocations[action.name](action)
            elif self.on_unhandled_action is not None:
                self.on_unhandled_action(UnhandledActionEventArgs(self, action))
            else:
                # No handler registered for this action
                raise RuntimeError(
                    f"There is no action defined for action '{action.name}' on agent '{self._agent_id}' "
                    f"({self._conversation_id}), but it was invoked by the model with: {action.arguments}. "
                    f"Did you forget to call {self.receive.__name__}() or {self.handle.__name__}()? You can also handle unexpected action invocations using the 'on_unhandled_action' event."
                )

        # If we have nothing to tell the server (no action responses), we're done
        # Otherwise, continue the loop to send the responses
        return len(self._action_responses) == 0

    def set_user_prompt(self, user_prompt: str) -> None:
        """
        Sets the user prompt to send to the AI agent.
        Clears any existing prompt parts and adds the new prompt.

        Args:
            user_prompt: The prompt text to send to the agent

        Raises:
            ValueError: If user_prompt is empty or whitespace-only
        """
        if not user_prompt or user_prompt.isspace():
            raise ValueError("User prompt cannot be empty or whitespace-only")
        self._prompt_parts.clear()
        self.add_user_prompt(user_prompt)

    def add_user_prompt(self, *prompts: str) -> None:
        """
        Adds one or more user prompts to the conversation.

        Args:
            *prompts: One or more prompt strings to add

        Raises:
            ValueError: If any prompt is empty or whitespace-only
        """
        for prompt in prompts:
            if not prompt or prompt.isspace():
                raise ValueError("User prompt cannot be empty or whitespace-only")
            self._prompt_parts.append(TextPart(prompt))

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
