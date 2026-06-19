from __future__ import annotations

import json
import traceback
from typing import List, Dict, Any, IO, Optional, TypeVar, TYPE_CHECKING, Callable, Union
from datetime import timedelta

from ravendb.documents.ai.ai_answer import AiAnswer, AiConversationStatus
from ravendb.documents.ai.content_part import ContentPart, TextPart
from ravendb.documents.operations.ai.agents import (
    AiAgentActionRequest,
    AiAgentActionResponse,
    AiConversationCreationOptions,
)
from ravendb.exceptions.exceptions import InvalidOperationException
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
    # Usable as a context manager: `with store.ai.conversation(agent_id) as c:`.

    def __init__(
        self,
        store: DocumentStore,
        agent_id: str = None,
        options: AiConversationCreationOptions = None,
        conversation_id: str = None,
        change_vector: str = None,
        debug: Optional[bool] = None,
    ):
        self._store = store
        self._agent_id = agent_id
        self._options = options or AiConversationCreationOptions()
        self._conversation_id = conversation_id
        self._change_vector = change_vector
        self._debug = debug

        self._prompt_parts: List[ContentPart] = []
        self._action_responses: Dict[str, AiAgentActionResponse] = {}
        self._artificial_actions: List[AiAgentArtificialActionResponse] = []
        self._action_requests: Optional[List[AiAgentActionRequest]] = None
        self._attachments_commands: List = []
        self._dispatched_tool_ids: set = set()

        self._invocations: Dict[str, Callable[[AiAgentActionRequest], None]] = {}
        self.on_unhandled_action: Optional[Callable[[UnhandledActionEventArgs], None]] = None

    def add_attachment(self, name: str, stream: Union[bytes, IO[bytes]], content_type: str) -> None:
        # `stream` is raw bytes or any binary file-like; each stream may only
        # be used once per turn (SingleNodeBatchCommand enforces uniqueness).
        if stream is None:
            raise ValueError("stream cannot be None")
        from ravendb.documents.commands.batches import PutAttachmentCommandData

        self._attachments_commands.append(
            PutAttachmentCommandData("__this__", name, stream, content_type, change_vector=None)
        )

    def copy_attachment_from(self, source_document_id: str, file_name: str) -> None:
        if not source_document_id or (isinstance(source_document_id, str) and source_document_id.isspace()):
            raise ValueError("source_document_id cannot be None or empty")
        if not file_name or (isinstance(file_name, str) and file_name.isspace()):
            raise ValueError("file_name cannot be None or empty")
        from ravendb.documents.commands.batches import CopyAttachmentCommandData

        self._attachments_commands.append(
            CopyAttachmentCommandData(source_document_id, file_name, "__this__", file_name, change_vector=None)
        )

    def __enter__(self) -> AiConversation:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    @classmethod
    def with_conversation_id(
        cls, store: DocumentStore, conversation_id: str, change_vector: str = None
    ) -> AiConversation:
        return cls(
            store=store,
            conversation_id=conversation_id,
            change_vector=change_vector,
        )

    @property
    def required_actions(self) -> List[AiAgentActionRequest]:
        if self._action_requests is None:
            raise RuntimeError("You have to call run() first")
        return self._action_requests

    def add_action_response(self, action_id: str, action_response: str) -> None:
        from ravendb.documents.operations.ai.agents import AiAgentActionResponse

        if action_id in self._action_responses:
            raise InvalidOperationException(
                f"An action response for tool-id '{action_id}' was already added. "
                f"Each tool call must have exactly one response. "
                f"If you're using handle, return the value from the handler (don't call add_action_response manually)."
            )

        response = AiAgentActionResponse(tool_id=action_id)

        if isinstance(action_response, str):
            response.content = action_response

        self._action_responses[action_id] = response

    def add_artificial_action_with_response(self, tool_id: str, action_response) -> None:
        # Injects a synthetic tool-call + response so the agent "believes" it
        # already executed `tool_id` and got `action_response` back.
        if not tool_id or (isinstance(tool_id, str) and tool_id.isspace()):
            raise ValueError("tool_id cannot be None or empty")
        if action_response is None:
            raise ValueError(f"Action response for '{tool_id}' cannot be None.")

        if isinstance(action_response, str):
            content = action_response
        else:
            content = json.dumps(action_response)

        self._artificial_actions.append(AiAgentArtificialActionResponse(tool_id=tool_id, content=content))

    def run(self) -> AiAnswer:
        self._dispatched_tool_ids.clear()

        while True:
            r = self._run_internal()
            if self._handle_server_reply(r):
                return r

    def stream(self, stream_property_path: str = None, on_chunk: Optional[Callable[[str], None]] = None) -> AiAnswer:
        while True:
            r = self._run_internal(stream_property_path=stream_property_path, streamed_chunks_callback=on_chunk)
            if self._handle_server_reply(r):
                return r

    def _run_internal(
        self,
        stream_property_path: Optional[str] = None,
        streamed_chunks_callback: Optional[Callable[[str], None]] = None,
    ) -> AiAnswer:
        from ravendb.documents.operations.ai.agents import RunConversationOperation
        import time

        # Already round-tripped and nothing new to send (no pending actions either).
        if (
            self._action_requests is not None
            and len(self._action_requests) == 0
            and len(self._prompt_parts) == 0
            and len(self._action_responses) == 0
            and len(self._artificial_actions) == 0
            and len(self._attachments_commands) == 0
        ):
            return AiAnswer(
                answer=None,
                status=AiConversationStatus.DONE,
                usage=None,
                elapsed=None,
            )

        if not self._agent_id:
            raise ValueError("Agent ID is required")

        # Trailing "/" tells the server to assign a unique id.
        if not self._conversation_id:
            self._conversation_id = "conversations/"

        operation = RunConversationOperation(
            agent_id=self._agent_id,
            conversation_id=self._conversation_id,
            prompt_parts=self._prompt_parts,
            action_responses=list(self._action_responses.values()),
            artificial_actions=self._artificial_actions,
            options=self._options,
            change_vector=self._change_vector,
            stream_property_path=stream_property_path,
            streamed_chunks_callback=streamed_chunks_callback,
            attachments_commands=self._attachments_commands,
            debug=self._debug,
        )

        try:
            start_time = time.time()
            result = self._store.maintenance.send(operation)
            elapsed = timedelta(seconds=time.time() - start_time)

            self._change_vector = result.change_vector
            self._conversation_id = result.conversation_id
            self._action_requests = result.action_requests or []

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
        finally:
            self._prompt_parts.clear()
            self._action_responses.clear()
            self._artificial_actions.clear()
            self._attachments_commands.clear()

    def _handle_server_reply(self, answer: AiAnswer) -> bool:
        # Returns True when the conversation is done.
        if answer.status == AiConversationStatus.DONE:
            return True

        if len(self._action_requests) == 0:
            raise RuntimeError(
                f"There are no action requests to process, but Status was {answer.status}, should not be possible."
            )

        for action in self._action_requests:
            # Skip actions we've already dispatched in a previous turn of this run
            if action.tool_id in self._dispatched_tool_ids:
                continue
            self._dispatched_tool_ids.add(action.tool_id)

            if action.name in self._invocations:
                self._invocations[action.name](action)
            elif self.on_unhandled_action is not None:
                self.on_unhandled_action(UnhandledActionEventArgs(self, action))
            else:
                raise RuntimeError(
                    f"There is no action defined for action '{action.name}' on agent '{self._agent_id}' "
                    f"({self._conversation_id}), but it was invoked by the model with: {action.arguments}. "
                    f"Did you forget to call {self.receive.__name__}() or {self.handle.__name__}()? You can also handle unexpected action invocations using the 'on_unhandled_action' event."
                )

        # No responses to deliver => nothing more to tell the server.
        return len(self._action_responses) == 0

    def set_user_prompt(self, user_prompt: str) -> None:
        if not user_prompt or user_prompt.isspace():
            raise ValueError("User prompt cannot be empty or whitespace-only")
        self._prompt_parts.clear()
        self.add_user_prompt(user_prompt)

    def add_user_prompt(self, *prompts: str) -> None:
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
