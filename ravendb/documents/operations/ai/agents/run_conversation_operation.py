from __future__ import annotations
import enum
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, TypeVar, Generic, Callable

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from ravendb.http.server_node import ServerNode
import requests
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.documents.ai.ai_output_options import AiOutputOptions
from ravendb.documents.ai.content_part import ContentPart

TSchema = TypeVar("TSchema")


class AiAgentActionRequestType(enum.Enum):
    USER_ACTION = "UserAction"
    SUB_AGENT = "SubAgent"

    def __str__(self) -> str:
        return self.value


class AiAgentActionRequest:
    def __init__(
        self,
        name: str = None,
        tool_id: str = None,
        arguments: str = None,
        type: AiAgentActionRequestType = AiAgentActionRequestType.USER_ACTION,
        sub_conversation_id: Optional[str] = None,
    ):
        self.name = name
        self.tool_id = tool_id
        self.arguments = arguments
        self.type = type
        self.sub_conversation_id = sub_conversation_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentActionRequest:
        return cls(
            name=json_dict.get("Name"),
            tool_id=json_dict.get("ToolId"),
            arguments=json_dict.get("Arguments"),
            type=AiAgentActionRequestType(json_dict.get("Type") or "UserAction"),
            sub_conversation_id=json_dict.get("SubConversationId"),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "ToolId": self.tool_id,
            "Arguments": self.arguments,
            "Type": self.type.value,
            "SubConversationId": self.sub_conversation_id,
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AiAgentActionRequest):
            return False
        return (
            self.tool_id == other.tool_id
            and self.name == other.name
            and self.arguments == other.arguments
            and self.type == other.type
            and self.sub_conversation_id == other.sub_conversation_id
        )

    def __hash__(self) -> int:
        return hash((self.tool_id, self.name, self.arguments, self.type, self.sub_conversation_id))

    def __repr__(self) -> str:
        return json.dumps(self.to_json())


@dataclass
class AiAgentActionResponse:
    tool_id: Optional[str] = None
    content: Optional[str] = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentActionResponse:
        return cls(tool_id=json_dict.get("ToolId"), content=json_dict.get("Content"))

    def to_json(self) -> Dict[str, Any]:
        return {
            "ToolId": self.tool_id,
            "Content": self.content,
        }


@dataclass
class AiAgentArtificialActionResponse:
    # Synthetic (tool_id, content) pair injected to make the model "believe"
    # it executed a tool. Sent in addition to a real ActionResponses entry.
    tool_id: Optional[str] = None
    content: Optional[str] = None

    def validate(self) -> None:
        if not self.tool_id or self.tool_id.isspace():
            raise ValueError("tool_id cannot be None or empty")
        if not self.content or self.content.isspace():
            raise ValueError("content cannot be None or empty")

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentArtificialActionResponse:
        return cls(tool_id=json_dict["ToolId"], content=json_dict.get["Content"])

    def to_json(self) -> Dict[str, Any]:
        return {
            "ToolId": self.tool_id,
            "Content": self.content,
        }


@dataclass
class AiUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cached_tokens: int = 0
    reasoning_tokens: int = 0

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiUsage:
        return cls(
            prompt_tokens=json_dict.get("PromptTokens", 0),
            completion_tokens=json_dict.get("CompletionTokens", 0),
            total_tokens=json_dict.get("TotalTokens", 0),
            cached_tokens=json_dict.get("CachedTokens", 0),
            reasoning_tokens=json_dict.get("ReasoningTokens", 0),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "PromptTokens": self.prompt_tokens,
            "CompletionTokens": self.completion_tokens,
            "TotalTokens": self.total_tokens,
            "CachedTokens": self.cached_tokens,
            "ReasoningTokens": self.reasoning_tokens,
        }

    @staticmethod
    def get_usage_difference(current: AiUsage, previous: AiUsage) -> AiUsage:
        # cached/completion/reasoning are last-response-only, so they pass
        # through. prompt/total are clamped against bogus negative model output.
        previous_total_without_reasoning = (
            previous.completion_tokens - previous.reasoning_tokens + previous.prompt_tokens
        )
        return AiUsage(
            prompt_tokens=max(current.prompt_tokens - previous_total_without_reasoning, 0),
            total_tokens=max(current.total_tokens - previous_total_without_reasoning, 0),
            cached_tokens=current.cached_tokens,
            completion_tokens=current.completion_tokens,
            reasoning_tokens=current.reasoning_tokens,
        )


class ConversationResult(Generic[TSchema]):
    def __init__(
        self,
        conversation_id: Optional[str] = None,
        change_vector: Optional[str] = None,
        response: Optional[TSchema] = None,
        usage: Optional[AiUsage] = None,
        action_requests: Optional[List[AiAgentActionRequest]] = None,
    ):
        self.conversation_id: Optional[str] = conversation_id
        self.change_vector: Optional[str] = change_vector
        self.response: Optional[TSchema] = response
        self.usage: Optional[AiUsage] = usage
        self.action_requests: List[AiAgentActionRequest] = action_requests or []

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConversationResult:
        return cls(
            conversation_id=json_dict.get("ConversationId"),
            change_vector=json_dict.get("ChangeVector"),
            response=json_dict.get("Response"),
            usage=AiUsage.from_json(json_dict["Usage"]) if json_dict.get("Usage") else None,
            action_requests=(
                [AiAgentActionRequest.from_json(req) for req in json_dict["ActionRequests"]]
                if json_dict.get("ActionRequests")
                else None
            ),
        )


class AiConversationParameterOptions:
    def __init__(self, send_to_model: bool = True):
        self.send_to_model = send_to_model


class AiConversationParameter:
    def __init__(self, value: Any = None, send_to_model: bool = True):
        self.value = value
        self.send_to_model = send_to_model

    def to_json(self) -> Dict[str, Any]:
        return {
            "Value": self.value.to_json() if callable(getattr(self.value, "to_json", None)) else self.value,
            "SendToModel": self.send_to_model,
        }


class AiConversationCreationOptions:
    def __init__(
        self,
        parameters: Optional[Dict[str, Any]] = None,
        expiration_in_sec: Optional[int] = None,
        max_model_iterations_per_call: Optional[int] = None,
    ):
        self.expiration_in_sec: Optional[int] = expiration_in_sec
        self.max_model_iterations_per_call: Optional[int] = max_model_iterations_per_call
        self.parameters: Optional[Dict[str, AiConversationParameter]] = None
        if parameters:
            for name, value in parameters.items():
                self.add_parameter(name, value)

    def add_parameter(
        self,
        name: str,
        value: Any,
        options: Optional[AiConversationParameterOptions] = None,
    ) -> AiConversationCreationOptions:
        # `value` may be a raw value (wrapped here) or an AiConversationParameter.
        if self.parameters is None:
            self.parameters = {}
        if not isinstance(value, AiConversationParameter):
            value = AiConversationParameter(
                value=value,
                send_to_model=options.send_to_model if options else True,
            )
        self.parameters[name] = value
        return self

    def to_json(self) -> Dict[str, Any]:
        return {
            "ExpirationInSec": self.expiration_in_sec,
            "MaxModelIterationsPerCall": self.max_model_iterations_per_call,
            "Parameters": (
                {name: param.to_json() for name, param in self.parameters.items()}
                if self.parameters is not None
                else None
            ),
        }


class ConversationRequestBody:
    def __init__(
        self,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = None,
        user_prompt: Optional[List[ContentPart]] = None,
        creation_options: Optional[AiConversationCreationOptions] = None,
        attachment_commands: Optional[List[Any]] = None,
        output_options: Optional[AiOutputOptions] = None,
    ):
        self.action_responses: Optional[List[AiAgentActionResponse]] = action_responses
        self.artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = artificial_actions
        self.user_prompt: Optional[List[ContentPart]] = user_prompt
        self.creation_options: Optional[AiConversationCreationOptions] = creation_options
        self.attachment_commands: Optional[List[Any]] = attachment_commands
        self.output_options: Optional[AiOutputOptions] = output_options

    def to_json(self) -> Dict[str, Any]:
        body = {
            "ActionResponses": (
                [resp.to_json() for resp in self.action_responses] if self.action_responses is not None else None
            ),
            "ArtificialActions": (
                [resp.to_json() for resp in self.artificial_actions] if self.artificial_actions is not None else None
            ),
            "CreationOptions": (self.creation_options or AiConversationCreationOptions()).to_json(),
            "UserPrompt": [part.to_json() for part in self.user_prompt] if self.user_prompt is not None else None,
            "AttachmentCommands": (
                [cmd.serialize(None) for cmd in self.attachment_commands]
                if self.attachment_commands is not None
                else None
            ),
        }

        # Only sent when the caller overrides the agent's own schema for this turn.
        if self.output_options is not None:
            body["OutputOptions"] = self.output_options.to_json()

        return body


class RunConversationOperation(MaintenanceOperation[ConversationResult[TSchema]]):
    def __init__(
        self,
        agent_id: str,
        conversation_id: str,
        prompt_parts: Optional[List[ContentPart]] = None,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = None,
        options: Optional[AiConversationCreationOptions] = None,
        change_vector: Optional[str] = None,
        stream_property_path: Optional[str] = None,
        streamed_chunks_callback: Optional[Callable[[str], None]] = None,
        attachments_commands: Optional[List[Any]] = None,
        output_options: Optional[AiOutputOptions] = None,
        debug: Optional[bool] = None,
        cancel_pending_action_tools: bool = False,
    ):
        if not agent_id or (isinstance(agent_id, str) and agent_id.isspace()):
            raise ValueError("agent_id cannot be None or empty")
        if not conversation_id or (isinstance(conversation_id, str) and conversation_id.isspace()):
            raise ValueError("conversation_id cannot be None or empty")
        if (stream_property_path is None) != (streamed_chunks_callback is None):
            raise ValueError("Both stream_property_path and streamed_chunks_callback must be specified together")

        self._agent_id = agent_id
        self._conversation_id = conversation_id
        self._prompt_parts = prompt_parts
        self._action_responses = action_responses
        self._artificial_actions = artificial_actions or []
        self._options = options
        self._change_vector = change_vector
        self._stream_property_path = stream_property_path
        self._streamed_chunks_callback = streamed_chunks_callback
        self._attachments_commands = attachments_commands or []
        self._output_options = output_options
        self._debug = debug
        self._cancel_pending_action_tools = cancel_pending_action_tools

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[ConversationResult[TSchema]]:
        return RunConversationCommand(
            agent_id=self._agent_id,
            conversation_id=self._conversation_id,
            prompt_parts=self._prompt_parts,
            action_responses=self._action_responses,
            artificial_actions=self._artificial_actions,
            options=self._options,
            change_vector=self._change_vector,
            stream_property_path=self._stream_property_path,
            streamed_chunks_callback=self._streamed_chunks_callback,
            conventions=conventions,
            attachments_commands=self._attachments_commands,
            output_options=self._output_options,
            debug=self._debug,
            cancel_pending_action_tools=self._cancel_pending_action_tools,
        )


class RunConversationCommand(RavenCommand[ConversationResult[TSchema]]):
    def __init__(
        self,
        agent_id: str,
        conversation_id: str,
        prompt_parts: Optional[List[ContentPart]] = None,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = None,
        options: Optional[AiConversationCreationOptions] = None,
        change_vector: Optional[str] = None,
        stream_property_path: Optional[str] = None,
        streamed_chunks_callback: Optional[Callable[[str], None]] = None,
        conventions: Optional[DocumentConventions] = None,
        attachments_commands: Optional[List[Any]] = None,
        output_options: Optional[AiOutputOptions] = None,
        debug: Optional[bool] = None,
        cancel_pending_action_tools: bool = False,
    ):
        from ravendb.util.util import RaftIdGenerator
        from ravendb.documents.commands.batches import PutAttachmentCommandData

        super().__init__(ConversationResult)
        self._agent_id = agent_id
        self._conversation_id = conversation_id
        self._prompt_parts = prompt_parts
        self._action_responses = action_responses
        self._artificial_actions = artificial_actions or []
        self._options = options
        self._change_vector = change_vector
        self._stream_property_path = stream_property_path
        self._streamed_chunks_callback = streamed_chunks_callback
        self._conventions = conventions
        self._output_options = output_options
        self._debug = debug
        self._cancel_pending_action_tools = cancel_pending_action_tools
        self._attachments_commands = attachments_commands or []

        # Raft id pinned at construction so retries keep the same id.
        self._raft_id = (
            RaftIdGenerator.new_id() if self._conversation_id.endswith("|") else RaftIdGenerator.dont_care_id()
        )

        # Each PutAttachmentCommandData must carry a unique stream — re-using
        # a stream across commands corrupts the multipart upload.
        seen_streams = set()
        self._put_attachments: List[PutAttachmentCommandData] = []
        for cmd in self._attachments_commands:
            if isinstance(cmd, PutAttachmentCommandData):
                stream = cmd.stream
                if stream is None:
                    continue
                stream_id = id(stream)
                if stream_id in seen_streams:
                    raise RuntimeError(
                        "It is forbidden to re-use the same stream for more than one attachment. "
                        "Use a unique stream per put attachment command."
                    )
                seen_streams.add(stream_id)
                self._put_attachments.append(cmd)

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        from urllib.parse import quote
        from ravendb.primitives.constants import Headers

        url = (
            f"{node.url}/databases/{node.database}/ai/agent"
            f"?conversationId={quote(self._conversation_id)}"
            f"&agentId={quote(self._agent_id)}"
        )
        if self._change_vector:
            url += f"&changeVector={quote(self._change_vector)}"
        if self._stream_property_path:
            url += f"&streaming=true&streamPropertyPath={quote(self._stream_property_path)}"

        # Add debug flag if requested
        if self._debug is not None:
            url += f"&debug={self._debug}"

        # Always sent: the server distinguishes "cancel the tool calls still pending" from
        # "answer them", and has no default of its own.
        url += f"&cancelPendingActionTools={self._cancel_pending_action_tools}"

        request_body = ConversationRequestBody(
            action_responses=self._action_responses,
            artificial_actions=self._artificial_actions,
            user_prompt=self._prompt_parts,
            creation_options=self._options,
            attachment_commands=self._attachments_commands if self._attachments_commands else None,
            output_options=self._output_options,
        )
        body = json.dumps(request_body.to_json())
        request = requests.Request("POST", url)

        if self._attachments_commands:
            # Positional multipart matching the server's MultipartReader
            # (AbstractAiAgentProcessor.ParseMultipartAsync on v7.2):
            #   0: conversation body, 1: {"Commands": [...]}, 2+: streams.
            commands_payload = json.dumps(
                {"Commands": [cmd.serialize(self._conventions) for cmd in self._attachments_commands]}
            )
            files = {
                "body": (None, body, "application/json"),
                "commands": (None, commands_payload, "application/json"),
            }
            for put in self._put_attachments:
                files[put.name] = (
                    put.name,
                    put.stream,
                    put.content_type,
                    {Headers.COMMAND_TYPE: Headers.ATTACHMENT_STREAM},
                )
            request.files = files
        else:
            request.headers = {"Content-Type": "application/json"}
            request.data = body
        return request

    # todo: rewrite via custom set_response_raw + RAW response type
    def process_response(self, cache, response: requests.Response, url) -> ResponseDisposeHandling:
        if not self._stream_property_path:
            return super().process_response(cache, response, url)

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            if line.startswith("{"):
                response_json = json.loads(line)
                self.result = ConversationResult.from_json(response_json)
                return ResponseDisposeHandling.AUTOMATIC
            # Non-final lines are JSON-encoded chunks (e.g. "\\\"chunk\\\"").
            try:
                chunk = json.loads(line)
            except Exception:
                chunk = line
            if self._streamed_chunks_callback:
                self._streamed_chunks_callback(chunk)
        self.result = ConversationResult()
        return ResponseDisposeHandling.AUTOMATIC

    def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
        if self._stream_property_path:
            from ravendb.util.request_utils import RequestUtils

            prepared_request = session.prepare_request(request)
            RequestUtils.remove_zstd_encoding(prepared_request)
            return session.send(prepared_request, cert=session.cert, stream=True)
        return super().send(session, request)

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = ConversationResult()
            return

        response_json = json.loads(response)
        self.result = ConversationResult.from_json(response_json)
