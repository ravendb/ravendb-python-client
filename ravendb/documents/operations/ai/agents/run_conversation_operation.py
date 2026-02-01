from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, TypeVar, Generic, Callable

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from ravendb.http.server_node import ServerNode
import requests
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.documents.ai.content_part import ContentPart

TSchema = TypeVar("TSchema")


class AiAgentActionRequest:
    """Represents an action request from an AI agent."""

    def __init__(self, name: str = None, tool_id: str = None, arguments: str = None):
        self.name = name
        self.tool_id = tool_id
        self.arguments = arguments

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentActionRequest:
        return cls(
            name=json_dict.get("Name"),
            tool_id=json_dict.get("ToolId"),
            arguments=json_dict.get("Arguments"),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "ToolId": self.tool_id,
            "Arguments": self.arguments,
        }


@dataclass
class AiAgentActionResponse:
    """Represents a response to an AI agent action request."""

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
    """
    Represents an artificial action (tool call) and response to inject into the model's conversation context.
    This allows programmatically prompting the agent by making it "believe" it executed a tool.
    """

    tool_id: Optional[str] = None
    content: Optional[str] = None

    def validate(self) -> None:
        """Validates that tool_id and content are not empty."""
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
    """Represents AI token usage statistics."""

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
        """
        Calculate the usage difference between current and previous usage.

        Args:
            current: The current usage statistics
            previous: The previous usage statistics

        Returns:
            An AiUsage object representing the difference
        """
        previous_total_without_reasoning = (
            previous.completion_tokens - previous.reasoning_tokens + previous.prompt_tokens
        )
        return AiUsage(
            # in case the model gives us crappy results and current.prompt_tokens - previous_total_without_reasoning < 0
            prompt_tokens=max(current.prompt_tokens - previous_total_without_reasoning, 0),
            # in case the model gives us crappy results and current.total_tokens - previous_total_without_reasoning < 0
            total_tokens=max(current.total_tokens - previous_total_without_reasoning, 0),
            # we don't want to subtract cached tokens, as they are only for the last response
            cached_tokens=current.cached_tokens,
            # we don't want to subtract completion tokens, as they are only for the last response
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
        usage = None
        if json_dict.get("Usage"):
            usage = AiUsage.from_json(json_dict["Usage"])

        action_requests = None
        if json_dict.get("ActionRequests"):
            action_requests = [AiAgentActionRequest.from_json(req) for req in json_dict["ActionRequests"]]

        return cls(
            conversation_id=json_dict.get("ConversationId"),
            change_vector=json_dict.get("ChangeVector"),
            response=json_dict.get("Response"),
            usage=usage,
            action_requests=action_requests,
        )


class AiConversationCreationOptions:
    """
    Options for creating AI agent conversations, including parameters and expiration settings.
    """

    def __init__(self, parameters: Optional[Dict[str, Any]] = None, expiration_in_sec: Optional[int] = None):
        self.expiration_in_sec: Optional[int] = expiration_in_sec
        self.parameters: Optional[Dict[str, Any]] = parameters

    def add_parameter(self, name: str, value: Any) -> AiConversationCreationOptions:
        """
        Adds a parameter to the conversation creation options.

        Args:
            name: The parameter name
            value: The parameter value

        Returns:
            Self for method chaining
        """
        if self.parameters is None:
            self.parameters = {}
        self.parameters[name] = value
        return self

    def to_json(self) -> Dict[str, Any]:
        """
        Converts the creation options to a JSON-serializable dictionary.

        Returns:
            Dictionary representation of the creation options
        """
        return {"ExpirationInSec": self.expiration_in_sec, "Parameters": self.parameters}


class ConversationRequestBody:
    """
    Request body for AI agent conversation operations, containing user prompts,
    action responses, artificial actions, and creation options.
    """

    def __init__(
        self,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = None,
        user_prompt: Optional[List[ContentPart]] = None,
        creation_options: Optional[AiConversationCreationOptions] = None,
    ):
        self.action_responses: Optional[List[AiAgentActionResponse]] = action_responses
        self.artificial_actions: Optional[List[AiAgentArtificialActionResponse]] = artificial_actions
        self.user_prompt: Optional[List[ContentPart]] = user_prompt  # List of ContentPart objects
        self.creation_options: Optional[AiConversationCreationOptions] = creation_options

    def to_json(self) -> Dict[str, Any]:
        """
        Converts the request body to a JSON-serializable dictionary.

        Returns:
            Dictionary representation of the request body
        """
        return {
            "ActionResponses": (
                None if self.action_responses is None else [resp.to_json() for resp in self.action_responses]
            ),
            "ArtificialActions": (
                None if self.artificial_actions is None else [resp.to_json() for resp in self.artificial_actions]
            ),
            "CreationOptions": (self.creation_options or AiConversationCreationOptions()).to_json(),
            "UserPrompt": None if self.user_prompt is None else [part.to_json() for part in self.user_prompt],
        }


class RunConversationOperation(MaintenanceOperation[ConversationResult[TSchema]]):
    """
    Operation for running AI agent conversations.

    Both agent_id and conversation_id are required. The agent_id identifies which AI agent to use,
    while conversation_id tracks the conversation state across multiple turns.
    """

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
    ):
        """
        Initialize a RunConversationOperation.

        Args:
            agent_id: The ID of the AI agent (required)
            conversation_id: The ID of the conversation (required)
            prompt_parts: List of ContentPart objects to send to the agent
            action_responses: List of action responses from previous turn
            artificial_actions: List of artificial actions to inject into conversation context
            options: Creation options including parameters and expiration
            change_vector: Change vector for optimistic concurrency
            stream_property_path: Optional response property name to stream
            streamed_chunks_callback: Optional callback invoked per streamed chunk
        """
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
    ):
        from ravendb.util.util import RaftIdGenerator

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
        self._raft_id = RaftIdGenerator.dont_care_id()

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        from urllib.parse import quote
        from ravendb.util.util import RaftIdGenerator

        # Build URL with required query parameters
        url = (
            f"{node.url}/databases/{node.database}/ai/agent"
            f"?conversationId={quote(self._conversation_id)}"
            f"&agentId={quote(self._agent_id)}"
        )

        # Check if this is a Raft operation (conversation_id ends with '|')
        if self._conversation_id.endswith("|"):
            self._raft_id = RaftIdGenerator.new_id()

        # Add changeVector to URL if provided (for optimistic concurrency)
        if self._change_vector:
            url += f"&changeVector={quote(self._change_vector)}"

        # Add streaming flags if requested
        if self._stream_property_path:
            url += f"&streaming=true&streamPropertyPath={quote(self._stream_property_path)}"

        # Build request body with correct structure to match .NET client
        request_body = ConversationRequestBody(
            action_responses=self._action_responses,
            artificial_actions=self._artificial_actions,
            user_prompt=self._prompt_parts,
            creation_options=self._options,
        )

        body = json.dumps(request_body.to_json())

        # Create request
        request = requests.Request("POST", url)
        request.headers = {"Content-Type": "application/json"}

        request.data = body
        return request

    # todo: this should be handled by writing custom set_response_raw method, and ravendcommandresponsetype set to RAW
    def process_response(self, cache, response: requests.Response, url) -> ResponseDisposeHandling:
        # If not streaming, delegate to the default handler
        if not self._stream_property_path:
            return super().process_response(cache, response, url)

        try:
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith("{"):
                    response_json = json.loads(line)
                    self.result = ConversationResult.from_json(response_json)
                    return ResponseDisposeHandling.AUTOMATIC
                # Non-final lines are JSON-encoded strings (e.g. "\\\"chunk\\\"")
                try:
                    chunk = json.loads(line)
                except Exception:
                    chunk = line
                if self._streamed_chunks_callback:
                    self._streamed_chunks_callback(chunk)
            # No final JSON object received; set empty result
            self.result = ConversationResult()
            return ResponseDisposeHandling.AUTOMATIC
        finally:
            # Response will be closed by RequestExecutor when AUTOMATIC is returned
            pass

    def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
        if self._stream_property_path:
            from ravendb.util.request_utils import RequestUtils

            prepared_request = session.prepare_request(request)
            RequestUtils.remove_zstd_encoding(prepared_request)
            return session.send(prepared_request, cert=session.cert, stream=True)
        return super().send(session, request)

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = ConversationResult()  # Uses default constructor with all None values
            return

        response_json = json.loads(response)
        self.result = ConversationResult.from_json(response_json)
