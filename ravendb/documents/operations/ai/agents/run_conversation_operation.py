from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, TypeVar, Generic

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
import requests

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
class AiUsage:
    """Represents AI token usage statistics."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cached_tokens: int = 0

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiUsage:
        return cls(
            prompt_tokens=json_dict.get("PromptTokens", 0),
            completion_tokens=json_dict.get("CompletionTokens", 0),
            total_tokens=json_dict.get("TotalTokens", 0),
            cached_tokens=json_dict.get("CachedTokens", 0),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "PromptTokens": self.prompt_tokens,
            "CompletionTokens": self.completion_tokens,
            "TotalTokens": self.total_tokens,
            "CachedTokens": self.cached_tokens,
        }


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
    action responses, and creation options.
    """

    def __init__(
        self,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        user_prompt: Optional[List[str]] = None,
        creation_options: Optional[AiConversationCreationOptions] = None,
    ):
        self.action_responses: Optional[List[AiAgentActionResponse]] = action_responses
        self.user_prompt: Optional[List[str]] = user_prompt  # List of prompt parts
        self.creation_options: Optional[AiConversationCreationOptions] = creation_options

    def to_json(self) -> Dict[str, Any]:
        """
        Converts the request body to a JSON-serializable dictionary.

        Returns:
            Dictionary representation of the request body
        """
        result = {}

        # ActionResponses: null if None, otherwise array
        result["ActionResponses"] = (
            None if self.action_responses is None else [resp.to_json() for resp in self.action_responses]
        )

        # UserPrompt: null if None, otherwise array (even if empty)
        result["UserPrompt"] = self.user_prompt

        # CreationOptions: always present (create empty if None, matching C# behavior)
        result["CreationOptions"] = (self.creation_options or AiConversationCreationOptions()).to_json()

        return result


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
        prompt_parts: Optional[List[str]] = None,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        options: Optional[AiConversationCreationOptions] = None,
        change_vector: Optional[str] = None,
    ):
        """
        Initialize a RunConversationOperation.

        Args:
            agent_id: The ID of the AI agent (required)
            conversation_id: The ID of the conversation (required)
            prompt_parts: List of prompt strings to send to the agent
            action_responses: List of action responses from previous turn
            options: Creation options including parameters and expiration
            change_vector: Change vector for optimistic concurrency
        """
        if not agent_id or (isinstance(agent_id, str) and agent_id.isspace()):
            raise ValueError("agent_id cannot be None or empty")
        if not conversation_id or (isinstance(conversation_id, str) and conversation_id.isspace()):
            raise ValueError("conversation_id cannot be None or empty")

        self._agent_id = agent_id
        self._conversation_id = conversation_id
        self._prompt_parts = prompt_parts
        self._action_responses = action_responses
        self._options = options
        self._change_vector = change_vector

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[ConversationResult[TSchema]]:
        return RunConversationCommand(
            agent_id=self._agent_id,
            conversation_id=self._conversation_id,
            prompt_parts=self._prompt_parts,
            action_responses=self._action_responses,
            options=self._options,
            change_vector=self._change_vector,
            conventions=conventions,
        )


class RunConversationCommand(RavenCommand[ConversationResult[TSchema]]):
    def __init__(
        self,
        agent_id: str,
        conversation_id: str,
        prompt_parts: Optional[List[str]] = None,
        action_responses: Optional[List[AiAgentActionResponse]] = None,
        options: Optional[AiConversationCreationOptions] = None,
        change_vector: Optional[str] = None,
        conventions: Optional[DocumentConventions] = None,
    ):
        from ravendb.util.util import RaftIdGenerator

        super().__init__(ConversationResult)
        self._agent_id = agent_id
        self._conversation_id = conversation_id
        self._prompt_parts = prompt_parts
        self._action_responses = action_responses
        self._options = options
        self._change_vector = change_vector
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

        # Build request body with correct structure to match .NET client
        request_body = ConversationRequestBody(
            action_responses=self._action_responses,
            user_prompt="".join(self._prompt_parts),
            creation_options=self._options,
        )

        body = json.dumps(request_body.to_json())

        # Create request
        request = requests.Request("POST", url)
        request.headers = {"Content-Type": "application/json"}

        request.data = body
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = ConversationResult()  # Uses default constructor with all None values
            return

        response_json = json.loads(response)
        self.result = ConversationResult.from_json(response_json)
