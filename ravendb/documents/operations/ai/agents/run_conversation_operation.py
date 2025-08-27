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


@dataclass
class AiAgentActionRequest:
    """Represents an action request from an AI agent."""

    name: Optional[str] = None
    tool_id: Optional[str] = None
    arguments: Optional[str] = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentActionRequest:
        return cls(name=json_dict.get("Name"), tool_id=json_dict.get("ToolId"), arguments=json_dict.get("Arguments"))

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
    def __init__(self):
        self.conversation_id: Optional[str] = None
        self.change_vector: Optional[str] = None
        self.response: Optional[TSchema] = None
        self.usage: Optional[AiUsage] = None
        self.action_requests: List[AiAgentActionRequest] = []

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConversationResult:
        result = cls()
        result.conversation_id = json_dict.get("ConversationId")
        result.change_vector = json_dict.get("ChangeVector")
        result.response = json_dict.get("Response")

        if json_dict.get("Usage"):
            result.usage = AiUsage.from_json(json_dict["Usage"])

        if json_dict.get("ActionRequests"):
            result.action_requests = [AiAgentActionRequest.from_json(req) for req in json_dict["ActionRequests"]]

        return result


class AiConversationCreationOptions:
    """
    Options for creating AI agent conversations, including parameters and expiration settings.
    """

    def __init__(self):
        self.expiration_in_sec: Optional[int] = None
        self.parameters: Optional[Dict[str, Any]] = None

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

    def __init__(self):
        self.action_responses: Optional[List[AiAgentActionResponse]] = None
        self.user_prompt: Optional[str] = None
        self.creation_options: Optional[AiConversationCreationOptions] = None

    def to_json(self) -> Dict[str, Any]:
        """
        Converts the request body to a JSON-serializable dictionary.

        Returns:
            Dictionary representation of the request body
        """
        # Build dictionary with only non-None values
        result = {}

        if self.action_responses is not None:
            result["ActionResponses"] = [resp.to_json() for resp in self.action_responses]

        if self.user_prompt is not None:
            result["UserPrompt"] = self.user_prompt

        if self.creation_options is not None:
            result["CreationOptions"] = self.creation_options.to_json()

        return result


class RunConversationOperation(MaintenanceOperation[ConversationResult[TSchema]]):
    def __init__(
        self,
        agent_id_or_conversation_id: str,
        user_prompt: str = None,
        parameters_or_action_responses: Any = None,
        change_vector: str = None,
    ):
        # Reset all fields first
        self._conversation_id = None
        self._agent_id = None
        self._user_prompt = None
        self._parameters = None
        self._action_responses = None
        self._change_vector = None

        if change_vector is not None or isinstance(parameters_or_action_responses, list):
            # Constructor overload: conversationId-based
            if not agent_id_or_conversation_id or agent_id_or_conversation_id.isspace():
                raise ValueError("conversation_id cannot be None or empty")

            self._conversation_id = agent_id_or_conversation_id
            self._user_prompt = user_prompt
            self._action_responses = (
                parameters_or_action_responses if isinstance(parameters_or_action_responses, list) else None
            )
            self._change_vector = change_vector
        else:
            # Constructor overload: agentId-based
            if not agent_id_or_conversation_id or agent_id_or_conversation_id.isspace():
                raise ValueError("agent_id cannot be None or empty")
            if user_prompt is not None and (not user_prompt or user_prompt.isspace()):
                raise ValueError("user_prompt cannot be empty")

            self._agent_id = agent_id_or_conversation_id
            self._user_prompt = user_prompt
            self._parameters = (
                parameters_or_action_responses if isinstance(parameters_or_action_responses, dict) else None
            )

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[ConversationResult[TSchema]]:
        return RunConversationCommand(
            conversation_id=self._conversation_id,
            agent_id=self._agent_id,
            prompt=self._user_prompt,
            parameters=self._parameters,
            action_responses=self._action_responses,
            change_vector=self._change_vector,
            conventions=conventions,
        )


class RunConversationCommand(RavenCommand[ConversationResult[TSchema]]):
    def __init__(
        self,
        conversation_id: str = None,
        agent_id: str = None,
        prompt: str = None,
        parameters: Dict[str, Any] = None,
        action_responses: List[AiAgentActionResponse] = None,
        change_vector: str = None,
        conventions: DocumentConventions = None,
    ):
        super().__init__(ConversationResult)
        self._conversation_id = conversation_id
        self._agent_id = agent_id
        self._prompt = prompt
        self._parameters = parameters
        self._action_responses = action_responses
        self._change_vector = change_vector
        self._conventions = conventions

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/ai/agent"

        # Add query parameters - server requires BOTH agentId and conversationId
        query_params = []
        from urllib.parse import quote

        if self._conversation_id and self._agent_id:
            # Continuing conversation - we have both
            query_params.append(f"conversationId={quote(self._conversation_id)}")
            query_params.append(f"agentId={quote(self._agent_id)}")
        elif self._conversation_id:
            # We only have conversation ID - this might fail, but let's try
            query_params.append(f"conversationId={quote(self._conversation_id)}")
        elif self._agent_id:
            # New conversation - use conversation prefix as per RavenDB documentation
            # The server will generate the full conversation ID from the prefix
            conversation_prefix = "conversations/"
            query_params.append(f"conversationId={quote(conversation_prefix)}")
            query_params.append(f"agentId={quote(self._agent_id)}")

        if query_params:
            url += "?" + "&".join(query_params)

        # Build request body with correct structure to match .NET client
        request_body = ConversationRequestBody()
        request_body.action_responses = self._action_responses
        request_body.user_prompt = self._prompt

        # Always include CreationOptions to match .NET client structure
        creation_options = AiConversationCreationOptions()
        creation_options.parameters = self._parameters
        request_body.creation_options = creation_options

        body = json.dumps(request_body.to_json())

        # Create request
        request = requests.Request("POST", url)
        request.headers = {"Content-Type": "application/json"}

        if self._change_vector:
            request.headers["If-Match"] = self._change_vector

        request.data = body
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = ConversationResult()
            return

        response_json = json.loads(response)
        self.result = ConversationResult.from_json(response_json)

    def get_raft_unique_request_id(self) -> str:
        # Generate a unique ID for Raft operations
        import uuid

        return str(uuid.uuid4())
