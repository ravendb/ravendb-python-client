from __future__ import annotations
import json
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from urllib.parse import quote

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
import requests

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents.ai_agent_configuration import AiAgentConfiguration


class GetAiAgentsResponse:
    def __init__(self):
        self.ai_agents: List[AiAgentConfiguration] = []

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetAiAgentsResponse:
        from ravendb.documents.operations.ai.agents.ai_agent_configuration import AiAgentConfiguration

        response = cls()
        if json_dict.get("AiAgents"):
            response.ai_agents = [AiAgentConfiguration.from_json(agent_json) for agent_json in json_dict["AiAgents"]]
        return response


class GetAiAgentOperation(MaintenanceOperation[GetAiAgentsResponse]):
    def __init__(self, agent_id: str = None):
        if agent_id is not None and (not agent_id or agent_id.isspace()):
            raise ValueError("agent_id cannot be empty")
        self._agent_id = agent_id

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[GetAiAgentsResponse]:
        return GetAiAgentCommand(self._agent_id)


class GetAiAgentCommand(RavenCommand[GetAiAgentsResponse]):
    def __init__(self, agent_id: str = None):
        super().__init__(GetAiAgentsResponse)
        self._agent_id = agent_id

    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/ai/agent"

        if self._agent_id:
            url += f"?agentId={quote(self._agent_id)}"

        request = requests.Request("GET", url)
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = GetAiAgentsResponse()
            return

        response_json = json.loads(response)
        self.result = GetAiAgentsResponse.from_json(response_json)
