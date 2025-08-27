from __future__ import annotations
import json
from typing import Optional, Dict, Any
from urllib.parse import quote

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
import requests
from ravendb.documents.operations.ai.agents.add_or_update_ai_agent_operation import AiAgentConfigurationResult


class DeleteAiAgentOperation(MaintenanceOperation[AiAgentConfigurationResult]):
    def __init__(self, identifier: str):
        if not identifier or identifier.isspace():
            raise ValueError("identifier cannot be None or empty")
        self._identifier = identifier

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AiAgentConfigurationResult]:
        return DeleteAiAgentCommand(self._identifier)


class DeleteAiAgentCommand(RavenCommand[AiAgentConfigurationResult]):
    def __init__(self, identifier: str):
        super().__init__(AiAgentConfigurationResult)
        self._identifier = identifier

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/ai/agent?agentId={quote(self._identifier)}"

        request = requests.Request("DELETE", url)
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = AiAgentConfigurationResult()
            return

        response_json = json.loads(response)
        self.result = AiAgentConfigurationResult.from_json(response_json)

    def get_raft_unique_request_id(self) -> str:
        # Generate a unique ID for Raft operations
        import uuid

        return str(uuid.uuid4())
