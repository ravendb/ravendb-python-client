from __future__ import annotations
import json
from typing import Optional, Type, Dict, Any, TYPE_CHECKING

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
import requests


if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents.ai_agent_configuration import AiAgentConfiguration


class AiAgentConfigurationResult:
    def __init__(self):
        self.identifier: Optional[str] = None
        self.raft_command_index: Optional[int] = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentConfigurationResult:
        result = cls()
        result.identifier = json_dict.get("Identifier")
        result.raft_command_index = json_dict.get("RaftCommandIndex")
        return result


class AddOrUpdateAiAgentOperation(MaintenanceOperation[AiAgentConfigurationResult]):
    def __init__(self, configuration: AiAgentConfiguration, schema_type: Type = None):
        if configuration is None:
            raise ValueError("configuration cannot be None")

        if not configuration.output_schema and not configuration.sample_object and schema_type is None:
            raise ValueError(
                "Please provide a non-empty value for either output_schema or sample_object or schema_type"
            )

        self._configuration = configuration
        self._sample_schema = schema_type() if schema_type else None

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AiAgentConfigurationResult]:
        return AddOrUpdateAiAgentCommand(self._configuration, self._sample_schema, conventions)


class AddOrUpdateAiAgentCommand(RavenCommand[AiAgentConfigurationResult]):
    def __init__(
        self,
        configuration: AiAgentConfiguration,
        sample_schema: Any,
        conventions: DocumentConventions,
    ):
        super().__init__(AiAgentConfigurationResult)
        self._configuration = configuration
        self._sample_schema = sample_schema
        self._conventions = conventions

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/ai/agent"

        # Create a copy of the configuration to avoid modifying the original
        config_to_send = self._configuration

        # Set sample object if not provided but we have a schema type
        if not config_to_send.sample_object and self._sample_schema:
            if hasattr(self._sample_schema, "__dict__"):
                config_to_send.sample_object = json.dumps(self._sample_schema.__dict__)
            else:
                config_to_send.sample_object = json.dumps(self._sample_schema)

        # Convert configuration to JSON
        body_json = config_to_send.to_json()

        body = json.dumps(body_json)

        request = requests.Request("PUT", url)
        request.headers = {"Content-Type": "application/json"}
        request.data = body
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
