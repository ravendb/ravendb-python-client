from __future__ import annotations

import json
from typing import Any, Dict, TYPE_CHECKING, Optional

import requests

from ravendb import DocumentsCompressionConfiguration, RaftCommand, ServerNode
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator
from ravendb.http.raven_command import RavenCommand

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class DocumentCompressionConfigurationResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> DocumentCompressionConfigurationResult:
        return cls(json_dict["RaftCommandIndex"])


class UpdateDocumentsCompressionConfigurationOperation(MaintenanceOperation[DocumentCompressionConfigurationResult]):
    def __init__(self, configuration: DocumentsCompressionConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")
        self._documents_compression_configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[DocumentCompressionConfigurationResult]":
        return self.UpdateDocumentCompressionConfigurationCommand(self._documents_compression_configuration)

    class UpdateDocumentCompressionConfigurationCommand(
        RavenCommand[DocumentCompressionConfigurationResult], RaftCommand
    ):
        def __init__(self, configuration: DocumentsCompressionConfiguration):
            super().__init__(DocumentCompressionConfigurationResult)

            if configuration is None:
                raise ValueError("Configuration cannot be None")

            self._documents_compression_configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/documents-compression/config"

            request = requests.Request("POST", url)

            request.data = self._documents_compression_configuration.to_json()

            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = DocumentCompressionConfigurationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
