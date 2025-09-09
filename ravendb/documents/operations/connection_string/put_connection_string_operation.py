import json
from typing import Dict

import requests

from ravendb import ConnectionString, RavenCommand, ServerNode, RaftCommand
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator


class PutConnectionStringResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    def to_json(self) -> Dict:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PutConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class PutConnectionStringOperation(MaintenanceOperation[PutConnectionStringResult]):
    def __init__(self, connection_string: ConnectionString = None):
        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[PutConnectionStringResult]":
        return self.PutConnectionStringCommand(conventions, self._connection_string)

    class PutConnectionStringCommand(RavenCommand[PutConnectionStringResult], RaftCommand):
        def __init__(
            self, document_conventions: DocumentConventions = None, connection_string: ConnectionString = None
        ):
            super().__init__(PutConnectionStringResult)

            if connection_string is None:
                raise ValueError("Connection string cannot be None")

            self._document_conventions = document_conventions
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/connection-strings"

            request = requests.Request("PUT")
            request.url = url
            request.data = self._connection_string.to_json()

            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = PutConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
