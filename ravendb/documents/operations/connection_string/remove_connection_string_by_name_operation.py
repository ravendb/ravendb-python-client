import json
import urllib
from typing import Dict

import requests

from ravendb import RavenCommand, RaftCommand, ServerNode
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator
from ravendb.serverwide.server_operation_executor import ConnectionStringType


class RemoveConnectionStringByNameResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    def to_json(self) -> Dict:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RemoveConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class RemoveConnectionStringByNameOperation(MaintenanceOperation[RemoveConnectionStringByNameResult]):
    def __init__(self, connection_string_name: str, connection_string_type: ConnectionStringType):
        self._connection_string_name = connection_string_name
        self._connection_string_type = connection_string_type

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[RemoveConnectionStringResult]":
        return self.RemoveConnectionStringCommand(self._connection_string_name, self._connection_string_type)

    class RemoveConnectionStringCommand(RavenCommand[RemoveConnectionStringByNameResult], RaftCommand):
        def __init__(self, connection_string_name: str, connection_string_type: ConnectionStringType):
            super().__init__(RemoveConnectionStringByNameResult)
            self._connection_string_name = connection_string_name
            self._connection_string_type = connection_string_type

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/connection-strings?connectionString={urllib.parse.quote(self._connection_string_name)}&type={self._connection_string_type.value}"

            request = requests.Request("DELETE")
            request.url = url

            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = RemoveConnectionStringByNameResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
