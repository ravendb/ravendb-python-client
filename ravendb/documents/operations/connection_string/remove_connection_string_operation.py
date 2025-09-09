import json
import urllib
from typing import Dict

import requests

from ravendb import RavenCommand, RaftCommand, ConnectionString, ServerNode
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator


class RemoveConnectionStringResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    def to_json(self) -> Dict:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PutConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class RemoveConnectionStringOperation(MaintenanceOperation[RemoveConnectionStringResult]):
    def __init__(self, connection_string: ConnectionString = None):
        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[_T]":
        return self.RemoveConnectionStringCommand(self._connection_string)

    class RemoveConnectionStringCommand(RavenCommand[RemoveConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ConnectionString = None):
            super().__init__(RemoveConnectionStringResult)
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/connection-strings?connectionString={urllib.parse.quote(self._connection_string.name)}&type={self._connection_string.get_type}"

            request = requests.Request("DELETE")
            request.url = url

            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = RemoveConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
