import json
from typing import Optional

import requests

from ravendb.http.server_node import ServerNode
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand


class GetNextOperationIdCommand(RavenCommand[int]):
    def __init__(self):
        super(GetNextOperationIdCommand, self).__init__(int)
        self._node_tag = 0

    @property
    def node_tag(self):
        return self._node_tag

    def is_read_request(self) -> bool:
        return False  # disable caching

    def create_request(self, node: ServerNode) -> requests.Request:
        return requests.Request("GET", f"{node.url}/databases/{node.database}/operations/next-operation-id")

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        json_node = json.loads(response)
        self.result = json_node.get("Id", None)
        self._node_tag = json_node.get("NodeTag", None)


class KillOperationCommand(VoidRavenCommand):
    def __init__(self, operation_id: int, node_tag: Optional[str] = None):
        super(KillOperationCommand, self).__init__()
        self._id = operation_id
        self._selected_node_tag = node_tag

    def create_request(self, node: ServerNode) -> requests.Request:
        return requests.Request("POST", f"{node.url}/databases/{node.database}/operations/kill?id={self._id}")
