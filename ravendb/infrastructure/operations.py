import json
from typing import TYPE_CHECKING, Set

import requests

from ravendb.http.topology import RaftCommand
from ravendb.documents.operations.definitions import VoidMaintenanceOperation
from ravendb.documents.smuggler.common import DatabaseItemType
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.serverwide.operations.common import ServerOperation
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class AdminJsConsoleOperation(ServerOperation[dict]):
    def __init__(self, script: str):
        self.__script = script

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[dict]:
        return AdminJsConsoleOperation.__AdminJsConsoleCommand(self.__script)

    class __AdminJsConsoleCommand(RavenCommand[dict]):
        def __init__(self, script: str):
            super().__init__(dict)
            self.__script = script

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request("POST", f"{node.url}/admin/console?serverScript=true")
            request.data = {"Script": self.__script}
            return request

        def is_read_request(self) -> bool:
            return False

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = json.loads(response)


class CreateSampleDataOperation(VoidMaintenanceOperation):
    def __init__(self, operate_on_types=None):
        if operate_on_types is None:
            operate_on_types = {DatabaseItemType.DOCUMENTS}
        self._operate_on_types = operate_on_types

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.CreateSampleDataCommand(self._operate_on_types)

    class CreateSampleDataCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, operate_on_types: Set[DatabaseItemType]):
            super().__init__()
            self._operate_on_types = operate_on_types

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/studio/sample-data"
            url += f"?{'&'.join([f'operateOnTypes={x.value}' for x in self._operate_on_types])}"
            return requests.Request("POST", url)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
