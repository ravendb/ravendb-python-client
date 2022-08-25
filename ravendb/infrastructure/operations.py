import json
from typing import TYPE_CHECKING

import requests

from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.serverwide.operations.common import ServerOperation

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
