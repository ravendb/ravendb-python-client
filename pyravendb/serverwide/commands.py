import json
import uuid
from typing import Optional

import requests

from pyravendb.http.http import Topology
from pyravendb.http.raven_command import RavenCommand
from pyravendb.http.server_node import ServerNode
from pyravendb.tools.utils import Utils


class GetDatabaseTopologyCommand(RavenCommand):
    def __init__(self, debug_tag: Optional[str] = None, application_identifier: Optional[uuid.UUID] = None):
        super().__init__(Topology)
        self.__debug_tag = debug_tag
        self.__application_identifier = application_identifier

    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> (requests.Request, str):
        url = f"{node.url}/topology?name={node.database}{('&'+self.__debug_tag) if self.__debug_tag else None}"
        if self.__application_identifier:
            url += f"&applicationIdentifier" + str(self.__application_identifier)
        if ".fiddler" in node.url.lower():
            url += f"&localUrl={Utils.escape(node.url,False,False)}"
        return requests.Request(method="GET", url=url)

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            return

        self.result = Utils.initialize_object(json.loads(response), self._result_class)
