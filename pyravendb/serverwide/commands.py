import json
import uuid
from typing import Optional

import requests

from pyravendb.http import Topology, ClusterTopologyResponse
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

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/topology?name={node.database}{('&'+self.__debug_tag) if self.__debug_tag else ''}"
        if self.__application_identifier:
            url += f"&applicationIdentifier=" + str(self.__application_identifier)
        if ".fiddler" in node.url.lower():
            url += f"&localUrl={Utils.escape(node.url,False,False)}"
        return requests.Request(method="GET", url=url)

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            return

        # todo: that's pretty bad way to do that, replace with initialization function that take nested object types
        self.result: Topology = Utils.initialize_object(json.loads(response), self._result_class, True)
        node_list = []
        for node in self.result.nodes:
            node_list.append(Utils.initialize_object(node, ServerNode, True))
        self.result.nodes = node_list


class GetClusterTopologyCommand(RavenCommand[ClusterTopologyResponse]):
    def __init__(self, debug_tag: str = None):
        super(GetClusterTopologyCommand, self).__init__(ClusterTopologyResponse)
        self.__debug_tag = debug_tag

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/cluster/topology"
        if self.__debug_tag is not None:
            url += f"?{self.__debug_tag}"

        return requests.Request("GET", url)

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            super()._throw_invalid_response()

        self.result: ClusterTopologyResponse = ClusterTopologyResponse.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return True
