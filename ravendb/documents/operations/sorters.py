from typing import List, TYPE_CHECKING

import requests

from ravendb import SorterDefinition, RaftCommand, ServerNode
from ravendb.documents.operations.definitions import VoidMaintenanceOperation
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class PutSortersOperation(VoidMaintenanceOperation):
    def __init__(self, *sorters_to_add: SorterDefinition):
        if not sorters_to_add:
            raise ValueError("Sorters must not be empty or None")

        self._sorters_to_add = sorters_to_add

    def get_command(self, conventions: "DocumentConventions") -> "PutSortersOperation.PutSortersCommand":
        return self.PutSortersCommand(conventions, list(self._sorters_to_add))

    class PutSortersCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, conventions: "DocumentConventions", sorters_to_add: List[SorterDefinition]):
            if conventions is None:
                raise ValueError("Conventions cannot be None")

            if sorters_to_add is None:
                raise ValueError("Sorters cannot be None")

            if any([sorter is None for sorter in sorters_to_add]):
                raise ValueError("Sorter cannot be None")

            super().__init__()

            self._sorters_to_add = sorters_to_add

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/sorters"

            request = requests.Request("PUT", url)
            request.data = {"Sorters": [sorter.to_json() for sorter in self._sorters_to_add]}

            return request

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DeleteSorterOperation(VoidMaintenanceOperation):
    def __init__(self, sorter_name: str = None):
        if sorter_name is None:
            raise ValueError("SorterName cannot be None")

        self._sorter_name = sorter_name

    def get_command(self, conventions: "DocumentConventions") -> "DeleteSorterOperation.DeleteSorterCommand":
        return self.DeleteSorterCommand(self._sorter_name)

    class DeleteSorterCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, sorter_name: str = None):
            if sorter_name is None:
                raise ValueError("SorterName cannot be None")

            super().__init__()
            self._sorter_name = sorter_name

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/sorters?name={Utils.quote_key(self._sorter_name)}"
            return requests.Request("DELETE", url)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
