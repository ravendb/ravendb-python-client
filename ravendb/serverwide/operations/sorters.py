from typing import List, TYPE_CHECKING

import requests

from ravendb import ServerNode
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.http.topology import RaftCommand
from ravendb.documents.queries.sorting import SorterDefinition
from ravendb.serverwide.operations.common import VoidServerOperation
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class PutServerWideSortersOperation(VoidServerOperation):
    def __init__(self, *sorters_to_add: SorterDefinition):
        if not sorters_to_add:
            raise ValueError("Sorters must not be empty or None")

        self._sorters_to_add = sorters_to_add

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.PutServerWideSortersCommand(conventions, list(self._sorters_to_add))

    class PutServerWideSortersCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, conventions: "DocumentConventions", sorters_to_add: List[SorterDefinition]):
            super().__init__()
            if conventions is None:
                raise ValueError("Conventions cannot be None")
            if sorters_to_add is None:
                raise ValueError("Sorters cannot be None")

            self._sorters_to_add = []

            for sorter in sorters_to_add:
                if sorter.name is None:
                    raise ValueError("Sorter name cannot be None")

                self._sorters_to_add.append(sorter.to_json())

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/sorters"

            return requests.Request(
                "PUT",
                url,
                data={
                    "Sorters": self._sorters_to_add,
                },
            )

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DeleteServerWideSorterOperation(VoidServerOperation):
    def __init__(self, sorter_name: str):
        if sorter_name is None:
            raise ValueError("SorterName cannot be None")

        self._sorter_name = sorter_name

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.DeleteServerWideSorterCommand(self._sorter_name)

    class DeleteServerWideSorterCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, sorter_name: str) -> "VoidRavenCommand":
            super().__init__()
            if sorter_name is None:
                raise ValueError("Sorter name cannot be None")

            self._sorter_name = sorter_name

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("DELETE", f"{node.url}/admin/sorters?name={Utils.quote_key(self._sorter_name)}")

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
