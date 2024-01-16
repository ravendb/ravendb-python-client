from typing import List, TYPE_CHECKING

import requests

from ravendb import RaftCommand, ServerNode
from ravendb.documents.indexes.analysis.definitions import AnalyzerDefinition
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.serverwide.operations.common import VoidServerOperation
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class PutServerWideAnalyzersOperation(VoidServerOperation):
    def __init__(self, *analyzers_to_add: AnalyzerDefinition):
        if not analyzers_to_add:
            raise ValueError("Analyzers must not be empty or None")

        self._analyzers_to_add = analyzers_to_add

    def get_command(
        self, conventions: "DocumentConventions"
    ) -> "PutServerWideAnalyzersOperation.PutServerWideAnalyzersCommand":
        return self.PutServerWideAnalyzersCommand(conventions, list(self._analyzers_to_add))

    class PutServerWideAnalyzersCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, conventions: "DocumentConventions", analyzers_to_add: List[AnalyzerDefinition]):
            if not conventions:
                raise ValueError("Conventions cannot be None")

            if analyzers_to_add is None:
                raise ValueError("Analyzers cannot be None")

            super().__init__()

            self._analyzers_to_add = []
            for i in range(len(analyzers_to_add)):
                analyzer = analyzers_to_add[i]
                if analyzer.name is None:
                    raise ValueError("Name cannot be None")

                self._analyzers_to_add.append(analyzer.to_json())

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/analyzers"

            request = requests.Request(method="PUT", url=url)
            request.data = {"Analyzers": self._analyzers_to_add}

            return request

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DeleteServerWideAnalyzerOperation(VoidServerOperation):
    def __init__(self, analyzer_name: str):
        if analyzer_name is None:
            raise ValueError("Analyzer name cannot be None")

        self._analyzer_name = analyzer_name

    def get_command(
        self, conventions: "DocumentConventions"
    ) -> "DeleteServerWideAnalyzerOperation.DeleteServerWideAnalyzerCommand":
        return self.DeleteServerWideAnalyzerCommand(self._analyzer_name)

    class DeleteServerWideAnalyzerCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, analyzer_name: str):
            if analyzer_name is None:
                raise ValueError("Analyzer name cannot be None")
            super().__init__()

            self._analyzer_name = analyzer_name

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("DELETE", f"{node.url}/admin/analyzers?name={Utils.quote_key(self._analyzer_name)}")

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
