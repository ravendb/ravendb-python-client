from __future__ import annotations
import json
from typing import Optional, TYPE_CHECKING
from urllib.parse import quote

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.documents.starting_point_change_vector import StartingPointChangeVector
from ravendb.documents.operations.ai.ai_task_operation_results import AddGenAiOperationResult
import requests

from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration


class AddGenAiOperation(MaintenanceOperation[AddGenAiOperationResult]):
    """
    Operation to add a new GenAI task to the database.
    """

    def __init__(
        self,
        configuration: GenAiConfiguration,
        starting_point: Optional[StartingPointChangeVector] = StartingPointChangeVector.LAST_DOCUMENT,
    ):
        if configuration is None:
            raise ValueError("configuration cannot be None")

        self._configuration = configuration
        self._starting_point = starting_point

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AddGenAiOperationResult]:
        return AddGenAiCommand(self._configuration, self._starting_point, conventions)


class AddGenAiCommand(RavenCommand[AddGenAiOperationResult]):
    def __init__(
        self,
        configuration: GenAiConfiguration,
        starting_point: StartingPointChangeVector,
        conventions: DocumentConventions,
    ):
        super().__init__(AddGenAiOperationResult)
        self._configuration = configuration
        self._starting_point = starting_point
        self._conventions = conventions

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/etl?changeVector={quote(self._starting_point.value)}"

        body_json = self._configuration.to_json()
        body = json.dumps(body_json)

        request = requests.Request("PUT", url)
        request.headers = {"Content-Type": "application/json"}
        request.data = body
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = AddGenAiOperationResult()
            return

        response_json = json.loads(response)
        self.result = AddGenAiOperationResult.from_json(response_json)

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()
