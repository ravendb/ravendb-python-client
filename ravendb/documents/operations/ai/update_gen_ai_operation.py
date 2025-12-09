from __future__ import annotations
import json
from typing import Optional, List, TYPE_CHECKING
from urllib.parse import quote

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.documents.starting_point_change_vector import StartingPointChangeVector
from ravendb.documents.operations.etl.etl_operation_results import UpdateEtlOperationResult
import requests

from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration


class UpdateGenAiOperation(MaintenanceOperation[UpdateEtlOperationResult]):
    """
    Operation to update an existing GenAI task in the database.
    """

    def __init__(
        self,
        task_id: int,
        configuration: GenAiConfiguration,
        starting_point: Optional[StartingPointChangeVector] = None,
        reset: bool = False,
    ):
        if configuration is None:
            raise ValueError("configuration cannot be None")

        self._task_id = task_id
        self._configuration = configuration
        self._starting_point = starting_point or StartingPointChangeVector.DO_NOT_CHANGE
        self._reset = reset

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[UpdateEtlOperationResult]:
        transformations_to_reset: Optional[List[str]] = None
        if self._reset:
            transformations_to_reset = [self._configuration.TRANSFORMATION_NAME]

        return UpdateGenAiCommand(
            self._task_id,
            self._configuration,
            self._starting_point,
            transformations_to_reset,
            conventions,
        )


class UpdateGenAiCommand(RavenCommand[UpdateEtlOperationResult]):
    def __init__(
        self,
        task_id: int,
        configuration: GenAiConfiguration,
        starting_point: StartingPointChangeVector,
        transformations_to_reset: Optional[List[str]],
        conventions: DocumentConventions,
    ):
        super().__init__(UpdateEtlOperationResult)
        self._task_id = task_id
        self._configuration = configuration
        self._starting_point = starting_point
        self._transformations_to_reset = transformations_to_reset
        self._conventions = conventions

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/etl?id={self._task_id}"

        if self._transformations_to_reset:
            for transformation in self._transformations_to_reset:
                url += f"&reset={quote(transformation)}"

        url += f"&changeVector={quote(self._starting_point.value)}"

        body_json = self._configuration.to_json()
        body = json.dumps(body_json)

        request = requests.Request("PUT", url)
        request.headers = {"Content-Type": "application/json"}
        request.data = body
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = UpdateEtlOperationResult()
            return

        response_json = json.loads(response)
        self.result = UpdateEtlOperationResult.from_json(response_json)

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()
