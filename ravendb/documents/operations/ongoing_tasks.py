import json
from enum import Enum
from typing import Optional, TYPE_CHECKING, Union

import requests

from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.serverwide.operations.common import ModifyOngoingTaskResult
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator
from ravendb.http.raven_command import RavenCommand

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class OngoingTaskType(Enum):
    REPLICATION = "Replication"
    RAVEN_ETL = "RavenEtl"
    SQL_ETL = "SqlEtl"
    OLAP_ETL = "OlapEtl"
    BACKUP = "Backup"
    SUBSCRIPTION = "Subscription"
    PULL_REPLICATION_AS_HUB = "PullReplicationAsHub"
    PULL_REPLICATION_AS_SINK = "PullReplicationAsSink"


class ToggleOngoingTaskStateOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    def __init__(
        self, task_name_or_id: Union[int, str], type_of_task: Optional[OngoingTaskType], disable: Optional[bool]
    ):
        if isinstance(task_name_or_id, str):
            task_name = task_name_or_id
            if not task_name or task_name.isspace():
                raise RuntimeError("Task name id must have a non empty value")

            self._task_name = task_name
            self._task_id = 0
        elif isinstance(task_name_or_id, int):
            task_id = task_name_or_id
            self._task_name = None
            self._task_id = task_id
        else:
            raise TypeError("Unexpected type of the 'task_name_or_id'.")

        self._type_of_task = type_of_task
        self._disable = disable

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return ToggleOngoingTaskStateOperation._ToggleTaskStateCommand(
            self._task_id, self._task_name, self._type_of_task, self._disable
        )

    class _ToggleTaskStateCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, task_id: int, task_name: str, type_of_task: OngoingTaskType, disable: bool):
            super(ToggleOngoingTaskStateOperation._ToggleTaskStateCommand, self).__init__(ModifyOngoingTaskResult)
            self._task_id = task_id
            self._task_name = task_name
            self._type_of_task = type_of_task
            self._disable = disable

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/admin/tasks/state"
                f"?key={self._task_id}"
                f"&type={self._type_of_task.value}"
                f"&disable={'true' if self._disable else 'false'}"
            )

            if self._task_name is not None:
                url += f"&taskName={Utils.quote_key(self._task_name)}"

            return requests.Request("POST", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is not None:
                self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
