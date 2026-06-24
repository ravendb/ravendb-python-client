from __future__ import annotations

import http
import json
from typing import List, Optional, TYPE_CHECKING

import requests

from ravendb.documents.operations.definitions import MaintenanceOperation, VoidMaintenanceOperation
from ravendb.documents.operations.replication.definitions import (
    DetailedReplicationHubAccess,
    ExternalReplication,
    PullReplicationAsSink,
    PullReplicationDefinition,
    PullReplicationDefinitionAndCurrentConnections,
    ReplicationHubAccess,
    ReplicationHubAccessResult,
)
from ravendb.exceptions.raven_exceptions import ReplicationHubNotFoundException
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType, VoidRavenCommand
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ModifyOngoingTaskResult
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.http.server_node import ServerNode


class PutPullReplicationAsHubOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    """Creates or updates a pull-replication hub task on the (source) database."""

    def __init__(
        self,
        pull_replication_definition: Optional[PullReplicationDefinition] = None,
        name: str = None,
    ):
        if pull_replication_definition is None:
            if not name or name.isspace():
                raise ValueError("Name cannot be None or whitespace")
            pull_replication_definition = PullReplicationDefinition(name)

        if not pull_replication_definition.name or pull_replication_definition.name.isspace():
            raise ValueError("PullReplicationDefinition name cannot be None or whitespace")

        self._pull_replication_definition = pull_replication_definition

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return self.PutPullReplicationAsHubCommand(self._pull_replication_definition)

    class PutPullReplicationAsHubCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, pull_replication_definition: PullReplicationDefinition):
            super().__init__(ModifyOngoingTaskResult)
            self._pull_replication_definition = pull_replication_definition

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/tasks/pull-replication/hub"
            request = requests.Request("PUT", url)
            request.data = self._pull_replication_definition.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class UpdatePullReplicationAsSinkOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    """Creates or updates a pull-replication sink task on the (destination) database."""

    def __init__(self, pull_replication: PullReplicationAsSink, use_server_certificate: bool = False):
        if pull_replication is None:
            raise ValueError("PullReplicationAsSink cannot be None")
        self._pull_replication = pull_replication
        self._use_server_certificate = use_server_certificate

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return self.UpdatePullReplicationAsSinkCommand(self._pull_replication, self._use_server_certificate)

    class UpdatePullReplicationAsSinkCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, pull_replication: PullReplicationAsSink, use_server_certificate: bool):
            super().__init__(ModifyOngoingTaskResult)
            self._pull_replication = pull_replication
            self._use_server_certificate = use_server_certificate

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/tasks/sink-pull-replication"
            sink_json = self._pull_replication.to_json()
            if self._use_server_certificate:
                sink_json["CertificateWithPrivateKey"] = None
            request = requests.Request("POST", url)
            request.data = {"PullReplicationAsSink": sink_json}
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class UpdateExternalReplicationOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    """Creates or updates an external-replication task on the database."""

    def __init__(self, new_watcher: ExternalReplication):
        if new_watcher is None:
            raise ValueError("ExternalReplication cannot be None")
        self._new_watcher = new_watcher

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return self.UpdateExternalReplicationCommand(self._new_watcher)

    class UpdateExternalReplicationCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, new_watcher: ExternalReplication):
            super().__init__(ModifyOngoingTaskResult)
            self._new_watcher = new_watcher

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/tasks/external-replication"
            request = requests.Request("POST", url)
            request.data = {"Watcher": self._new_watcher.to_json()}
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class RegisterReplicationHubAccessOperation(MaintenanceOperation[None]):
    """Registers a sink's certificate access against an existing replication hub."""

    def __init__(self, hub_name: str, access: ReplicationHubAccess):
        if not hub_name or hub_name.isspace():
            raise ValueError("Hub name cannot be None or whitespace")
        if access is None:
            raise ValueError("Access cannot be None")
        self._hub_name = hub_name
        self._access = access

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[None]:
        return self.RegisterReplicationHubAccessCommand(self._hub_name, self._access)

    class RegisterReplicationHubAccessCommand(RavenCommand[None], RaftCommand):
        def __init__(self, hub_name: str, access: ReplicationHubAccess):
            super().__init__()
            self._hub_name = hub_name
            self._access = access
            self._response_type = RavenCommandResponseType.RAW

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/admin/tasks/pull-replication/hub/access"
                f"?name={Utils.quote_key(self._hub_name)}"
            )
            request = requests.Request("PUT", url)
            request.data = self._access.to_json()
            return request

        def set_response_raw(self, response: requests.Response, stream: bytes) -> None:
            if response.status_code == http.HTTPStatus.NOT_FOUND:
                raise ReplicationHubNotFoundException(
                    f"The replication hub {self._hub_name} was not found on the database. "
                    f"Did you forget to define it first?"
                )

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class UnregisterReplicationHubAccessOperation(VoidMaintenanceOperation):
    """Revokes a previously-registered sink certificate (by thumbprint) from a replication hub."""

    def __init__(self, hub_name: str, thumbprint: str):
        if not hub_name or hub_name.isspace():
            raise ValueError("Hub name cannot be None or whitespace")
        if not thumbprint or thumbprint.isspace():
            raise ValueError("Thumbprint cannot be None or whitespace")
        self._hub_name = hub_name
        self._thumbprint = thumbprint

    def get_command(self, conventions: "DocumentConventions") -> VoidRavenCommand:
        return self.UnregisterReplicationHubAccessCommand(self._hub_name, self._thumbprint)

    class UnregisterReplicationHubAccessCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, hub_name: str, thumbprint: str):
            super().__init__()
            self._hub_name = hub_name
            self._thumbprint = thumbprint

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/admin/tasks/pull-replication/hub/access"
                f"?name={Utils.quote_key(self._hub_name)}&thumbprint={Utils.quote_key(self._thumbprint)}"
            )
            return requests.Request("DELETE", url)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetReplicationHubAccessOperation(MaintenanceOperation[List[DetailedReplicationHubAccess]]):
    """Lists the sinks (certificates) registered to access a replication hub."""

    def __init__(self, hub_name: str, start: int = 0, page_size: int = 25):
        if not hub_name or hub_name.isspace():
            raise ValueError("Hub name cannot be None or whitespace")
        self._hub_name = hub_name
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[List[DetailedReplicationHubAccess]]:
        return self.GetReplicationHubAccessCommand(self._hub_name, self._start, self._page_size)

    class GetReplicationHubAccessCommand(RavenCommand[List[DetailedReplicationHubAccess]]):
        def __init__(self, hub_name: str, start: int, page_size: int):
            super().__init__(DetailedReplicationHubAccess)
            self._hub_name = hub_name
            self._start = start
            self._page_size = page_size

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/admin/tasks/pull-replication/hub/access"
                f"?name={Utils.quote_key(self._hub_name)}&start={self._start}&pageSize={self._page_size}"
            )
            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self.result = []
                return
            self.result = ReplicationHubAccessResult.from_json(json.loads(response)).results


class GetPullReplicationTasksInfoOperation(MaintenanceOperation[PullReplicationDefinitionAndCurrentConnections]):
    """Retrieves a pull-replication hub definition together with its current sink connections."""

    def __init__(self, task_id: int):
        self._task_id = task_id

    def get_command(
        self, conventions: "DocumentConventions"
    ) -> RavenCommand[PullReplicationDefinitionAndCurrentConnections]:
        return self.GetPullReplicationTasksInfoCommand(self._task_id)

    class GetPullReplicationTasksInfoCommand(RavenCommand[PullReplicationDefinitionAndCurrentConnections]):
        def __init__(self, task_id: int):
            super().__init__(PullReplicationDefinitionAndCurrentConnections)
            self._task_id = task_id

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = f"{node.url}/databases/{node.database}/tasks/pull-replication/hub?key={self._task_id}"
            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return
            self.result = PullReplicationDefinitionAndCurrentConnections.from_json(json.loads(response))
