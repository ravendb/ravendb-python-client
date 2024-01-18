from __future__ import annotations

import json
from typing import Optional, List, TYPE_CHECKING

import requests

from ravendb import ServerNode
from ravendb.documents.operations.ongoing_tasks import OngoingTaskType
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.http.topology import RaftCommand
from ravendb.documents.operations.backups.settings import PeriodicBackupConfiguration
from ravendb.serverwide.operations.common import ServerOperation, T, VoidServerOperation
from ravendb.serverwide.operations.ongoing_tasks import IServerWideTask, ServerWideTaskResponse
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator


if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class ServerWideBackupConfiguration(PeriodicBackupConfiguration, IServerWideTask):
    def __init__(self, name_prefix: str = "Server Wide Backup"):
        super(ServerWideBackupConfiguration, self).__init__()
        IServerWideTask.__init__(self)

        self._name_prefix = name_prefix


class PutServerWideBackupConfigurationOperation(
    ServerOperation["PutServerWideBackupConfigurationOperation.PutServerWideBackupConfigurationResponse"]
):
    def __init__(self, configuration: ServerWideBackupConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")

        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.PutServerWideBackupConfigurationCommand(self._configuration)

    class PutServerWideBackupConfigurationCommand(
        RavenCommand["PutServerWideBackupConfigurationOperation.PutServerWideBackupConfigurationResponse"], RaftCommand
    ):
        def __init__(self, configuration: ServerWideBackupConfiguration):
            super().__init__(PutServerWideBackupConfigurationOperation.PutServerWideBackupConfigurationResponse)
            if configuration is None:
                raise ValueError("Configuration cannot be None")
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/backup"
            request = requests.Request(method="PUT", url=url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            self.result = PutServerWideBackupConfigurationOperation.PutServerWideBackupConfigurationResponse.from_json(
                json.loads(response)
            )

    class PutServerWideBackupConfigurationResponse(ServerWideTaskResponse):
        pass


class GetServerWideBackupConfigurationsOperation(ServerOperation[List[ServerWideBackupConfiguration]]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[List[ServerWideBackupConfiguration]]:
        return self.GetServerWideBackupConfigurationsCommand()

    class GetServerWideBackupConfigurationsCommand(RavenCommand[List[ServerWideBackupConfiguration]]):
        def __init__(self):
            super().__init__(ServerWideBackupConfiguration)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/tasks?type=Backup"
            return requests.Request(method="GET", url=url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = [
                ServerWideBackupConfiguration.from_json(result_json) for result_json in json.loads(response)["Results"]
            ]


class GetServerWideBackupConfigurationOperation(ServerOperation[ServerWideBackupConfiguration]):
    def __init__(self, name: str):
        if name is None:
            raise ValueError("Name cannot be None")

        self._name = name

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.GetServerWideBackupConfigurationCommand(self._name)

    class GetServerWideBackupConfigurationCommand(RavenCommand[ServerWideBackupConfiguration]):
        def __init__(self, name: str):
            super().__init__(ServerWideBackupConfiguration)

            if name is None:
                raise ValueError("Name cannot be None")

            self._name = name

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/tasks?type=Backup&name=" + Utils.quote_key(self._name)
            return requests.Request(method="GET", url=url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            results = [ServerWideBackupConfiguration.from_json(bc_json) for bc_json in json.loads(response)["Results"]]
            if not results:
                return

            if len(results) > 1:
                self._throw_invalid_response()

            self.result = results[0]


class DeleteServerWideTaskOperation(VoidServerOperation):
    def __init__(self, name: str, type_: OngoingTaskType):
        if name is None:
            raise ValueError("Name cannot be None")

        self._name = name
        self._type = type_

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.DeleteServerWideTaskCommand(self._name, self._type)

    class DeleteServerWideTaskCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, name: str, type_: OngoingTaskType):
            super().__init__()
            if name is None:
                raise ValueError("Name cannot be None")

            self._name = name
            self._type = type_

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/task?type={self._type.value}&name={Utils.quote_key(self._name)}"
            return requests.Request(method="DELETE", url=url)
