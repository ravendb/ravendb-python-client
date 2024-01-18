from __future__ import annotations
import json
from datetime import timedelta
from enum import Enum
from typing import Optional, Any, Dict, TYPE_CHECKING

import requests

from ravendb import ServerNode
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.serverwide.operations.common import ServerOperation, T, VoidServerOperation
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class LogMode(Enum):
    NONE = "None"
    OPERATIONS = "Operations"
    INFORMATION = "Information"


class GetLogsConfigurationResult:
    def __init__(
        self,
        current_mode: LogMode = None,
        mode: LogMode = None,
        path: str = None,
        use_utc_time: bool = None,
        retention_time: timedelta = None,
        retention_size: int = None,
        compress: bool = None,
    ):
        self.current_mode = current_mode
        self.mode = mode
        self.path = path
        self.use_utc_time = use_utc_time
        self.retention_time = retention_time
        self.retention_size = retention_size
        self.compress = compress

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetLogsConfigurationResult:
        return cls(
            LogMode(json_dict["CurrentMode"]),
            LogMode(json_dict["Mode"]),
            json_dict["Path"],
            json_dict["UseUtcTime"],
            Utils.string_to_timedelta(json_dict["RetentionTime"]),
            json_dict["RetentionSize"],
            json_dict["Compress"],
        )


class GetLogsConfigurationOperation(ServerOperation[GetLogsConfigurationResult]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.GetLogsConfigurationCommand()

    class GetLogsConfigurationCommand(RavenCommand[GetLogsConfigurationResult]):
        def __init__(self):
            super().__init__(GetLogsConfigurationResult)

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{node.url}/admin/logs/configuration")

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = GetLogsConfigurationResult.from_json(json.loads(response))


class SetLogsConfigurationOperation(VoidServerOperation):
    class Parameters:
        def __init__(
            self,
            mode: LogMode = None,
            retention_time: timedelta = None,
            retention_size: int = None,
            compress: bool = None,
        ) -> None:
            self.mode = mode
            self.retention_time = retention_time
            self.retention_size = retention_size
            self.compress = compress

        @classmethod
        def from_get_logs(cls, get_logs: GetLogsConfigurationResult) -> "SetLogsConfigurationOperation.Parameters":
            return cls(get_logs.mode, get_logs.retention_time, get_logs.retention_size, get_logs.compress)

        def to_json(self) -> Dict[str, Any]:
            return {
                "Mode": self.mode.value,
                "RetentionTime": Utils.timedelta_to_str(self.retention_time),
                "RetentionSize": self.retention_size,
                "Compress": self.compress,
            }

    def __init__(self, parameters: Parameters) -> None:
        if parameters is None:
            raise ValueError("Parameters cannot be None")

        self._parameters = parameters

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.SetLogsConfigurationCommand(self._parameters)

    class SetLogsConfigurationCommand(VoidRavenCommand):
        def __init__(self, parameters: "SetLogsConfigurationOperation.Parameters"):
            if parameters is None:
                raise ValueError("Parameters must be None")

            super().__init__()
            self._parameters = parameters

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/logs/configuration"

            request = requests.Request("POST", url, data=self._parameters.to_json())
            return request
