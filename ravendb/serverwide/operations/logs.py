from __future__ import annotations
import json
from enum import Enum
from typing import List, Optional, Any, Dict, TYPE_CHECKING

import requests

from ravendb import ServerNode
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.serverwide.operations.common import ServerOperation, T, VoidServerOperation

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class LogLevel(Enum):
    # RavenDB 7.x logging is NLog-based; the old LogMode (None/Operations/Information) is gone.
    TRACE = "Trace"
    DEBUG = "Debug"
    INFO = "Info"
    WARN = "Warn"
    ERROR = "Error"
    FATAL = "Fatal"
    OFF = "Off"

    def __str__(self):
        return self.value


class LogFilterAction(Enum):
    NEUTRAL = "Neutral"
    LOG = "Log"
    IGNORE = "Ignore"
    LOG_FINAL = "LogFinal"
    IGNORE_FINAL = "IgnoreFinal"

    def __str__(self):
        return self.value


class LogFilter:
    def __init__(
        self,
        min_level: LogLevel = None,
        max_level: LogLevel = None,
        condition: str = None,
        action: LogFilterAction = None,
    ):
        self.min_level = min_level
        self.max_level = max_level
        self.condition = condition
        self.action = action

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> LogFilter:
        return cls(
            LogLevel(json_dict["MinLevel"]),
            LogLevel(json_dict["MaxLevel"]),
            json_dict.get("Condition"),
            LogFilterAction(json_dict["Action"]),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "MinLevel": self.min_level.value,
            "MaxLevel": self.max_level.value,
            "Condition": self.condition,
            "Action": self.action.value,
        }


# ---- GET /admin/logs/configuration result sub-objects ----
class LogsConfiguration:
    def __init__(
        self,
        path: str = None,
        current_min_level: LogLevel = None,
        current_filters: List[LogFilter] = None,
        current_log_filter_default_action: LogFilterAction = None,
        min_level: LogLevel = None,
        archive_above_size_in_mb: int = None,
        max_archive_days: int = None,
        max_archive_files: int = None,
        enable_archive_file_compression: bool = None,
    ):
        self.path = path
        self.current_min_level = current_min_level
        self.current_filters = current_filters if current_filters is not None else []
        self.current_log_filter_default_action = current_log_filter_default_action
        self.min_level = min_level
        self.archive_above_size_in_mb = archive_above_size_in_mb
        self.max_archive_days = max_archive_days
        self.max_archive_files = max_archive_files
        self.enable_archive_file_compression = enable_archive_file_compression

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> LogsConfiguration:
        return cls(
            path=json_dict.get("Path"),
            current_min_level=LogLevel(json_dict["CurrentMinLevel"]),
            current_filters=[LogFilter.from_json(f) for f in (json_dict.get("CurrentFilters") or [])],
            current_log_filter_default_action=LogFilterAction(json_dict["CurrentLogFilterDefaultAction"]),
            min_level=LogLevel(json_dict["MinLevel"]),
            archive_above_size_in_mb=json_dict.get("ArchiveAboveSizeInMb"),
            max_archive_days=json_dict.get("MaxArchiveDays"),
            max_archive_files=json_dict.get("MaxArchiveFiles"),
            enable_archive_file_compression=json_dict.get("EnableArchiveFileCompression"),
        )


class AuditLogsConfiguration:
    def __init__(
        self,
        path: str = None,
        level: LogLevel = None,
        archive_above_size_in_mb: int = None,
        max_archive_days: int = None,
        max_archive_files: int = None,
        enable_archive_file_compression: bool = None,
    ):
        self.path = path
        self.level = level
        self.archive_above_size_in_mb = archive_above_size_in_mb
        self.max_archive_days = max_archive_days
        self.max_archive_files = max_archive_files
        self.enable_archive_file_compression = enable_archive_file_compression

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AuditLogsConfiguration:
        return cls(
            path=json_dict.get("Path"),
            level=LogLevel(json_dict["Level"]),
            archive_above_size_in_mb=json_dict.get("ArchiveAboveSizeInMb"),
            max_archive_days=json_dict.get("MaxArchiveDays"),
            max_archive_files=json_dict.get("MaxArchiveFiles"),
            enable_archive_file_compression=json_dict.get("EnableArchiveFileCompression"),
        )


class MicrosoftLogsConfiguration:
    def __init__(self, current_min_level: LogLevel = None, min_level: LogLevel = None):
        self.current_min_level = current_min_level
        self.min_level = min_level

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> MicrosoftLogsConfiguration:
        return cls(LogLevel(json_dict["CurrentMinLevel"]), LogLevel(json_dict["MinLevel"]))


class AdminLogsConfiguration:
    def __init__(
        self,
        current_min_level: LogLevel = None,
        current_filters: List[LogFilter] = None,
        current_log_filter_default_action: LogFilterAction = None,
    ):
        self.current_min_level = current_min_level
        self.current_filters = current_filters if current_filters is not None else []
        self.current_log_filter_default_action = current_log_filter_default_action

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AdminLogsConfiguration:
        return cls(
            LogLevel(json_dict["CurrentMinLevel"]),
            [LogFilter.from_json(f) for f in (json_dict.get("CurrentFilters") or [])],
            LogFilterAction(json_dict["CurrentLogFilterDefaultAction"]),
        )


class GetLogsConfigurationResult:
    def __init__(
        self,
        logs: LogsConfiguration = None,
        audit_logs: AuditLogsConfiguration = None,
        microsoft_logs: MicrosoftLogsConfiguration = None,
        admin_logs: AdminLogsConfiguration = None,
    ):
        self.logs = logs
        self.audit_logs = audit_logs
        self.microsoft_logs = microsoft_logs
        self.admin_logs = admin_logs

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetLogsConfigurationResult:
        return cls(
            LogsConfiguration.from_json(json_dict["Logs"]) if json_dict.get("Logs") else None,
            AuditLogsConfiguration.from_json(json_dict["AuditLogs"]) if json_dict.get("AuditLogs") else None,
            (
                MicrosoftLogsConfiguration.from_json(json_dict["MicrosoftLogs"])
                if json_dict.get("MicrosoftLogs")
                else None
            ),
            AdminLogsConfiguration.from_json(json_dict["AdminLogs"]) if json_dict.get("AdminLogs") else None,
        )


class GetLogsConfigurationOperation(ServerOperation[GetLogsConfigurationResult]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.GetLogsConfigurationCommand()

    class GetLogsConfigurationCommand(RavenCommand[GetLogsConfigurationResult]):
        def __init__(self):
            super().__init__(GetLogsConfigurationResult)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{node.url}/admin/logs/configuration")

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = GetLogsConfigurationResult.from_json(json.loads(response))


class SetLogsConfigurationOperation(VoidServerOperation):
    # Mirrors the .NET client: one call sets exactly one of Logs / MicrosoftLogs / AdminLogs (+ Persist).
    class LogsConfiguration:
        def __init__(
            self,
            min_level: LogLevel,
            filters: List[LogFilter] = None,
            log_filter_default_action: LogFilterAction = None,
        ):
            self.min_level = min_level
            self.filters = filters if filters is not None else []
            self.log_filter_default_action = log_filter_default_action

        def to_json(self) -> Dict[str, Any]:
            result = {"MinLevel": self.min_level.value, "Filters": [f.to_json() for f in self.filters]}
            if self.log_filter_default_action is not None:
                result["LogFilterDefaultAction"] = self.log_filter_default_action.value
            return result

    class MicrosoftLogsConfiguration:
        def __init__(self, min_level: LogLevel):
            self.min_level = min_level

        def to_json(self) -> Dict[str, Any]:
            return {"MinLevel": self.min_level.value}

    class AdminLogsConfiguration:
        def __init__(
            self,
            min_level: LogLevel,
            filters: List[LogFilter] = None,
            log_filter_default_action: LogFilterAction = None,
        ):
            self.min_level = min_level
            self.filters = filters if filters is not None else []
            self.log_filter_default_action = log_filter_default_action

        def to_json(self) -> Dict[str, Any]:
            result = {"MinLevel": self.min_level.value, "Filters": [f.to_json() for f in self.filters]}
            if self.log_filter_default_action is not None:
                result["LogFilterDefaultAction"] = self.log_filter_default_action.value
            return result

    class Parameters:
        def __init__(
            self,
            logs: "SetLogsConfigurationOperation.LogsConfiguration" = None,
            microsoft_logs: "SetLogsConfigurationOperation.MicrosoftLogsConfiguration" = None,
            admin_logs: "SetLogsConfigurationOperation.AdminLogsConfiguration" = None,
            persist: bool = False,
        ) -> None:
            self.logs = logs
            self.microsoft_logs = microsoft_logs
            self.admin_logs = admin_logs
            self.persist = persist

        def to_json(self) -> Dict[str, Any]:
            result: Dict[str, Any] = {"Persist": self.persist}
            if self.logs is not None:
                result["Logs"] = self.logs.to_json()
            if self.microsoft_logs is not None:
                result["MicrosoftLogs"] = self.microsoft_logs.to_json()
            if self.admin_logs is not None:
                result["AdminLogs"] = self.admin_logs.to_json()
            return result

    def __init__(self, configuration, persist: bool = False) -> None:
        if configuration is None:
            raise ValueError("Configuration cannot be None")

        parameters = SetLogsConfigurationOperation.Parameters(persist=persist)
        if isinstance(configuration, SetLogsConfigurationOperation.LogsConfiguration):
            parameters.logs = configuration
        elif isinstance(configuration, SetLogsConfigurationOperation.MicrosoftLogsConfiguration):
            parameters.microsoft_logs = configuration
        elif isinstance(configuration, SetLogsConfigurationOperation.AdminLogsConfiguration):
            parameters.admin_logs = configuration
        else:
            raise TypeError("Unsupported configuration type: " + type(configuration).__name__)

        self._parameters = parameters

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.SetLogsConfigurationCommand(self._parameters)

    class SetLogsConfigurationCommand(VoidRavenCommand):
        def __init__(self, parameters: "SetLogsConfigurationOperation.Parameters"):
            if parameters is None:
                raise ValueError("Parameters cannot be None")

            super().__init__()
            self._parameters = parameters

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/logs/configuration"

            request = requests.Request("POST", url, data=self._parameters.to_json())
            return request
