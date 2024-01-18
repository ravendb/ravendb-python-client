from __future__ import annotations
from abc import ABC
from typing import List, Any, Dict

import requests

from ravendb import RaftCommand, ServerNode
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import VoidServerOperation
from ravendb.util.util import RaftIdGenerator


class IServerWideTask(ABC):
    def __init__(self, excluded_databases: List[str] = None):
        self.excluded_databases = excluded_databases


class ServerWideTaskResponse:
    def __init__(self, name: str = None, raft_command_index: int = None):
        self.name = name
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ServerWideTaskResponse":
        return cls(json_dict["Name"], json_dict["RaftCommandIndex"])


class SetDatabasesLockOperation(VoidServerOperation):
    class Parameters:
        def __init__(self, database_names: List[str] = None, mode: DatabaseRecord.DatabaseLockMode = None):
            self.database_names = database_names
            self.mode = mode

        def to_json(self) -> Dict[str, Any]:
            return {"DatabaseNames": self.database_names, "Mode": self.mode.value}

    def __init__(self, database_name: str, mode: DatabaseRecord.DatabaseLockMode = None):
        if database_name is None:
            raise ValueError("Database name cannot be None")

        self._parameters = self.Parameters()
        self._parameters.database_names = [database_name]
        self._parameters.mode = mode

    @classmethod
    def from_parameters(cls, parameters: Parameters) -> SetDatabasesLockOperation:
        if parameters is None:
            raise ValueError("Parameters cannot be None")

        if not parameters.database_names:
            raise ValueError("Database names cannot be None or empty")

        this = cls("")
        this._parameters = parameters

        return this

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.SetDatabasesLockCommand(conventions, self._parameters)

    class SetDatabasesLockCommand(VoidRavenCommand, RaftCommand):
        def __init__(
            self, conventions: "DocumentConventions", parameters: SetDatabasesLockOperation.Parameters
        ) -> None:
            super().__init__()
            if conventions is None:
                raise ValueError("Conventions cannot be None")
            if parameters is None:
                raise ValueError("Parameters cannot be None")

            self._parameters = parameters.to_json()

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("POST", f"{node.url}/admin/databases/set-lock", data=self._parameters)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
