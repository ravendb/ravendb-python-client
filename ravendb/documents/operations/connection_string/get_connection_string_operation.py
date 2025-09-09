import json
from typing import Dict, Optional

import requests

from ravendb import RavenCommand, ServerNode
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.olap import OlapConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType


class GetConnectionStringsResult:
    def __init__(
        self,
        raven_connection_strings: Dict[str, RavenConnectionString] = None,
        sql_connection_strings: Dict[str, SqlConnectionString] = None,
        olap_connection_strings: Dict[str, OlapConnectionString] = None,
    ):
        self.raven_connection_strings = raven_connection_strings
        self.sql_connection_strings = sql_connection_strings
        self.olap_connection_strings = olap_connection_strings

    def to_json(self) -> Dict:
        return {
            "RavenConnectionStrings": self._raven_connection_strings,
            "SqlConnectionStrings": self._sql_connection_strings,
            "OlapConnectionStrings": self._olap_connection_strings,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "GetConnectionStringsResult":
        return cls(
            raven_connection_strings=json_dict["RavenConnectionStrings"],
            sql_connection_strings=json_dict["SqlConnectionStrings"],
            olap_connection_strings=json_dict["OlapConnectionStrings"],
        )


class GetConnectionStringsOperation(MaintenanceOperation[GetConnectionStringsResult]):
    def __init__(self, connection_string_name: str = None, connection_string_type: ConnectionStringType = None):
        self._connection_string_name = connection_string_name
        self._type = connection_string_type

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[GetConnectionStringsResult]":
        return self.GetConnectionStringsCommand(self._connection_string_name, self._type)

    class GetConnectionStringsCommand(RavenCommand[GetConnectionStringsResult]):
        def __init__(self, connection_string_name: str = None, connection_string_type: ConnectionStringType = None):
            super().__init__(GetConnectionStringsResult)
            self._connection_string_name = connection_string_name
            self._type = connection_string_type

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/connection-strings"

            if self._connection_string_name:
                url += f"?connectionStringName={self._connection_string_name}&type={self._type.value}"

            request = requests.Request("GET")
            request.url = url

            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = GetConnectionStringsResult.from_json(json.loads(response))
