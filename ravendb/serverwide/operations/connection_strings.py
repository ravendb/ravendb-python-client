from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from ravendb.documents.operations.connection_strings import ConnectionString
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.elastic_search.connection import ElasticSearchConnectionString
from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
from ravendb.documents.operations.etl.queue.connection import QueueConnectionString
from ravendb.documents.operations.etl.snowflake.connection import SnowflakeConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ServerOperation
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


def _deserialize_connection_string(json_dict: Dict[str, Any], connection_string_type: ConnectionStringType):
    if connection_string_type == ConnectionStringType.RAVEN:
        return RavenConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.SQL:
        return SqlConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.OLAP:
        return OlapConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.ELASTIC_SEARCH:
        return ElasticSearchConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.QUEUE:
        return QueueConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.SNOWFLAKE:
        return SnowflakeConnectionString.from_json(json_dict)
    if connection_string_type == ConnectionStringType.AI:
        return AiConnectionString.from_json(json_dict)
    raise ValueError(f"Unknown connection string type: {connection_string_type}")


class ServerWideConnectionString:
    def __init__(self, connection_string: ConnectionString = None, excluded_databases: Optional[List[str]] = None):
        self.connection_string = connection_string
        self.excluded_databases = excluded_databases

    @property
    def name(self) -> Optional[str]:
        return self.connection_string.name if self.connection_string else None

    @property
    def type(self) -> Optional[ConnectionStringType]:
        return ConnectionStringType(self.connection_string.get_type) if self.connection_string else None

    def to_json(self) -> Dict[str, Any]:
        json_dict = self.connection_string.to_json() if self.connection_string else {}
        json_dict["Type"] = self.type
        json_dict["ExcludedDatabases"] = self.excluded_databases
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> Optional["ServerWideConnectionString"]:
        if json_dict is None:
            return None

        type_raw = json_dict.get("Type")
        if type_raw is None:
            return None

        return cls(
            connection_string=_deserialize_connection_string(json_dict, ConnectionStringType(type_raw)),
            excluded_databases=json_dict.get("ExcludedDatabases"),
        )


class GetServerWideConnectionStringsResult:
    def __init__(self, results: Optional[List[ServerWideConnectionString]] = None):
        self.results = results if results is not None else []

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "GetServerWideConnectionStringsResult":
        results = []
        for item in json_dict.get("Results") or []:
            parsed = ServerWideConnectionString.from_json(item)
            if parsed is not None:
                results.append(parsed)
        return cls(results)


class GetServerWideConnectionStringsOperation(ServerOperation[GetServerWideConnectionStringsResult]):
    def __init__(
        self,
        connection_string_name: Optional[str] = None,
        connection_string_type: Optional[ConnectionStringType] = None,
    ):
        if connection_string_name is not None and not connection_string_name.strip():
            raise ValueError("Connection string name must not be null or empty.")

        self._connection_string_name = connection_string_name
        self._type = connection_string_type

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[GetServerWideConnectionStringsResult]:
        return GetServerWideConnectionStringsOperation._GetServerWideConnectionStringsCommand(
            self._connection_string_name, self._type
        )

    class _GetServerWideConnectionStringsCommand(RavenCommand[GetServerWideConnectionStringsResult]):
        def __init__(
            self,
            connection_string_name: Optional[str] = None,
            connection_string_type: Optional[ConnectionStringType] = None,
        ):
            super().__init__(GetServerWideConnectionStringsResult)
            self._connection_string_name = connection_string_name
            self._type = connection_string_type

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/connection-strings"

            query_params = []
            if self._connection_string_name is not None:
                query_params.append(f"name={Utils.quote_key(self._connection_string_name)}")
            if self._type is not None:
                query_params.append(f"type={self._type.value}")

            if query_params:
                url += f"?{'&'.join(query_params)}"

            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = GetServerWideConnectionStringsResult.from_json(json.loads(response))


class PutServerWideConnectionStringResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "PutServerWideConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class PutServerWideConnectionStringOperation(ServerOperation[PutServerWideConnectionStringResult]):
    def __init__(self, connection_string: ServerWideConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")
        if connection_string.connection_string is None:
            raise ValueError("ServerWideConnectionString.connection_string must not be null.")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[PutServerWideConnectionStringResult]:
        return PutServerWideConnectionStringOperation._PutServerWideConnectionStringCommand(self._connection_string)

    class _PutServerWideConnectionStringCommand(RavenCommand[PutServerWideConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ServerWideConnectionString):
            super().__init__(PutServerWideConnectionStringResult)
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/connection-strings"
            request = requests.Request("PUT", url)
            request.data = self._connection_string.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = PutServerWideConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class RemoveServerWideConnectionStringResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "RemoveServerWideConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class RemoveServerWideConnectionStringOperation(ServerOperation[RemoveServerWideConnectionStringResult]):
    def __init__(self, connection_string: ConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")
        if not connection_string.name or not connection_string.name.strip():
            raise ValueError("Connection string name must not be null or empty.")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[RemoveServerWideConnectionStringResult]:
        return RemoveServerWideConnectionStringOperation._RemoveServerWideConnectionStringCommand(
            self._connection_string
        )

    class _RemoveServerWideConnectionStringCommand(RavenCommand[RemoveServerWideConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ConnectionString):
            super().__init__(RemoveServerWideConnectionStringResult)
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/admin/configuration/server-wide/connection-strings"
                f"?name={Utils.quote_key(self._connection_string.name)}&type={self._connection_string.get_type}"
            )
            return requests.Request("DELETE", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = RemoveServerWideConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
