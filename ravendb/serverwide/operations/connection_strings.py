from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import requests

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
from ravendb.documents.operations.connection_strings import ConnectionString, ConnectionStringUsage
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.elastic_search.connection import ElasticSearchConnectionString
from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
from ravendb.documents.operations.etl.queue.connection import QueueConnectionString
from ravendb.documents.operations.etl.snowflake.connection import SnowflakeConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ServerOperation
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class ServerWideConnectionStringUsage(ConnectionStringUsage):
    """A server-wide usage of a connection string; the server aggregates usages across
    databases, so each usage also carries the DatabaseName where the task lives."""

    def __init__(
        self,
        kind: Optional[str] = None,
        id: Optional[int] = None,
        identifier: Optional[str] = None,
        name: Optional[str] = None,
        database_name: Optional[str] = None,
    ):
        super().__init__(kind, id, identifier, name)
        self.database_name = database_name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ServerWideConnectionStringUsage:
        return cls(
            kind=json_dict.get("Kind"),
            id=json_dict.get("Id"),
            identifier=json_dict.get("Identifier"),
            name=json_dict.get("Name"),
            database_name=json_dict.get("DatabaseName"),
        )


_CONNECTION_STRING_FROM_JSON = {
    ConnectionStringType.RAVEN: RavenConnectionString.from_json,
    ConnectionStringType.SQL: SqlConnectionString.from_json,
    ConnectionStringType.OLAP: OlapConnectionString.from_json,
    ConnectionStringType.ELASTIC_SEARCH: ElasticSearchConnectionString.from_json,
    ConnectionStringType.QUEUE: QueueConnectionString.from_json,
    ConnectionStringType.SNOWFLAKE: SnowflakeConnectionString.from_json,
    ConnectionStringType.AI: AiConnectionString.from_json,
}


class ServerWideConnectionString:
    def __init__(
        self,
        connection_string: Optional[ConnectionString] = None,
        excluded_databases: Optional[List[str]] = None,
        used_by: Optional[List[ServerWideConnectionStringUsage]] = None,
    ):
        self.connection_string = connection_string
        self.excluded_databases = excluded_databases
        self.used_by = used_by if used_by is not None else []

    @property
    def name(self) -> Optional[str]:
        return self.connection_string.name if self.connection_string else None

    @property
    def get_type(self) -> str:
        return self.connection_string.get_type if self.connection_string else ConnectionStringType.NONE.value

    def to_json(self) -> Dict[str, Any]:
        # UsedBy is read-only server metadata and is never written (C# ToJson omits it).
        json_dict = self.connection_string.to_json() if self.connection_string else {}
        json_dict["Type"] = self.get_type
        json_dict["ExcludedDatabases"] = self.excluded_databases
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> Optional[ServerWideConnectionString]:
        if json_dict is None:
            return None

        # The server rejects a body without Type; the C# FromBlittable returns null there.
        type_value = json_dict.get("Type")
        if not type_value:
            return None

        try:
            connection_string_type = ConnectionStringType(type_value)
        except ValueError:
            return None

        from_json = _CONNECTION_STRING_FROM_JSON.get(connection_string_type)
        if from_json is None:
            return None

        result = cls(
            connection_string=from_json(json_dict),
            excluded_databases=json_dict.get("ExcludedDatabases"),
        )

        used_by = json_dict.get("UsedBy")
        if used_by:
            result.used_by = [ServerWideConnectionStringUsage.from_json(usage) for usage in used_by]

        return result


class PutServerWideConnectionStringResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> PutServerWideConnectionStringResult:
        return cls(raft_command_index=json_dict.get("RaftCommandIndex"))


class PutServerWideConnectionStringOperation(ServerOperation[PutServerWideConnectionStringResult]):
    def __init__(self, connection_string: ServerWideConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")
        if connection_string.connection_string is None:
            raise ValueError("connection_string.connection_string must not be None")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[PutServerWideConnectionStringResult]:
        return self.PutServerWideConnectionStringCommand(self._connection_string)

    class PutServerWideConnectionStringCommand(RavenCommand[PutServerWideConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ServerWideConnectionString):
            super().__init__(PutServerWideConnectionStringResult)
            if connection_string is None:
                raise ValueError("connection_string cannot be None")
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/configuration/server-wide/connection-strings"
            request = requests.Request("PUT", url)
            request.headers = {"Content-Type": "application/json"}
            request.data = json.dumps(self._connection_string.to_json())
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = PutServerWideConnectionStringResult.from_json(json.loads(response))


class GetServerWideConnectionStringsResult:
    def __init__(self, results: Optional[List[ServerWideConnectionString]] = None):
        self.results = results if results is not None else []

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetServerWideConnectionStringsResult:
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
        type: Optional[ConnectionStringType] = None,
    ):
        if connection_string_name is not None and (not connection_string_name or connection_string_name.isspace()):
            raise ValueError("Connection string name must not be null or empty.")

        self._connection_string_name = connection_string_name
        self._type = type

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[GetServerWideConnectionStringsResult]:
        return self.GetServerWideConnectionStringsCommand(self._connection_string_name, self._type)

    class GetServerWideConnectionStringsCommand(RavenCommand[GetServerWideConnectionStringsResult]):
        def __init__(
            self,
            connection_string_name: Optional[str] = None,
            type: Optional[ConnectionStringType] = None,
        ):
            super().__init__(GetServerWideConnectionStringsResult)
            self._connection_string_name = connection_string_name
            self._type = type

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            from urllib.parse import quote

            url = f"{node.url}/admin/configuration/server-wide/connection-strings"

            query_params = []
            if self._connection_string_name is not None:
                query_params.append(f"name={quote(self._connection_string_name)}")
            if self._type is not None and self._type != ConnectionStringType.NONE:
                # The query value is the enum NAME string, never the Python enum repr.
                query_params.append(f"type={self._type.value}")

            if query_params:
                url += "?" + "&".join(query_params)

            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = GetServerWideConnectionStringsResult.from_json(json.loads(response))


class RemoveServerWideConnectionStringResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RemoveServerWideConnectionStringResult:
        return cls(raft_command_index=json_dict.get("RaftCommandIndex"))


class RemoveServerWideConnectionStringOperation(ServerOperation[RemoveServerWideConnectionStringResult]):
    def __init__(self, connection_string: ConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")
        if not connection_string.name or connection_string.name.isspace():
            raise ValueError("Connection string name must not be null or empty.")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[RemoveServerWideConnectionStringResult]:
        return self.RemoveServerWideConnectionStringCommand(self._connection_string)

    class RemoveServerWideConnectionStringCommand(RavenCommand[RemoveServerWideConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ConnectionString):
            super().__init__(RemoveServerWideConnectionStringResult)
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, node: ServerNode) -> requests.Request:
            from urllib.parse import quote

            url = (
                f"{node.url}/admin/configuration/server-wide/connection-strings"
                f"?name={quote(self._connection_string.name)}"
                f"&type={self._connection_string.get_type}"
            )

            return requests.Request("DELETE", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = RemoveServerWideConnectionStringResult.from_json(json.loads(response))
