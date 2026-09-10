from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import requests

from ravendb.documents.operations.connection_strings import (
    ConnectionString,
    ConnectionStringUsage,
    ConnectionStringUsageKind,
)
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ServerOperation
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class ServerWideConnectionStringUsage(ConnectionStringUsage):
    """
    A usage of a server-wide connection string. Adds the database the referencing task
    or agent lives in, since server-wide usages are aggregated across every database.
    """

    def __init__(
        self,
        kind: ConnectionStringUsageKind = None,
        id_: Optional[int] = None,
        identifier: str = None,
        name: str = None,
        database_name: str = None,
    ):
        super().__init__(kind, id_, identifier, name)
        self.database_name = database_name

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["DatabaseName"] = self.database_name
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ServerWideConnectionStringUsage:
        kind = json_dict.get("Kind")
        return cls(
            kind=ConnectionStringUsageKind(kind) if kind else None,
            id_=json_dict.get("Id"),
            identifier=json_dict.get("Identifier"),
            name=json_dict.get("Name"),
            database_name=json_dict.get("DatabaseName"),
        )


class ServerWideConnectionString:
    """
    A connection string that the cluster propagates to every database, except those
    listed in excluded_databases.
    """

    NAME_PREFIX = "Server Wide Connection String"

    def __init__(
        self,
        connection_string: ConnectionString = None,
        excluded_databases: List[str] = None,
        used_by: List[ServerWideConnectionStringUsage] = None,
    ):
        self.connection_string = connection_string
        # Databases that should not receive this connection string. When None or empty it
        # is propagated everywhere.
        self.excluded_databases = excluded_databases
        # Computed server-side when reading; anything a client sends here is ignored.
        self.used_by = used_by or []

    @property
    def name(self) -> Optional[str]:
        return self.connection_string.name if self.connection_string else None

    @property
    def type(self) -> ConnectionStringType:
        if self.connection_string is None:
            return ConnectionStringType.NONE
        return ConnectionStringType(self.connection_string.get_type)

    @staticmethod
    def get_database_record_connection_string_name(name: str) -> str:
        """The name a propagated server-wide connection string carries inside a database record."""
        return f"{ServerWideConnectionString.NAME_PREFIX}, {name}"

    def to_json(self) -> Dict[str, Any]:
        # The connection string is flattened into the top-level object rather than nested,
        # which is what the server-wide endpoint expects.
        json_dict = self.connection_string.to_json() if self.connection_string else {}
        json_dict["Type"] = self.type.value
        json_dict["ExcludedDatabases"] = self.excluded_databases
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Optional[ServerWideConnectionString]:
        if json_dict is None:
            return None

        type_ = json_dict.get("Type")
        if type_ is None:
            return None

        return cls(
            connection_string=cls._deserialize_connection_string(json_dict, ConnectionStringType(type_)),
            excluded_databases=json_dict.get("ExcludedDatabases"),
            used_by=[ServerWideConnectionStringUsage.from_json(usage) for usage in json_dict.get("UsedBy") or []],
        )

    @staticmethod
    def _deserialize_connection_string(
        json_dict: Dict[str, Any], connection_string_type: ConnectionStringType
    ) -> ConnectionString:
        from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
        from ravendb.documents.operations.etl.configuration import RavenConnectionString
        from ravendb.documents.operations.etl.elastic_search.connection import ElasticSearchConnectionString
        from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
        from ravendb.documents.operations.etl.queue.connection import QueueConnectionString
        from ravendb.documents.operations.etl.snowflake.connection import SnowflakeConnectionString
        from ravendb.documents.operations.etl.sql import SqlConnectionString

        types = {
            ConnectionStringType.RAVEN: RavenConnectionString,
            ConnectionStringType.SQL: SqlConnectionString,
            ConnectionStringType.OLAP: OlapConnectionString,
            ConnectionStringType.ELASTIC_SEARCH: ElasticSearchConnectionString,
            ConnectionStringType.QUEUE: QueueConnectionString,
            ConnectionStringType.SNOWFLAKE: SnowflakeConnectionString,
            ConnectionStringType.AI: AiConnectionString,
        }

        if connection_string_type not in types:
            raise NotImplementedError(f"Unknown connection string type: {connection_string_type}")

        return types[connection_string_type].from_json(json_dict)


class GetServerWideConnectionStringsResult:
    def __init__(self, results: List[ServerWideConnectionString] = None):
        self.results = results or []

    def to_json(self) -> Dict[str, Any]:
        return {"Results": [connection_string.to_json() for connection_string in self.results]}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetServerWideConnectionStringsResult:
        return cls(results=[ServerWideConnectionString.from_json(result) for result in json_dict.get("Results") or []])


class GetServerWideConnectionStringsOperation(ServerOperation[GetServerWideConnectionStringsResult]):
    """
    Reads server-wide connection strings from the cluster, either all of them or
    filtered by name and type.
    """

    def __init__(self, connection_string_name: str = None, connection_string_type: ConnectionStringType = None):
        if connection_string_name is not None and not connection_string_name.strip():
            raise ValueError("Connection string name must not be empty.")

        self._connection_string_name = connection_string_name
        self._type = connection_string_type

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[GetServerWideConnectionStringsResult]:
        return self._GetServerWideConnectionStringsCommand(self._connection_string_name, self._type)

    class _GetServerWideConnectionStringsCommand(RavenCommand[GetServerWideConnectionStringsResult]):
        def __init__(self, connection_string_name: str = None, connection_string_type: ConnectionStringType = None):
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
            if self._type is not None and self._type != ConnectionStringType.NONE:
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

    def to_json(self) -> Dict[str, Any]:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> PutServerWideConnectionStringResult:
        return cls(json_dict.get("RaftCommandIndex"))


class PutServerWideConnectionStringOperation(ServerOperation[PutServerWideConnectionStringResult]):
    """
    Creates or updates a server-wide connection string. The cluster propagates it to
    every database that is not listed in excluded_databases.
    """

    def __init__(self, connection_string: ServerWideConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")

        if connection_string.connection_string is None:
            raise ValueError("ServerWideConnectionString.connection_string must not be None.")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[PutServerWideConnectionStringResult]:
        return self._PutServerWideConnectionStringCommand(self._connection_string)

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

    def to_json(self) -> Dict[str, Any]:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RemoveServerWideConnectionStringResult:
        return cls(json_dict.get("RaftCommandIndex"))


class RemoveServerWideConnectionStringOperation(ServerOperation[RemoveServerWideConnectionStringResult]):
    """
    Removes a server-wide connection string from the cluster and from every database
    record that received it. Fails when an ongoing task still uses it.
    """

    def __init__(self, connection_string: ConnectionString):
        if connection_string is None:
            raise ValueError("connection_string cannot be None")

        if not connection_string.name or connection_string.name.isspace():
            raise ValueError("Connection string name must not be empty.")

        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[RemoveServerWideConnectionStringResult]:
        return self._RemoveServerWideConnectionStringCommand(self._connection_string)

    class _RemoveServerWideConnectionStringCommand(RavenCommand[RemoveServerWideConnectionStringResult], RaftCommand):
        def __init__(self, connection_string: ConnectionString):
            super().__init__(RemoveServerWideConnectionStringResult)
            self._connection_string = connection_string

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/admin/configuration/server-wide/connection-strings"
                f"?name={Utils.quote_key(self._connection_string.name)}"
                f"&type={self._connection_string.get_type}"
            )

            return requests.Request("DELETE", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = RemoveServerWideConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
