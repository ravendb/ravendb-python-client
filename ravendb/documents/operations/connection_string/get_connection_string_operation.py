import json
from typing import Dict, Optional

import requests

from ravendb import RavenCommand, ServerNode
from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.elastic_search import ElasticSearchConnectionString
from ravendb.documents.operations.etl.olap import OlapConnectionString
from ravendb.documents.operations.etl.queue import QueueConnectionString
from ravendb.documents.operations.etl.snowflake import SnowflakeConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType


class GetConnectionStringsResult:
    def __init__(
        self,
        raven_connection_strings: Dict[str, RavenConnectionString] = None,
        sql_connection_strings: Dict[str, SqlConnectionString] = None,
        olap_connection_strings: Dict[str, OlapConnectionString] = None,
        ai_connection_strings: Dict[str, AiConnectionString] = None,
        elastic_search_connection_strings: Dict[str, ElasticSearchConnectionString] = None,
        queue_connection_strings: Dict[str, QueueConnectionString] = None,
        snowflake_connection_strings: Dict[str, SnowflakeConnectionString] = None,
    ):
        self.raven_connection_strings = raven_connection_strings
        self.sql_connection_strings = sql_connection_strings
        self.olap_connection_strings = olap_connection_strings
        self.ai_connection_strings = ai_connection_strings
        self.elastic_search_connection_strings = elastic_search_connection_strings
        self.queue_connection_strings = queue_connection_strings
        self.snowflake_connection_strings = snowflake_connection_strings

    def to_json(self) -> Dict:
        return {
            "RavenConnectionStrings": [x.to_json() for x in self.raven_connection_strings.values()],
            "SqlConnectionStrings": [x.to_json() for x in self.sql_connection_strings.values()],
            "OlapConnectionStrings": [x.to_json() for x in self.olap_connection_strings.values()],
            "AiConnectionStrings": [x.to_json() for x in self.ai_connection_strings.values()],
            "ElasticSearchConnectionStrings": [x.to_json() for x in self.elastic_search_connection_strings.values()],
            "QueueConnectionStrings": [x.to_json() for x in self.queue_connection_strings.values()],
            "SnowflakeConnectionStrings": [x.to_json() for x in self.snowflake_connection_strings.values()],
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "GetConnectionStringsResult":
        return cls(
            raven_connection_strings=json_dict["RavenConnectionStrings"],
            sql_connection_strings=json_dict["SqlConnectionStrings"],
            olap_connection_strings=json_dict["OlapConnectionStrings"],
            ai_connection_strings=json_dict["AiConnectionStrings"],
            elastic_search_connection_strings=json_dict["ElasticSearchConnectionStrings"],
            queue_connection_strings=json_dict["QueueConnectionStrings"],
            snowflake_connection_strings=json_dict["SnowflakeConnectionStrings"],
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
