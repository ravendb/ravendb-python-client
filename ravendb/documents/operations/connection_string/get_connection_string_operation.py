import json
from typing import Dict, Optional

import requests

from ravendb import RavenCommand, ServerNode
from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.elastic_search.connection import ElasticSearchConnectionString
from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
from ravendb.documents.operations.etl.queue.connection import QueueConnectionString
from ravendb.documents.operations.etl.snowflake.connection import SnowflakeConnectionString
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
    def from_json(cls, json_dict: Dict[str, Dict]) -> "GetConnectionStringsResult":
        return cls(
            raven_connection_strings=(
                {key: RavenConnectionString.from_json(rcs) for key, rcs in json_dict["RavenConnectionStrings"].items()}
                if json_dict["RavenConnectionStrings"]
                else None
            ),
            sql_connection_strings=(
                {key: SqlConnectionString.from_json(sqlcs) for key, sqlcs in json_dict["SqlConnectionStrings"].items()}
                if json_dict["SqlConnectionStrings"]
                else None
            ),
            olap_connection_strings=(
                {
                    key: OlapConnectionString.from_json(olapcs)
                    for key, olapcs in json_dict["OlapConnectionStrings"].items()
                }
                if json_dict["OlapConnectionStrings"]
                else None
            ),
            ai_connection_strings=(
                {key: AiConnectionString.from_json(aics) for key, aics in json_dict["AiConnectionStrings"].items()}
                if json_dict["AiConnectionStrings"]
                else None
            ),
            elastic_search_connection_strings=(
                {
                    key: ElasticSearchConnectionString.from_json(escs)
                    for key, escs in json_dict["ElasticSearchConnectionStrings"].items()
                }
                if json_dict["ElasticSearchConnectionStrings"]
                else None
            ),
            queue_connection_strings=(
                {key: QueueConnectionString.from_json(qcs) for key, qcs in json_dict["QueueConnectionStrings"].items()}
                if json_dict["QueueConnectionStrings"]
                else None
            ),
            snowflake_connection_strings=(
                {
                    key: SnowflakeConnectionString.from_json(scs)
                    for key, scs in json_dict["SnowflakeConnectionStrings"].items()
                }
                if json_dict["SnowflakeConnectionStrings"]
                else None
            ),
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
