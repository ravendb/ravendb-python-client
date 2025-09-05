import json
from typing import Dict

import requests

from ravendb import ConnectionString, RavenCommand, ServerNode, RaftCommand
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.olap import OlapConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.util.util import RaftIdGenerator


class PutConnectionStringResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    def to_json(self) -> Dict:
        return {"RaftCommandIndex": self.raft_command_index}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PutConnectionStringResult":
        return cls(json_dict["RaftCommandIndex"])


class PutConnectionStringOperation(MaintenanceOperation[PutConnectionStringResult]):
    def __init__(self, connection_string: ConnectionString = None):
        self._connection_string = connection_string

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[PutConnectionStringResult]":
        return self.PutConnectionStringCommand(conventions, self._connection_string)

    class PutConnectionStringCommand(RavenCommand[PutConnectionStringResult], RaftCommand):
        def __init__(
            self, document_conventions: DocumentConventions = None, connection_string: ConnectionString = None
        ):
            super().__init__(PutConnectionStringResult)

            if connection_string is None:
                raise ValueError("Connection string cannot be None")

            self._document_conventions = document_conventions
            self._connection_string = connection_string

        def _to_data(self) -> Dict:
            if isinstance(self._connection_string, RavenConnectionString):
                return {
                    "Name": self._connection_string.name,
                    "Database": self._connection_string.database,
                    "TopologyDiscoveryUrls": self._connection_string.topology_discovery_urls,
                    "Type": ConnectionStringType.RAVEN,
                }

            if isinstance(self._connection_string, SqlConnectionString):
                return {
                    "Name": self._connection_string.name,
                    "ConnectionString": self._connection_string.connection_string,
                    "FactoryName": self._connection_string.factory_name,
                    "Type": ConnectionStringType.SQL,
                }

            if isinstance(self._connection_string, OlapConnectionString):
                return {
                    "Name": self._connection_string.name,
                    "LocalSettings": self._connection_string.local_settings,
                    "S3Settings": self._connection_string.s3_settings,
                    "AzureSettings": self._connection_string.azure_settings,
                    "GlacierSettings": self._connection_string.glacier_settings,
                    "GoogleCloudSettings": self._connection_string.google_cloud_settings,
                    "FtpSettings": self._connection_string.ftp_settings,
                    "Type": ConnectionStringType.OLAP,
                }

            return None

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/connection-strings"

            request = requests.Request("PUT")
            request.url = url
            request.data = self._to_data()

            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = PutConnectionStringResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
