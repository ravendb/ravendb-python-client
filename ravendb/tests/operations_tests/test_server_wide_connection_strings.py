"""Server-wide connection string tests, ported from RavenDB_24310.

The live tests need a license that includes the ServerWideConnectionStrings
feature; under a license that gates it they skip on the server's own refusal.
"""

import os
import unittest

from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.http.server_node import ServerNode
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    PutServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringOperation,
    ServerWideConnectionString,
)
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import (
    CreateDatabaseOperation,
    DeleteDatabaseOperation,
    GetDatabaseRecordOperation,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase

CONNECTION_STRING_NAME = "MyRavenCS"


def _server_wide_connection_string(database="TargetDb"):
    return ServerWideConnectionString(
        connection_string=RavenConnectionString(
            name=CONNECTION_STRING_NAME,
            database=database,
            topology_discovery_urls=["http://localhost:8080"],
        )
    )


class TestServerWideConnectionStringWireShape(unittest.TestCase):
    def test_get_url_with_name_and_type(self):
        operation = GetServerWideConnectionStringsOperation(CONNECTION_STRING_NAME, ConnectionStringType.RAVEN)
        request = operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual(
            request.url,
            "http://localhost:8080/admin/configuration/server-wide/connection-strings"
            f"?name={CONNECTION_STRING_NAME}&type=Raven",
        )

    def test_get_url_without_filters(self):
        operation = GetServerWideConnectionStringsOperation()
        request = operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual(
            request.url,
            "http://localhost:8080/admin/configuration/server-wide/connection-strings",
        )

    def test_put_payload(self):
        operation = PutServerWideConnectionStringOperation(_server_wide_connection_string())
        request = operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual("PUT", request.method)
        self.assertEqual(
            request.url,
            "http://localhost:8080/admin/configuration/server-wide/connection-strings",
        )
        self.assertEqual(
            request.data,
            {
                "Name": CONNECTION_STRING_NAME,
                "Database": "TargetDb",
                "TopologyDiscoveryUrls": ["http://localhost:8080"],
                "Type": ConnectionStringType.RAVEN,
                "ExcludedDatabases": None,
            },
        )

    def test_delete_url(self):
        operation = RemoveServerWideConnectionStringOperation(RavenConnectionString(name=CONNECTION_STRING_NAME))
        request = operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual("DELETE", request.method)
        self.assertEqual(
            request.url,
            "http://localhost:8080/admin/configuration/server-wide/connection-strings"
            f"?name={CONNECTION_STRING_NAME}&type=Raven",
        )


@unittest.skipIf(
    os.environ.get("RAVENDB_LICENSE") is None and os.environ.get("RAVEN_License") is None,
    "Insufficient license permissions. Skipping on CI/CD.",
)
class TestServerWideConnectionStrings(TestBase):
    def setUp(self):
        super().setUp()

    def _run(self, operation):
        try:
            return self.store.maintenance.server.send(operation)
        except RavenException as e:
            # The ServerWideConnectionStrings license feature gates the endpoint;
            # skip when the deployed license does not include it.
            if "doesn't support" in str(e) and "connection string" in str(e).lower():
                self.skipTest("License does not support server-wide connection strings")
            raise

    def _put(self, database="TargetDb"):
        result = self._run(PutServerWideConnectionStringOperation(_server_wide_connection_string(database)))
        self.assertGreater(result.raft_command_index, 0)
        return result

    def _get(self):
        return self._run(GetServerWideConnectionStringsOperation(CONNECTION_STRING_NAME, ConnectionStringType.RAVEN))

    def test_can_create_and_get_server_wide_connection_string(self):
        self._put()

        get_result = self._get()
        self.assertEqual(1, len(get_result.results))
        self.assertEqual(CONNECTION_STRING_NAME, get_result.results[0].name)
        self.assertEqual(ConnectionStringType.RAVEN, get_result.results[0].type)
        self.assertEqual("TargetDb", get_result.results[0].connection_string.database)
        self.assertEqual(
            ["http://localhost:8080"],
            get_result.results[0].connection_string.topology_discovery_urls,
        )

    def test_can_delete_server_wide_connection_string(self):
        self._put()
        delete_result = self._run(
            RemoveServerWideConnectionStringOperation(RavenConnectionString(name=CONNECTION_STRING_NAME))
        )
        self.assertGreater(delete_result.raft_command_index, 0)
        self.assertEqual(0, len(self._get().results))

    def test_deleting_non_existent_server_wide_connection_string_is_no_op(self):
        delete_result = self._run(RemoveServerWideConnectionStringOperation(RavenConnectionString(name="DoesNotExist")))
        self.assertGreater(delete_result.raft_command_index, 0)
        self.assertEqual(0, len(self._get().results))

    def test_server_wide_connection_string_propagated_to_new_database(self):
        self._put()
        new_db = self.store.database + "-swcs"
        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(new_db)))
        try:
            record = self.store.maintenance.server.send(GetDatabaseRecordOperation(new_db))
            expected_name = "Server Wide Connection String, " + CONNECTION_STRING_NAME
            self.assertIn(expected_name, record.raven_connection_strings)
            self.assertEqual("TargetDb", record.raven_connection_strings[expected_name]["Database"])
        finally:
            self.store.maintenance.server.send(DeleteDatabaseOperation(new_db, hard_delete=True))
