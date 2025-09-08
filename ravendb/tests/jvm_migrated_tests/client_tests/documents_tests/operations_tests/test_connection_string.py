from ravendb import FtpSettings
from ravendb.documents.operations.connection_string.GetConnectionStringOperation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.PutConnectionStringOperation import (
    PutConnectionStringOperation,
    PutConnectionStringResult,
)
from ravendb.documents.operations.connection_string.RemoveConnectionStringOperation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.olap import OlapConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


class TestConnectionString(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_create_get_and_delete_connection_strings(self):
        raven_connection_string_1 = RavenConnectionString("r1", self.store.database, self.store.urls)
        sql_connection_string_1 = SqlConnectionString("s1", "test", "MySql.Data.MySqlClient")
        olap_connection_string_1 = OlapConnectionString("o1", ftp_settings=FtpSettings(url=self.store.urls[0]))

        put_result: PutConnectionStringResult = self.store.maintenance.send(
            PutConnectionStringOperation(raven_connection_string_1)
        )
        self.assertGreater(put_result.raft_command_index, 0)

        put_result: PutConnectionStringResult = self.store.maintenance.send(
            PutConnectionStringOperation(sql_connection_string_1)
        )
        self.assertGreater(put_result.raft_command_index, 0)

        put_result: PutConnectionStringResult = self.store.maintenance.send(
            PutConnectionStringOperation(olap_connection_string_1)
        )
        self.assertGreater(put_result.raft_command_index, 0)

        connection_strings = self.store.maintenance.send(GetConnectionStringsOperation())

        self.assertIn("r1", connection_strings.raven_connection_strings)
        self.assertEqual(1, len(connection_strings.raven_connection_strings))

        self.assertIn("s1", connection_strings.sql_connection_strings)
        self.assertEqual(1, len(connection_strings.sql_connection_strings))

        self.assertIn("o1", connection_strings.olap_connection_strings)
        self.assertEqual(1, len(connection_strings.olap_connection_strings))

        raven_only = self.store.maintenance.send(GetConnectionStringsOperation("r1", ConnectionStringType.RAVEN))
        self.assertIn("r1", raven_only.raven_connection_strings)
        self.assertEqual(1, len(raven_only.raven_connection_strings))
        self.assertIsNone(raven_only.sql_connection_strings)

        sql_only = self.store.maintenance.send(GetConnectionStringsOperation("s1", ConnectionStringType.SQL))
        self.assertIn("s1", sql_only.sql_connection_strings)
        self.assertEqual(1, len(sql_only.sql_connection_strings))
        self.assertIsNone(sql_only.raven_connection_strings)

        olap_only = self.store.maintenance.send(GetConnectionStringsOperation("o1", ConnectionStringType.OLAP))
        self.assertIn("o1", olap_only.olap_connection_strings)
        self.assertEqual(1, len(olap_only.olap_connection_strings))
        self.assertIsNone(olap_only.raven_connection_strings)

        remove_result = self.store.maintenance.send(RemoveConnectionStringOperation(sql_connection_string_1))
        self.assertGreater(remove_result.raft_command_index, 0)

        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("s1", ConnectionStringType.SQL))
        self.assertIsNone(after_delete.raven_connection_strings)
        self.assertIsNone(after_delete.sql_connection_strings)
