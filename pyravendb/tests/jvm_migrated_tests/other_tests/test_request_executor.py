import unittest
from pyravendb.tests.test_base import TestBase
from pyravendb.connection.requests_helpers import ServerNode
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.custom_exceptions.exceptions import DatabaseDoesNotExistException
from pyravendb.raven_operations.server_operations import GetDatabaseNamesOperation


class TestGetTopology(TestBase):
    def setUp(self):
        super(TestGetTopology, self).setUp()

    def test_can_fetch_databases_names(self):
        executor = RequestsExecutor.create(self.store.urls, self.store.database, None, None)
        database_names_operation = GetDatabaseNamesOperation(0, 20)
        command = database_names_operation.get_command(DocumentConventions())
        self.assertTrue(self.store.database in executor.execute(command))

    def test_can_issue_many_requests(self):
        ex = RequestsExecutor.create(self.store.urls, self.store.database, None, None)
        for _ in (ex.execute(GetDatabaseNamesOperation(0, 20).get_command(DocumentConventions())) for _ in range(50)):
            pass

    @unittest.skip("Needs proper error 503 handling while using GetTopologyCommand on wrong database key")
    def test_throws_when_updating_topology_of_not_existing_db(self):
        executor = RequestsExecutor.create(self.store.urls, self.store.database, None, None)
        server_node = ServerNode(self.store.urls[0], "no_such")
        with self.assertRaises(DatabaseDoesNotExistException):
            executor.update_topology(server_node)
