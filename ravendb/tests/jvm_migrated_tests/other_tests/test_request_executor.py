import unittest
from ravendb.http.topology import ServerNode, UpdateTopologyParameters
from ravendb.http.request_executor import RequestExecutor
from ravendb.serverwide.operations.common import GetDatabaseNamesOperation
from ravendb.tests.test_base import TestBase
from ravendb.documents.conventions import DocumentConventions
from ravendb.exceptions.exceptions import DatabaseDoesNotExistException


class TestGetTopology(TestBase):
    def setUp(self):
        super(TestGetTopology, self).setUp()

    def test_can_fetch_databases_names(self):
        executor = RequestExecutor.create(self.store.urls, self.store.database, self.store.conventions)
        database_names_operation = GetDatabaseNamesOperation(0, 20)
        command = database_names_operation.get_command(DocumentConventions())
        executor.execute_command(command)
        self.assertTrue(self.store.database in command.result)

    def test_can_issue_many_requests(self):
        ex = RequestExecutor.create(self.store.urls, self.store.database, self.store.conventions)
        for _ in range(50):
            ex.execute_command(GetDatabaseNamesOperation(0, 20).get_command(DocumentConventions()))

    @unittest.skip("Wait for request executor error handling")
    def test_throws_when_updating_topology_of_not_existing_db(self):
        executor = RequestExecutor.create(self.store.urls, self.store.database, self.store.conventions)
        server_node = ServerNode(self.store.urls[0], "no_such")
        update_topology_parameters = UpdateTopologyParameters(server_node)
        update_topology_parameters.timeout_in_ms = 5000
        with self.assertRaises(DatabaseDoesNotExistException):
            executor.update_topology_async(update_topology_parameters).result()
