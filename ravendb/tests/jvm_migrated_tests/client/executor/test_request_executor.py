from ravendb.documents.conventions.document_conventions import DocumentConventions
from ravendb.exceptions.exceptions import DatabaseDoesNotExistException
from ravendb.http.request_executor import RequestExecutor
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import UpdateTopologyParameters
from ravendb.tests.test_base import TestBase


class TestRequestExecutor(TestBase):
    def setUp(self):
        super(TestRequestExecutor, self).setUp()

    def test_throws_when_updating_topology_of_not_existing_db(self):
        conventions = DocumentConventions()

        with RequestExecutor.create(
            self.store.urls, "no_such_db", conventions, None, None, self.store.thread_pool_executor
        ) as executor:
            server_node = ServerNode(self.store.urls[0], "no_such")
            update_topology_parameters = UpdateTopologyParameters(server_node)
            update_topology_parameters.timeout_in_ms = 5000

            with self.assertRaises(DatabaseDoesNotExistException):
                executor.update_topology_async(update_topology_parameters).result()
