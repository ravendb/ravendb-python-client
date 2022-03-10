from ravendb.serverwide.commands import GetDatabaseTopologyCommand
from ravendb.tests.test_base import TestBase


class TestGetTopology(TestBase):
    def setUp(self):
        super(TestGetTopology, self).setUp()

    def test_get_topology(self):
        command = GetDatabaseTopologyCommand()
        self.store.get_request_executor().execute_command(command)
        result = command.result
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.etag)
        self.assertEqual(len(result.nodes), 1)

        server_node = result.nodes[0]
        self.assertEqual(server_node.url, self.store.urls[0])
        self.assertEqual(server_node.database, self.store.database)
        self.assertEqual(server_node.cluster_tag, "A")
        self.assertEqual(server_node.server_role, "Member")
