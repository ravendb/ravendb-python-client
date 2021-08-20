from pyravendb.commands.raven_commands import GetTopologyCommand
from pyravendb.tests.test_base import TestBase


class TestGetTopology(TestBase):
    def setUp(self):
        super(TestGetTopology, self).setUp()

    def test_get_topology(self):
        command = GetTopologyCommand()
        result = self.store.get_request_executor().execute(command)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result["Etag"])
        self.assertEqual(len(result["Nodes"]), 1)

        server_node = result["Nodes"][0]
        self.assertEqual(server_node["Url"], self.store.urls[0])
        self.assertEqual(server_node["Database"], self.store.database)
        self.assertEqual(server_node["ClusterTag"], "A")
        self.assertEqual(server_node["ServerRole"], "Member")
