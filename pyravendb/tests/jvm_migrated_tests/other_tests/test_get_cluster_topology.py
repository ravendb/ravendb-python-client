from pyravendb.commands.raven_commands import GetClusterTopologyCommand
from pyravendb.tests.test_base import TestBase


class TestGetClusterTopology(TestBase):
    def setUp(self):
        super(TestGetClusterTopology, self).setUp()

    def test_get_cluster_topology(self):
        command = GetClusterTopologyCommand()
        result = self.store.get_request_executor().execute(command)
        self.assertIsNotNone(result)
        self.assertNotEqual(len(result["Leader"]), 0)
        self.assertNotEqual(len(result["NodeTag"]), 0)

        topology = result["Topology"]
        self.assertIsNotNone(topology)
        self.assertIsNotNone(topology["TopologyId"])
        self.assertEqual(len(topology["Members"]), 1)
        self.assertEqual(len(topology["Watchers"]), 0)
        self.assertEqual(len(topology["Promotables"]), 0)
