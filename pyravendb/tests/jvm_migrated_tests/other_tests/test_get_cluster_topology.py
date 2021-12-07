from pyravendb.serverwide.commands import GetClusterTopologyCommand
from pyravendb.tests.test_base import TestBase


class TestGetClusterTopology(TestBase):
    def setUp(self):
        super(TestGetClusterTopology, self).setUp()

    def test_get_cluster_topology(self):
        command = GetClusterTopologyCommand()
        self.store.get_request_executor().execute_command(command)
        result = command.result
        self.assertIsNotNone(result)
        self.assertNotEqual(len(result.leader), 0)
        self.assertNotEqual(len(result.node_tag), 0)

        topology = result.topology
        self.assertIsNotNone(topology)
        self.assertIsNotNone(topology.topology_id)
        self.assertEqual(len(topology.members), 1)
        self.assertEqual(len(topology.watchers), 0)
        self.assertEqual(len(topology.promotables), 0)
