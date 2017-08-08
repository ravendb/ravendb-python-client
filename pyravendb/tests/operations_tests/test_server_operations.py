from pyravendb.tests.test_base import *
from pyravendb.commands.raven_commands import WaitForRaftIndexCommand
from pyravendb.connection.cluster_requests_executor import ClusterRequestExecutor
import unittest


class TestServerOperations(TestBase):
    def tearDown(self):
        super(TestServerOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_create_database_name_longer_than_260_chars(self):
        name = "long_database_name_" + ''.join(['z' for i in range(100)])
        try:
            self.store.admin.server.send(CreateDatabaseOperation(database_name=name))
            TestBase.wait_for_database_topology(self.store, name)
            database_names = self.store.admin.server.send(GetDatabaseNamesOperation(0, 3))
            self.assertTrue(name in database_names)
        finally:
            raft_index = self.store.admin.server.send(DeleteDatabaseOperation(database_name=name, hard_delete=True))
            request_executor = ClusterRequestExecutor.create(self.default_urls, None)
            request_executor.execute(WaitForRaftIndexCommand(raft_index["raft_command_index"]))

    def test_cannot_create_database_with_the_same_name(self):
        name = "Duplicate"
        try:
            self.store.admin.server.send(CreateDatabaseOperation(database_name=name))
            TestBase.wait_for_database_topology(self.store, name)
            with self.assertRaises(Exception):
                self.store.admin.server.send(CreateDatabaseOperation(database_name=name))
        finally:
            raft_index = self.store.admin.server.send(DeleteDatabaseOperation(database_name=name, hard_delete=True))
            request_executor = ClusterRequestExecutor.create(self.default_urls, None)
            command = WaitForRaftIndexCommand(index=raft_index["raft_command_index"])
            request_executor.execute(raven_command=command)

if __name__ == "__main__":
    unittest.main()
