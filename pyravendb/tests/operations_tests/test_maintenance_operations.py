from pyravendb.tests.test_base import *
from pyravendb.raven_operations.maintenance_operations import *
import unittest


class TestMaintenanceOperations(TestBase):
    def tearDown(self):
        super(TestMaintenanceOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_put_index_operation(self):
        index_definition_users = IndexDefinition("Users", "from doc in docs.Users select new {doc.Name}")
        index_definition_dogs = IndexDefinition("Dogs", "from doc in docs.Dogs select new {doc.brand}")

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs, index_definition_users))
        index_names = self.store.maintenance.send(GetIndexNamesOperation(0, 25))
        self.assertTrue("Users" in index_names and "Dogs" in index_names)

    def test_get_index_operation(self):
        index_definition_users = IndexDefinition("Users", "from doc in docs.Users select new {doc.Name}")
        self.store.maintenance.send(PutIndexesOperation(index_definition_users))
        users_index_definition = self.store.maintenance.send(GetIndexOperation("Users"))
        self.assertEqual(users_index_definition.name, "Users")


if __name__ == "__main__":
    unittest.main()
