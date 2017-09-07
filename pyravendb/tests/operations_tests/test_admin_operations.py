from pyravendb.tests.test_base import *
from pyravendb.raven_operations.admin_operations import *
from pyravendb.data.transformer import TransformerDefinition
import unittest


class TestAdminOperations(TestBase):
    def tearDown(self):
        super(TestAdminOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_put_index_operation(self):
        index_definition_users = IndexDefinition("Users", "from doc in docs.Users select new {doc.Name}")
        index_definition_dogs = IndexDefinition("Dogs", "from doc in docs.Dogs select new {doc.brand}")

        self.store.admin.send(PutIndexesOperation(index_definition_dogs, index_definition_users))
        index_names = self.store.admin.send(GetIndexNamesOperation(0, 25))
        self.assertTrue("Users" in index_names and "Dogs" in index_names)


if __name__ == "__main__":
    unittest.main()
