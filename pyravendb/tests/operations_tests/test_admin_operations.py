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

    def test_put_transformer_operation(self):
        transformer_definition_users = TransformerDefinition("UsersTrans",
                                                             "from user in results select new {user.first_name,user.last_name}")
        transformer_definition_dogs = TransformerDefinition("DogsTrans",
                                                            "from dog in results select new { dog.name, dog.brand }")

        self.store.admin.send(PutTransformerOperation(transformer_definition_users))
        self.store.admin.send(PutTransformerOperation(transformer_definition_dogs))
        transformer_names = self.store.admin.send(GetTransformerNamesOperation(0, 25))
        self.assertTrue("UsersTrans" in transformer_names and "DogsTrans" in transformer_names)


if __name__ == "__main__":
    unittest.main()
