import unittest
from pyravendb.commands.raven_commands import BatchCommand, GetDocumentCommand
from pyravendb.commands import commands_data
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    def setUp(self):
        super(TestDelete, self).setUp()
        self.put_command1 = commands_data.PutCommandData("products/1-A",
                                                         document={"Name": "tests", "Category": "testing"},
                                                         metadata={"Raven-Python-Type": "Products",
                                                                   "@collection": "Products"})
        self.put_command2 = commands_data.PutCommandData("products/2-A",
                                                         document={"Name": "testsDelete", "Category": "testing"},
                                                         metadata={"Raven-Python-Type": "Products",
                                                                   "@collection": "Products"})
        self.delete_command = commands_data.DeleteCommandData("products/2-A")
        patch = PatchRequest("this.Name = 'testing';")
        self.scripted_patch_command = commands_data.PatchCommandData("products/1-A", patch)
        self.requests_executor = self.store.get_request_executor()

    def tearDown(self):
        super(TestDelete, self).tearDown()
        self.delete_all_topology_files()

    def test_success_one_command(self):
        batch_command = BatchCommand([self.put_command1])
        batch_result = self.requests_executor.execute(batch_command)
        self.assertEqual(len(batch_result), 1)

    def test_success_multi_commands(self):
        batch_command = BatchCommand([self.put_command1, self.put_command2, self.delete_command])
        batch_result = self.requests_executor.execute(batch_command)
        self.assertEqual(len(batch_result), 3)

    def test_scripted_patch(self):
        batch_command = BatchCommand([self.put_command1, self.scripted_patch_command])
        self.requests_executor.execute(batch_command)
        result = self.requests_executor.execute(GetDocumentCommand("products/1-A"))
        self.assertEqual(result["Results"][0]["Name"], "testing")

    def test_fail(self):
        with self.assertRaises(ValueError):
            batch_command = BatchCommand([self.put_command1, self.put_command2, None])
            self.requests_executor.execute(batch_command)

    if __name__ == "__main__":
        unittest.main()
