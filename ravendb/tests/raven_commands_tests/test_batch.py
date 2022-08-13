import unittest

from ravendb.documents.commands.crud import GetDocumentsCommand
from ravendb.documents.commands.batches import (
    DeleteCommandData,
    PatchCommandData,
    PutCommandDataBase,
    SingleNodeBatchCommand,
)
from ravendb.documents.operations.patch import PatchRequest
from ravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    def setUp(self):
        super(TestDelete, self).setUp()
        self.put_command1 = PutCommandDataBase(
            "products/1-A",
            None,
            {
                "Name": "tests",
                "Category": "testing",
                "@metadata": {"Raven-Python-Type": "Products", "@collection": "Products"},
            },
        )
        self.put_command2 = PutCommandDataBase(
            "products/2-A",
            None,
            {
                "Name": "testsDelete",
                "Category": "testing",
                "@metadata": {"Raven-Python-Type": "Products", "@collection": "Products"},
            },
        )
        self.delete_command = DeleteCommandData("products/2-A", None)
        patch = PatchRequest()
        patch.script = "this.Name = 'testing';"
        self.scripted_patch_command = PatchCommandData("products/1-A", None, patch)
        self.requests_executor = self.store.get_request_executor()

    def tearDown(self):
        super(TestDelete, self).tearDown()
        self.delete_all_topology_files()

    def test_success_one_command(self):
        batch_command = SingleNodeBatchCommand(self.store.conventions, [self.put_command1])
        self.requests_executor.execute_command(batch_command)
        batch_result = batch_command.result
        self.assertEqual(len(batch_result.results), 1)

    def test_success_multi_commands(self):
        batch_command = SingleNodeBatchCommand(
            self.store.conventions, [self.put_command1, self.put_command2, self.delete_command]
        )
        self.requests_executor.execute_command(batch_command)
        batch_result = batch_command.result
        self.assertEqual(len(batch_result.results), 3)

    def test_scripted_patch(self):
        batch_command = SingleNodeBatchCommand(self.store.conventions, [self.put_command1, self.scripted_patch_command])
        self.requests_executor.execute_command(batch_command)
        get_doc_command = GetDocumentsCommand.from_single_id("products/1-A")
        self.requests_executor.execute_command(get_doc_command)
        get_result = get_doc_command.result
        self.assertEqual(get_result.results[0]["Name"], "testing")

    def test_fail(self):
        with self.assertRaises(ValueError):
            batch_command = SingleNodeBatchCommand(self.store.conventions, [self.put_command1, self.put_command2, None])
            self.requests_executor.execute_command(batch_command)

    if __name__ == "__main__":
        unittest.main()
