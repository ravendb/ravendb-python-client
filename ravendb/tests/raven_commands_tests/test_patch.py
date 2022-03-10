import unittest

from ravendb.documents.commands.crud import PutDocumentCommand
from ravendb.documents.operations.patch import PatchOperation, PatchRequest
from ravendb.tests.test_base import TestBase


class TestPatch(TestBase):
    def setUp(self):
        super(TestPatch, self).setUp()
        command = PutDocumentCommand("products/10", None, {"Name": "test", "@metadata": {}})
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute_command(command)

    def tearDown(self):
        super(TestPatch, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_patch_success_ignore_missing(self):
        command = PatchOperation.PatchCommand(
            self.store.conventions,
            document_id="products/10",
            change_vector=None,
            patch=PatchRequest("this.Name = 'testing'"),
            patch_if_missing=None,
        )
        self.requests_executor.execute_command(command)
        result = command.result
        self.assertIsNotNone(result)
        self.assertTrue(result.modified_document["Name"] == "testing")

    def test_patch_success_not_ignore_missing(self):
        command = PatchOperation.PatchCommand(
            self.store.conventions,
            document_id="products/10",
            change_vector=None,
            patch=PatchRequest("this.Name = 'testing'"),
            patch_if_missing=None,
            skip_patch_if_change_vector_mismatch=True,
        )
        self.requests_executor.execute_command(command)
        result = command.result
        self.assertIsNotNone(result)
        self.assertTrue(result.modified_document["Name"] == "testing")

    def test_patch_fail_not_ignore_missing(self):
        command = PatchOperation.PatchCommand(
            self.store.conventions,
            document_id="products/110",
            change_vector=None,
            patch=PatchRequest("this.Name = 'testing'"),
            patch_if_missing=None,
        )
        self.requests_executor.execute_command(command)
        self.assertIsNone(command.result)


if __name__ == "__main__":
    unittest.main
