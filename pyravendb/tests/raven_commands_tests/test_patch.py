import unittest
from pyravendb.commands.raven_commands import PutDocumentCommand, PatchCommand
from pyravendb.custom_exceptions.exceptions import DocumentDoesNotExistsException
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestPatch(TestBase):
    def setUp(self):
        super(TestPatch, self).setUp()
        command = PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}})
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute(command)

    def tearDown(self):
        super(TestPatch, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_patch_success_ignore_missing(self):
        command = PatchCommand(document_id="products/10", change_vector=None,
                               patch=PatchRequest("this.Name = 'testing'"), patch_if_missing=None)
        result = self.requests_executor.execute(command)
        self.assertIsNotNone(result)
        self.assertTrue(result["ModifiedDocument"]["Name"] == "testing")

    def test_patch_success_not_ignore_missing(self):
        command = PatchCommand(document_id="products/10", change_vector=None,
                               patch=PatchRequest("this.Name = 'testing'"),
                               patch_if_missing=None, skip_patch_if_change_vector_mismatch=True)
        result = self.requests_executor.execute(command)
        self.assertIsNotNone(result)
        self.assertTrue(result["ModifiedDocument"]["Name"] == "testing")

    def test_patch_fail_not_ignore_missing(self):
        command = PatchCommand(document_id="products/110", change_vector=None,
                               patch=PatchRequest("this.Name = 'testing'"),
                               patch_if_missing=None)
        self.assertIsNone(self.requests_executor.execute(command))


if __name__ == "__main__":
    unittest.main
