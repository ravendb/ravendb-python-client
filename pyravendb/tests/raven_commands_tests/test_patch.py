import unittest
from pyravendb.commands.raven_commands import PutDocumentCommand, PatchCommand
from pyravendb.custom_exceptions.exceptions import DocumentDoesNotExistsException
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestPatch(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestPatch, cls).setUpClass()
        command = PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}})
        cls.requests_executor.execute(command)

    def test_patch_success_ignore_missing(self):
        self.assertIsNotNone(self.requests_executor.execute(
            PatchCommand("products/10", PatchRequest("this.Name = 'testing'"))))

    def test_patch_success_not_ignore_missing(self):
        self.assertIsNotNone(
            self.requests_executor.execute(
                PatchCommand("products/10", PatchRequest("this.Name = 'testing'"), skip_patch_if_etag_mismatch=True)))

        def test_patch_fail_not_ignore_missing(self):
            with self.assertRaises(DocumentDoesNotExistsException):
                self.requests_executor.execute(
                    PatchCommand("products/10", PatchRequest("this.Name = 'testing'"),
                                 skip_patch_if_etag_mismatch=False))

    if __name__ == "__main__":
        unittest.main
